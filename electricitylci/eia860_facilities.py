#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# eia860_facilities.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import pandas as pd

from electricitylci.globals import paths
from electricitylci.globals import EIA860_BASE_URL

from electricitylci.utils import download_unzip
from electricitylci.utils import find_file_in_folder
from electricitylci.utils import create_ba_region_map
from electricitylci.utils import read_ba_codes


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is designed to download and import EIA860 data,
including power plant information such as plant code, location, balancing
authority, and primary fuel type.

For now, this module is using most of the code from eia923_generation.py.
It could be combined and generalized in the future.

Note that this is one of the few modules in ElectricityLCI that does not
depend on config.model_specs, which means you can utilize it freely.

Last updated:
    2026-02-12
"""
__all__ = [
    "add_balancing_authorities_to_plants",
    "eia860_balancing_authority",
    "eia860_boiler_info_design",
    "eia860_download",
    "eia860_EnviroAssoc_nox",
    "eia860_EnviroAssoc_so2",
    "eia860_generator_info",
    "load_eia860_excel",
]


##############################################################################
# FUNCTIONS
##############################################################################
def _clean_columns(df, year=None):
    """Remove special characters and convert column names to snake case.

    Parameters
    ----------
    df : pandas.DataFrame
        A pandas data frame with named columns.
    year : int, optional
        The year of the data, used for year-specific column name mapping.

    Returns
    -------
    pandas.DataFrame
        The same data frame received, but with column names cleaned.
    """
    df.columns = (
        df.columns.str.lower()
        .str.replace("[^0-9a-zA-Z\\-]+", " ", regex=True)
        .str.replace("-", "", regex=False)
        .str.strip()
        .str.replace(" ", "_", regex=False)
    )

    # Handle year-specific column name differences for firing type columns
    if year is not None:
        # Map different column names to standard names
        column_mapping = {}

        if year == 2011:
            # 2011: fire_primary_fuel1, fire_primary_fuel2, fire_primary_fuel3
            #   (no underscores before numbers) and
            #   plant_code instead of plant_id
            column_mapping = {
                'fire_primary_fuel1': 'firing_type_1',
                'fire_primary_fuel2': 'firing_type_2',
                'fire_primary_fuel3': 'firing_type_3',
                'plant_code': 'plant_id'
            }
        elif year == 2012:
            # 2012: Fire Primary Fuel 1, Fire Primary Fuel 2, and
            #   Fire Primary Fuel 3
            column_mapping = {
                'fire_primary_fuel_1': 'firing_type_1',
                'fire_primary_fuel_2': 'firing_type_2',
                'fire_primary_fuel_3': 'firing_type_3'
            }
        # 2013+: Firing Type 1, Firing Type 2, Firing Type 3 (already correct)

        # Apply the mapping
        df = df.rename(columns=column_mapping)

    # Ensure plant_id is string type for consistent merging
    if 'plant_id' in df.columns:
        df['plant_id'] = df['plant_id'].astype(str)

    return df


def _remove_table_note(df):
    """Helper function to remove a table note from the bottom of a data frame.
    """
    if df.empty:
        return df

    # Several worksheets have a table note as their last row!
    # HOTFIX: Check to see what the contents are in the last row, first column
    # and drop the row if its the table note. [250902; TWD]
    last_row_first_col = str(df.iloc[-1, 0])

    if last_row_first_col.strip().upper().startswith("NOTE:"):
        logging.info("Removing table footer from EIA860 worksheet")
        return df.iloc[:-1]
    else:
        return df


# NEW
def add_balancing_authorities_to_plants(plant_df, plant_col, year):
    """Helper function to add balancing authority information to a data frame
    with facility IDs.

    Relies on :func:`read_ba_codes` in utils.py to standardized balancing
    authority naming.

    Parameters
    ----------
    plant_df : pandas.DataFrame
        A data frame where ``plant_col`` has facility IDs
    plant_col : str
        The column name associated with plant IDs.
    year : int
        The year to associate with EIA Form 860.

    Returns
    -------
    tuple
        A tuple of length two:

        -   pandas.DataFrame, the original data frame returned with additional
            columns (e.g., 'State', 'NERC Region', 'Balancing Authority Name',
            and 'Balancing Authority Code').
        -   dict, a dictionary with keys associated with plant IDs (int) and
            values associated with balancing authority names (str)

    Notes
    -----
    This method may be better utilized if it extended to multiple years'
    worth of EIA 860 to try to capture as many facilities as possible.
    """

    # Read the EIA Form 860 for the given year
    eia_df = eia860_balancing_authority(year)

    # EIA has 'No BA' for AK facilities; this can cause problems, so remove it!
    noba_filter = eia_df['Balancing Authority Name'] == 'No BA'
    eia_df.loc[noba_filter, 'Balancing Authority Name'] = float('nan')

    # We want BA info, so lose any rows without it.
    eia_df = eia_df.dropna(
        subset=['Balancing Authority Name', 'Balancing Authority Code'],
        how='all'
    ).copy()

    # Standardized BA names
    ba_codes = read_ba_codes()
    eia_df["Balancing Authority Name"] = eia_df["Balancing Authority Code"].map(
        ba_codes['BA_Name'])

    # Make sure plant ID columns are integers for matching;
    # NOTE: you can typically get away with 32-bit integer for plant IDs
    eia_df["Plant Id"] = eia_df["Plant Id"].astype("int32")
    plant_df[plant_col] = plant_df[plant_col].astype("int32")

    # Get sets of plant IDs
    eia_plants = set(eia_df['Plant Id'].tolist())
    user_plants = set(plant_df[plant_col].tolist())

    # Analysis of user plants not found in EIA
    user_only = user_plants - eia_plants
    logging.warning(
        "Found %d plants without balancing authority info!" % len(user_only)
    )

    # Merge the EIA data to the plant data frame.
    plant_df = plant_df.merge(
        eia_df,
        how="left",
        left_on=plant_col,
        right_on="Plant Id"
    ).copy()

    # Create a helper dictionary to map plant IDs to their BA names.
    plant_ba_dict = {
        x[0]: x[1]
        for x in zip(
            eia_df["Plant Id"],
            eia_df["Balancing Authority Name"],
        )
    }

    return (plant_df, plant_ba_dict)


def eia860_balancing_authority(year, regional_aggregation=None):
    """Return a data frame consisting of EIA Plant IDs and other identifying
    information, including balancing authority area.

    Called in combinator.py, eia_io_trading.py, generation.py, and
    hydro_upstream.py; albeit none send regional aggregation parameter.

    Parameters
    ----------
    year : int
        The Form EIA860 year. Will be downloaded and parsed into CSV files
        locally, if not already.
    regional_aggregation : str
        An additional region column to add to the data frame (e.g.,
        BA, NERC, FERC, EIA).

    Returns
    -------
    pandas.DataFrame
        A data frame with columns:

        - 'Plant Id'
        - 'State'
        - 'NERC Region'
        - 'Balancing Authority Code'
        - 'Balancing Authority Name'
        - `regional_aggregation` (if not none)

    Notes
    -----
    Notable deficiencies in NERC and Balancing Authority categories include:

    - Most, if not all, facilities in AK and HI.
    - WECC (AZ, CA)
    - SERC (FL, GA)

    14 plants in 2020 are labeled as 'No BA' (see AK, HI, RI, ME).
    """

    expected_860_folder = os.path.join(
        paths.local_path, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        logging.info("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["2___Plant"],
            return_name=True,
        )
        eia = load_eia860_excel(eia860_path)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + ".csv"
        csv_path = os.path.join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if ".csv" in f and "Plant_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info(
                "Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = os.path.join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str},low_memory=False)

        else:
            logging.info("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["2___Plant"],
                return_name=True,
            )
            eia = load_eia860_excel(eia860_path)

            csv_fn = eia860_name.split(".")[0] + ".csv"
            csv_path = os.path.join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)

    ba_cols = [
        "Plant Id",
        "State",
        "NERC Region",
        "Balancing Authority Code",
        "Balancing Authority Name",
    ]
    eia_plant_ba_match = eia.loc[:, ba_cols].drop_duplicates()

    # Map the balancing authority to a larger region (e.g. FERC or EIA)
    if regional_aggregation:
        region_map = create_ba_region_map(region_col=regional_aggregation)
        eia_plant_ba_match[regional_aggregation] = eia_plant_ba_match[
            "Balancing Authority Code"
        ].map(region_map)

    return eia_plant_ba_match


def eia860_boiler_info_design(year):
    """Return a data frame containing boiler parameters from EIA Form 860.

    This data is used in ampd_plant_emissions.py to calculate emission factors.

    Parameters
    ----------
    year : int
        The year associated with EIA Form 860 data.

    Returns
    -------
    pandas.DataFrame
    """
    expected_860_folder = os.path.join(
        paths.local_path, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        logging.info("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        # Handle different file naming patterns for different years
        if year in [2011, 2012]:
            # For 2011-2012, files don't have the "6_2_" prefix
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["EnviroEquip", "xlsx"],
                return_name=True,
            )
        else:
            # For 2013+, use the standard pattern
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_2_EnviroEquip", "xlsx"],
                return_name=True,
            )

        # Handle different worksheet names for different years
        if year == 2011:
            worksheet_name = "boiler"
        elif year == 2012:
            worksheet_name = "Boiler"
        else:
            worksheet_name = "Boiler Info & Design Parameters"

        eia = load_eia860_excel(
            eia860_path, worksheet_name, 1
        )

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_info.csv"
        csv_path = os.path.join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        if year in [2011, 2012]:
            # For 2011-2012, look for files without the "6_2_" prefix
            csv_file = [
                f
                for f in all_files
                if "_boiler_info.csv" in f
                and "EnviroEquip" in f
            ]
        else:
            # For 2013+, use the standard pattern
            csv_file = [
                f
                for f in all_files
                if "_boiler_info.csv" in f
                and "6_2_EnviroEquip_Y{}".format(year) in f
            ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info(
                "Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = os.path.join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str},low_memory=False)

        else:
            logging.info("Loading data from previously downloaded excel file")

            # Handle different file naming patterns for different years
            if year in [2011, 2012]:
                # For 2011-2012, files don't have the "6_2_" prefix
                eia860_path, eia860_name = find_file_in_folder(
                    folder_path=expected_860_folder,
                    file_pattern_match=["EnviroEquip", "xlsx"],
                    return_name=True,
                )
            else:
                # For 2013+, use the standard pattern
                eia860_path, eia860_name = find_file_in_folder(
                    folder_path=expected_860_folder,
                    file_pattern_match=["6_2_EnviroEquip", "xlsx"],
                    return_name=True,
                )

            # Handle different worksheet names for different years
            if year == 2011:
                worksheet_name = "boiler"
            elif year == 2012:
                worksheet_name = "Boiler"
            else:
                worksheet_name = "Boiler Info & Design Parameters"

            eia = load_eia860_excel(
                eia860_path, worksheet_name, 1
            )

            csv_fn = eia860_name.split(".")[0] + "_boiler_info.csv"
            csv_path = os.path.join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia, year)
    return eia


def eia860_download(year, save_path):
    """
    Download and unzip one year of EIA 860 annual data to a subfolder
    of the data directory.

    Parameters
    ----------
    year : int or str
        The year of data to download and save
    save_path : path or str
        A folder where the zip file contents should be extracted
    """
    current_url = EIA860_BASE_URL + "xls/eia860{}.zip".format(year)
    archive_url = EIA860_BASE_URL + "archive/xls/eia860{}.zip".format(year)

    # try to download using the most current year url format
    try:
        download_unzip(current_url, save_path)
    except ValueError:
        download_unzip(archive_url, save_path)


def eia860_EnviroAssoc_nox(year):
    """Return a data frame containing the NOX-related environmental controls
    for the power plants from EIA Form 860.

    This data is used in ampd_plant_emissions.py to calculate NOX emission
    factors.

    Parameters
    ----------
    year : int
        The year associated with EIA Form 860 data.
        NOTE: does not work for years 2012 and prior.

    Returns
    -------
    pandas.DataFrame
        A data frame with columns:

        - 'utility_id' (int)
        - 'utility_name' (str)
        - 'plant_id' (str)
        - 'plant_name' (str)
        - 'boiler_id' (str)
        - 'nox_control_id' (str)
        - 'steam_plant_type' (float), optional (not in 2013)
    """
    expected_860_folder = os.path.join(
        paths.local_path, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        logging.info("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
            return_name=True,
        )
        eia = load_eia860_excel(eia860_path, "Boiler NOx", 1)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_nox.csv"
        csv_path = os.path.join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_boiler_nox.csv" in f
            and "6_1_EnviroAssoc_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info(
                "Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = os.path.join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str},low_memory=False)

        else:
            logging.info("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
                return_name=True,
            )
            eia = load_eia860_excel(eia860_path, "Boiler NOx", 1)

            csv_fn = eia860_name.split(".")[0] + "_boiler_nox.csv"
            csv_path = os.path.join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia


def eia860_EnviroAssoc_so2(year):
    """Return a data frame containing the SO2-related environmental controls
    for the power plants.

    This data is used in ampd_plant_emissions.py to calculate SO2 emission
    factors.
    """
    expected_860_folder = os.path.join(
        paths.local_path, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        logging.info("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
            return_name=True,
        )
        eia = load_eia860_excel(eia860_path, "Boiler SO2", 1)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_so2.csv"
        csv_path = os.path.join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_boiler_so2.csv" in f
            and "6_1_EnviroAssoc_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info(
                "Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = os.path.join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str},low_memory=False)

        else:
            logging.info("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
                return_name=True,
            )
            eia = load_eia860_excel(eia860_path, "Boiler SO2", 1)

            csv_fn = eia860_name.split(".")[0] + "_boiler_so2.csv"
            csv_path = os.path.join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia


def eia860_generator_info(year):
    """Return a data frame containing the information from EIA 860, Schedule 3,
    Generator Data. This includes the type of coal boilers used in the facility (e.g., uses pulverized coal, is supercritical, etc.).

    This data is used in ampd_plant_emissions.py to calculate emission factors.

    Parameters
    ----------
    year : int
        The year associated with EIA Form 860 data.

    Returns
    -------
    pandas.DataFrame
    """
    expected_860_folder = os.path.join(
        paths.local_path, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        logging.info("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        # NOTE: for 2011-2012, the "3_1_" is not in the filename
        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["3_1_Generator", "xlsx"],
            return_name=True,
        )
        eia = load_eia860_excel(eia860_path, "Operable", 1)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_generator_operable.csv"
        csv_path = os.path.join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_generator_operable.csv" in f
            and "3_1_Generator" in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info(
                "Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = os.path.join(expected_860_folder, fn)
            eia = pd.read_csv(
                csv_path, dtype={"Plant Id": str}, low_memory=False)

        else:
            logging.info("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["3_1_Generator", "xlsx"],
                return_name=True,
            )
            eia = load_eia860_excel(eia860_path, "Operable", 1)

            csv_fn = eia860_name.split(".")[0] + "_generator_operable.csv"
            csv_path = os.path.join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)

    return eia


def load_eia860_excel(eia860_path, sheet="Plant", header=1):
    """Read a named sheet from an EIA860 Excel workbook.

    If the column name, 'Plant Code' is found, it is replaced with 'Plant Id'
    to match the EIA923 data frame.

    Parameters
    ----------
    eia860_path : str
        File path to EIA 860 workbook
    sheet : str, optional
        Excel workbook sheet name, by default "Plant"
    header : int, optional
        Row number for column headers, by default 1

    Returns
    -------
    pandas.DataFrame
        EIA860 plant data.
    """
    eia = pd.read_excel(
        eia860_path,
        sheet_name=sheet,
        header=header,
        na_values=[".", " "],
        dtype={"Plant Code": str},
    )
    # Get rid of line breaks and rename Plant Code to Plant Id (match
    # the 923 column name)
    eia.columns = (
        eia.columns.str.replace("\n", " ", regex=False)
        .str.replace("Plant Code", "Plant Id", regex=False)
        .str.replace("Plant State", "State", regex=False)
    )

    # HOTFIX: remove table notes found in some 860 worksheets [240902;TWD]
    eia = _remove_table_note(eia)

    return eia


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    eia_nox = eia860_EnviroAssoc_nox(2016)
    eia_so2 = eia860_EnviroAssoc_so2(2016)
    eia_boiler = eia860_boiler_info_design(2016)
