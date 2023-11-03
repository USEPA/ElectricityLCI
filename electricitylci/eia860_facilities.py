#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# eia860_facilities.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import pandas as pd
import os
from electricitylci.globals import EIA860_BASE_URL, paths
from electricitylci.utils import (
    download_unzip,
    find_file_in_folder,
    create_ba_region_map,
)
import logging


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is designed to download and import EIA860 data,
including power plant information such as plant code, location, balancing
authority, and primary fuel type.

For now, this module is using most of the code from eia923_generation.py.
It could be combined and generalized in the future.

Last updated: 2023-11-03
"""


##############################################################################
# FUNCTIONS
##############################################################################
def _clean_columns(df):
    """Remove special characters and convert column names to snake case.

    Parameters
    ----------
    df : pandas.DataFrame
        A pandas data frame with named columns.

    Returns
    -------
    pandas.DataFrame
        The same data frame received, but with column names cleaned.
    """
    df.columns = (
        df.columns.str.lower()
        .str.replace("[^0-9a-zA-Z\-]+", " ", regex=True)
        .str.replace("-", "", regex=False)
        .str.strip()
        .str.replace(" ", "_", regex=False)
    )
    return df


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

    return eia


def eia860_balancing_authority(year, regional_aggregation=None):
    """Return a data frame consisting of EIA Plant IDs and other identifying
    information, including balancing authority area.
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


def eia860_primary_capacity(year):
    """Add docstring."""
    pass


def eia860_EnviroAssoc_so2(year):
    """Return a data frame containing the SO2-related environmental controls
    for the power plants.

    This data is used in ampd_plant_emissions.py to calculate SO2 emission
    factors."""
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

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["6_2_EnviroEquip", "xlsx"],
            return_name=True,
        )

        eia = load_eia860_excel(
            eia860_path, "Boiler Info & Design Parameters", 1
        )

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_info.csv"
        csv_path = os.path.join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
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
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_2_EnviroEquip", "xlsx"],
                return_name=True,
            )
            eia = load_eia860_excel(
                eia860_path, "Boiler Info & Design Parameters", 1
            )

            csv_fn = eia860_name.split(".")[0] + "_boiler_info.csv"
            csv_path = os.path.join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia


def eia860_EnviroAssoc_nox(year):
    """Return a data frame containing the NOX-related environmental controls
    for the power plants from EIA Form 860.

    This data is used in ampd_plant_emissions.py to calculate NOX emission
    factors.

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
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str},low_memory=False)

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


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    eia_nox = eia860_EnviroAssoc_nox(2016)
    eia_so2 = eia860_EnviroAssoc_so2(2016)
    eia_boiler = eia860_boiler_info_design(2016)
