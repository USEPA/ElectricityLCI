#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# eia923_generation.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
from functools import lru_cache
import logging
import os
from os.path import join

import pandas as pd

from electricitylci.eia860_facilities import eia860_balancing_authority
from electricitylci.globals import EIA923_BASE_URL
from electricitylci.globals import FUEL_CAT_CODES
from electricitylci.globals import paths
from electricitylci.utils import download_unzip
from electricitylci.utils import find_file_in_folder
try:
    from electricitylci.model_config import model_specs
except ImportError:
    import electricitylci.model_config as config
    config.model_specs = config.build_model_class()


##############################################################################
# GLOBALS
##############################################################################
__doc__ = """Download and import EIA 923 data, which primarily includes electricity generated and fuel used by facility. This module will download the data as needed and provides functions to access different pages of the Excel workbook.


Last edited:
    2024-03-08
"""
EIA923_PAGES = {
    "1": "Page 1 Generation and Fuel Data",
    "2": "Page 2 Stocks Data",
    "2a": "Page 2 Oil Stocks Data",
    "2b": "Page 2 Coal Stocks Data",
    "2c": "Page 2 Petcoke Stocks Data",
    "3": "Page 3 Boiler Fuel Data",
    "4": "Page 4 Generator Data",
    "5": "Page 5 Fuel Receipts and Costs",
    "6": "Page 6 Plant Frame",
    "7": "Page 7 File Layout",
    "8c": "8C Air Emissions Control Info",
}

EIA923_HEADER_ROWS = {
    "1": 5,
    "2": 5,
    "2a": 5,
    "2b": 5,
    "2c": 5,
    "3": 5,
    "4": 5,
    "5": 4,
    "6": 5,
    "8c": 4,
}


##############################################################################
# FUNCTIONS
##############################################################################
def _clean_columns(df):
    """Remove special characters and convert column names to snake case.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with column names.

    Returns
    -------
    pandas.DataFrame
        The same data frame with column names formatted.
    """
    df.columns = (
        df.columns.str.lower()
        .str.replace("[^0-9a-zA-Z\-]+", " ", regex=True)
        .str.replace("-", "", regex=False)
        .str.strip()
        .str.replace(" ", "_", regex=False)
    )
    return df


def eia923_download(year, save_path):
    """Download and unzip one year of EIA 923 annual data to a subfolder
    of the data directory.

    Parameters
    ----------
    year : int or str
        The year of data to download and save
    save_path : path or str
        A folder where the zip file contents should be extracted

    """
    current_url = EIA923_BASE_URL + "xls/f923_{}.zip".format(year)
    archive_url = EIA923_BASE_URL + "archive/xls/f923_{}.zip".format(year)

    # try to download using the most current year url format
    try:
        download_unzip(current_url, save_path)
    except ValueError:
        download_unzip(archive_url, save_path)


def load_eia923_excel(eia923_path, page="1"):
    """Add docstring."""
    page_to_load = EIA923_PAGES[page]
    header_row = EIA923_HEADER_ROWS[page]
    eia = pd.read_excel(
        eia923_path,
        sheet_name=page_to_load,
        header=header_row,
        na_values=["."],
        dtype={"Plant Id": str, "YEAR": str, "NAICS Code": str, "EIA Sector Number": str},
    )
    # Get ride of line breaks. And apparently 2015 had 'Plant State'
    # instead of 'State'
    eia.columns = eia.columns.str.replace("\n", " ", regex=False).str.replace(
        "Plant State", "State", regex=False
    )

    return eia


# This function is called multiple times by the various upstream modules.
# lru_cache allows us to only read from the csv only once.
@lru_cache(maxsize=10)
def eia923_download_extract(year, group_cols=None):
    """
    Download (if necessary) and extract a single year of generation/fuel
    consumption data from EIA-923.

    Data are grouped by plant level

    Parameters
    ----------
    year : int or str
        Year of data to download/extract
    group_cols : list, optional
        The columns from EIA923 generation and fuel sheet to use when grouping
        generation and fuel consumption data.
        Defaults to none.

    Returns
    -------
    pandas.DataFrame
        Columns include:

        - 'Plant Id' (str): Plant identifier (e.g., '1')
        - 'Plant Name' (str): Plant name (e.g., 'Sand Point')
        - 'State' (str): Two-letter state abbreviation (e.g., 'AL')
        - 'NAICS Code' (str): Industry code (e.g. '22')
        - 'EIA Sector Number' (int): Sector identifer (e.g., 1)
        - 'Reported Prime Mover' (str): Prime mover code (e.g., 'IC')
        - 'Reported Fuel Type Code' (str): Fuel code (e.g., 'BIT')
        - 'YEAR' (str): EIA Form 923 year (e.g., '2021')
        - 'Total Fuel Consumption MMBtu' (int)
        - 'Net Generation (Megawatthours)' (float)
    """
    # HOTFIX long default list in function parameter [2023-12-27; TWD]
    if group_cols is None:
        group_cols = [
            "Plant Id",
            "Plant Name",
            "State",
            "NAICS Code",
            "EIA Sector Number",
            "Reported Prime Mover",
            "Reported Fuel Type Code",
            "YEAR",
        ]

    expected_923_folder = join(paths.local_path, "f923_{}".format(year))
    if not os.path.exists(expected_923_folder):
        logging.info("Downloading EIA-923 files")
        eia923_download(year=year, save_path=expected_923_folder)

        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match=["2_3_4_5"],
            return_name=True,
        )
        eia = load_eia923_excel(eia923_path)

        # Save as csv for easier access in future
        csv_fn = eia923_name.split(".")[0] + "page_1.csv"
        csv_path = join(expected_923_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_923_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if ".csv" in f and "{}_Final".format(year) in f and "page_1" in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info("Loading {} EIA-923 data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_923_folder, fn)
            eia = pd.read_csv(
                csv_path,
                dtype={"Plant Id": str, "YEAR": str, "NAICS Code": str},
                low_memory=False,
            )
        else:
            eia923_path, eia923_name = find_file_in_folder(
                folder_path=expected_923_folder,
                file_pattern_match=["2_3_4_5", "xlsx"],
                return_name=True,
            )
            eia = load_eia923_excel(eia923_path)

            csv_fn = eia923_name.split(".")[0] + "_page_1.csv"
            csv_path = join(expected_923_folder, csv_fn)
            eia.to_csv(csv_path, index=False)

    # Grouping similar facilities together.
    sum_cols = [
        "Total Fuel Consumption MMBtu",
        "Net Generation (Megawatthours)",
    ]
    EIA_923_generation_data = eia.groupby(group_cols, as_index=False)[
        sum_cols].sum()

    return EIA_923_generation_data


def group_fuel_categories(df):
    """Map EIA Form 923 reported fuel types to their fuel category code."""
    new_fuel_categories = df["Reported Fuel Type Code"].map(FUEL_CAT_CODES)

    return new_fuel_categories


def eia923_primary_fuel(eia923_gen_fuel=None,
                        year=None,
                        method_col="Net Generation (Megawatthours)"):
    """
    Determine the primary fuel for each power plant. Include the NAICS code
    for each plant in output.

    Primary fuel can be determined using either generation (output) or fuel
    consumption (input). EIA923 doesn't list fuel inputs for non-combustion
    generation (wind, sun, hydro, etc), so an additional step to determine
    the primary fuel of these plants if 'Total Fuel Consumption MMBtu' is
    selected as the method.

    Parameters
    ----------
    year : int
        Year of 923 data
    method_col : str, optional
        The method to use when determining the primary fuel of a power plant
        (the default is 'Net Generation (Megawatthours)', and the alternative
        is 'Total Fuel Consumption MMBtu')

    """
    if year:
        eia923_gen_fuel = eia923_download_extract(year)

    group_cols = ["Plant Id", "NAICS Code", "EIA Sector Number","Reported Fuel Type Code"]
    sum_cols = [
        "Net Generation (Megawatthours)",
        "Total Fuel Consumption MMBtu",
    ]
    plant_fuel_total = eia923_gen_fuel.groupby(
        group_cols,
        as_index=False)[sum_cols].sum()

    # Find the dataframe index for the fuel with the most gen at each plant
    # Use this to slice the dataframe and return plant code and primary fuel
    primary_fuel_idx = plant_fuel_total.groupby(
        "Plant Id")[method_col].idxmax()

    data_cols = [
        "Plant Id",
        "NAICS Code",
        "EIA Sector Number",
        "Reported Fuel Type Code",
        "Net Generation (Megawatthours)",
    ]
    primary_fuel = plant_fuel_total.loc[primary_fuel_idx, data_cols]

    # Also going to include the percent of total generation from primary
    # fuel
    total_gen_plant = eia923_gen_fuel.groupby(
        "Plant Id",
        as_index=False)["Net Generation (Megawatthours)"].sum()
    total_gen_plant.rename(
        columns={"Net Generation (Megawatthours)": "total_gen"},
        inplace=True
    )
    primary_fuel = primary_fuel.merge(total_gen_plant, on="Plant Id")
    primary_fuel["primary fuel percent gen"] = (
        primary_fuel["Net Generation (Megawatthours)"]
        / primary_fuel["total_gen"]
        * 100
    )
    # HOTFIX: pandas FutureWarning syntax [2024-03-08; TWD]
    primary_fuel["primary fuel percent gen"] = primary_fuel[
        "primary fuel percent gen"].fillna(0)
    primary_fuel["FuelCategory"] = group_fuel_categories(primary_fuel)
    if model_specs.keep_mixed_plant_category:
        primary_fuel.loc[
            primary_fuel["primary fuel percent gen"]
            < model_specs.min_plant_percent_generation_from_primary_fuel_category,
            "FuelCategory",
        ] = "MIXED"
    primary_fuel.rename(
        columns={"Reported Fuel Type Code": "PrimaryFuel"}, inplace=True
    )
    criteria = (eia923_gen_fuel["Reported Fuel Type Code"] == "SUN") & (
        eia923_gen_fuel["Reported Prime Mover"] != "PV"
    )
    nonsolar_pv_plants = eia923_gen_fuel.loc[criteria, "Plant Id"].unique()
    primary_fuel.loc[
        (primary_fuel["Plant Id"].isin(nonsolar_pv_plants))
        & (primary_fuel["FuelCategory"] == "SOLAR"),
        "FuelCategory",
    ] = "SOLARTHERMAL"
    primary_fuel.reset_index(inplace=True, drop=True)

    keep_cols = [
        "Plant Id",
        "NAICS Code",
        "FuelCategory",
        "PrimaryFuel",
        "primary fuel percent gen",
    ]

    return primary_fuel.loc[:, keep_cols]


def calculate_plant_efficiency(gen_fuel_data):
    """Calculate plant efficiency (percentage).

    Plant efficiency is the ratio of net generation to total fuel
    consumption. British thermal unit to watt-hour conversion factor,
    3.412 is used.
    """
    plant_total = gen_fuel_data.groupby("Plant Id", as_index=False).sum()
    plant_total["efficiency"] = (
        plant_total["Net Generation (Megawatthours)"]
        * 10
        / (plant_total["Total Fuel Consumption MMBtu"] * 3.412)
        * 100
    )
    plant_total=plant_total.drop(columns=["EIA Sector Number"])
    plant_total=plant_total.merge(gen_fuel_data[["Plant Id","EIA Sector Number"]],on="Plant Id",how="left")
    return plant_total


def efficiency_filter(df, egrid_facility_efficiency_filters):
    """Return a slice of a data frame based on the values and
    upper/lower limits for plant efficiency."""
    upper = egrid_facility_efficiency_filters["upper_efficiency"]
    lower = egrid_facility_efficiency_filters["lower_efficiency"]

    df = df.loc[(df["efficiency"] >= lower) & (df["efficiency"] <= upper), :]

    return df


def build_generation_data(
        egrid_facilities_to_include=None, generation_years=None):
    """
    Build a dataset of facility-level generation using EIA923. This
    function will apply filters for positive generation, generation
    efficiency within a given range, and a minimum percent of generation
    from the primary fuel (if set in the config file). The returned
    dataframe also includes the balancing authority for every power
    plant.

    Parameters
    ----------
    egrid_facilities_to_include : list, optional
        List of plant codes to include (default is None, which builds a list)
    generation_years : list, optional
        Years of generation data to include in the output (default is None,
        which builds a list from the inventories of interest and eia_gen_year
        parameters)

    Returns
    ----------
    pandas.DataFrame
        Dataframe columns include: ['FacilityID', 'Electricity', 'Year'].
    """
    if generation_years is None:
        # Use the years from inventories of interest
        generation_years = set(
            list(model_specs.inventories_of_interest.values())
            + [model_specs.eia_gen_year]
        )

    df_list = []
    for year in generation_years:
        gen_fuel_data = eia923_download_extract(year)
        primary_fuel = eia923_primary_fuel(gen_fuel_data)
        gen_efficiency = calculate_plant_efficiency(gen_fuel_data)

        final_gen_df = gen_efficiency.merge(primary_fuel, on="Plant Id")
        if not egrid_facilities_to_include:
            if model_specs.include_only_egrid_facilities_with_positive_generation:
                final_gen_df = final_gen_df.loc[
                    final_gen_df["Net Generation (Megawatthours)"] >= 0, :
                ]
            if model_specs.filter_on_efficiency:
                final_gen_df = efficiency_filter(
                    final_gen_df,
                    model_specs.egrid_facility_efficiency_filters
                )
            if (
                model_specs.filter_on_min_plant_percent_generation_from_primary_fuel
                and not model_specs.keep_mixed_plant_category
            ):
                final_gen_df = final_gen_df.loc[
                    final_gen_df["primary fuel percent gen"]
                    >= model_specs.min_plant_percent_generation_from_primary_fuel_category,
                    :,
                ]
            if model_specs.filter_non_egrid_emission_on_NAICS:
            #     # Check with Wes to see what the filter here is supposed to be
                final_gen_df = final_gen_df.loc[
                    (final_gen_df['NAICS Code'] == '22') & (final_gen_df['EIA Sector Number'].isin(['1','2'])) , :
                ]
        else:
            final_gen_df = final_gen_df.loc[
                final_gen_df["Plant Id"].isin(egrid_facilities_to_include), :
            ]

        ba_match = eia860_balancing_authority(year)
        ba_match["Plant Id"] = ba_match["Plant Id"].astype(int)
        final_gen_df["Plant Id"] = final_gen_df["Plant Id"].astype(int)
        final_gen_df = final_gen_df.merge(ba_match, on="Plant Id", how="left")
        final_gen_df["Year"] = int(year)
        df_list.append(final_gen_df)

    all_years_gen = pd.concat(df_list)

    all_years_gen = all_years_gen.rename(
        columns={
            "Plant Id": "FacilityID",
            "Net Generation (Megawatthours)": "Electricity",
        }
    )

    all_years_gen = all_years_gen.loc[:, ["FacilityID", "Electricity", "Year"]]
    all_years_gen.reset_index(drop=True, inplace=True)
    all_years_gen["Year"] = all_years_gen["Year"].astype("int32")
    return all_years_gen


def eia923_generation_and_fuel(year):
    """Add docstring."""
    expected_923_folder = join(paths.local_path, "f923_{}".format(year))

    if not os.path.exists(expected_923_folder):
        logging.info("Downloading EIA-923 files")
        eia923_download(year=year, save_path=expected_923_folder)

        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match=["2_3_4_5", "xlsx"],
            return_name=True,
        )
        # Save as csv for easier access in future
        csv_fn = eia923_name.split(".")[0] + "page_1.csv"
        csv_path = join(expected_923_folder, csv_fn)
        eia = load_eia923_excel(expected_923_folder, page="1")
        eia.to_csv(csv_path, index=False)
    else:
        all_files = os.listdir(expected_923_folder)
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if ".csv" in f and "{}_Final".format(year) in f and "page_1" in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info("Loading {} EIA-923 data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_923_folder, fn)
            eia = pd.read_csv(
                csv_path,
                dtype={"Plant Id": str, "YEAR": str, "NAICS Code": str},
                low_memory=False,
            )
        else:
            eia923_path, eia923_name = find_file_in_folder(
                folder_path=expected_923_folder,
                file_pattern_match=["2_3_4_5", "xlsx"],
                return_name=True,
            )
            eia = load_eia923_excel(eia923_path, page="1")
            csv_fn = eia923_name.split(".")[0] + "_page_1.csv"
            csv_path = join(expected_923_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)

    return eia


def eia923_boiler_fuel(year):
    """Add docstring."""
    expected_923_folder = join(paths.local_path, "f923_{}".format(year))

    if not os.path.exists(expected_923_folder):
        logging.info("Downloading EIA-923 files")
        eia923_download(year=year, save_path=expected_923_folder)

        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match=["2_3_4_5", "xlsx"],
            return_name=True,
        )
        # Save as csv for easier access in future
        csv_fn = eia923_name.split(".")[0] + "page_3.csv"
        csv_path = join(expected_923_folder, csv_fn)
        eia = load_eia923_excel(expected_923_folder, page="3")
        eia.to_csv(csv_path, index=False)
    else:
        all_files = os.listdir(expected_923_folder)
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if ".csv" in f and "{}_Final".format(year) in f and "page_3" in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info("Loading {} EIA-923 data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_923_folder, fn)
            eia = pd.read_csv(
                csv_path,
                dtype={"Plant Id": str, "YEAR": str, "NAICS Code": str},
                low_memory=False,
            )
        else:

            eia923_path, eia923_name = find_file_in_folder(
                folder_path=expected_923_folder,
                file_pattern_match=["2_3_4_5", "xlsx"],
                return_name=True,
            )
            eia = load_eia923_excel(eia923_path, page="3")
            csv_fn = eia923_name.split(".")[0] + "_page_3.csv"
            csv_path = join(expected_923_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)

    return eia


def eia923_sched8_aec(year):
    """Add docstring."""
    expected_923_folder = join(paths.local_path, "f923_{}".format(year))

    if not os.path.exists(expected_923_folder):
        logging.info("Downloading EIA-923 files")
        eia923_download(year=year, save_path=expected_923_folder)

        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match=["Schedule_8", "xlsx"],
            return_name=True,
        )
        # Save as csv for easier access in future
        csv_fn = eia923_name.split(".")[0] + "page_8c.csv"
        csv_path = join(expected_923_folder, csv_fn)
        eia = load_eia923_excel(expected_923_folder, page="8c")
        eia.to_csv(csv_path, index=False)
    else:
        all_files = os.listdir(expected_923_folder)
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if ".csv" in f and "{}_Final".format(year) in f and "page_8c" in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            logging.info("Loading {} EIA-923 data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_923_folder, fn)
            eia = pd.read_csv(
                csv_path,
                dtype={"Plant Id": str, "YEAR": str, "NAICS Code": str},
                low_memory=False,
            )
        else:
            eia923_path, eia923_name = find_file_in_folder(
                folder_path=expected_923_folder,
                file_pattern_match=["Schedule_8", "xlsx"],
                return_name=True,
            )
            eia = load_eia923_excel(eia923_path, page="8c")
            csv_fn = eia923_name.split(".")[0] + "_page_8c.csv"
            csv_path = join(expected_923_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)

    return eia


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    from electricitylci.globals import output_dir

    gen_and_fuel_df = eia923_generation_and_fuel(2020)
    gen_and_fuel_df.to_csv(
        f"{output_dir}/gen_and_fuel_df_2020.csv",
        encoding="utf-8-sig"
    )
