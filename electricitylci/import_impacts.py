#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# import_impact.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.globals import paths
from electricitylci.utils import check_output_dir
from electricitylci.utils import download
from electricitylci.utils import read_ba_codes
from electricitylci.generation import add_temporal_correlation_score
from electricitylci.model_config import model_specs

##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses aggregate U.S.-level inventory for each fuel
category. These U.S. fuel category inventories are then used as proxies for
Canadian generation. The inventories are weighted by the percent of that type
of generation for the Canadian balancing authority area (e.g., if the BC
Hydro & Power Authority generation mix includes 9 per-cent biomass, then U.S.
-level biomass emissions are multiplied by 0.09. The result is a data frame
that includes balancing authority level inventories for Canadian imports.

See https://github.com/USEPA/ElectricityLCI/issues/231 for details and
references.

Last updated:
    2024-03-08
"""
__all__ = [
    "generate_canadian_mixes",
    "get_canadian_mix_file",
    "read_canadian_mix",
]


##############################################################################
# GLOBALS
##############################################################################
CA_ENERGY_FUTURES_URL = (
    "https://www.cer-rec.gc.ca/open/energy/energyfutures2023/"
    "electricity-generation-2023.csv")
'''str: The 2017 onward CSV resource for Canadian mixes.'''


##############################################################################
# FUNCTIONS
##############################################################################
def generate_canadian_mixes(us_inventory, gen_year):
    """Create the Canadian balancing authority inventory data frame.

    The inventories are weighted by the percent of each fuel type of
    generation for the Canadian balancing authority area. The aggregate U.S.
    inventory fuel categories and inventories are used as a proxy for
    Canadian generation.

    For example, if the BC Hydro & Power Authority generation mix includes
    9% biomass, then U.S.-level biomass emissions are multiplied by 0.09.
    The result is a data frame that includes balancing authority level
    inventories for Canadian imports.

    Parameters
    ----------
    us_inventory: pandas.DataFrame
        A data frame containing flow-level inventory for all fuel categories
        in the United States.
    gen_year : int
        The generation year (e.g., 2016).

    Returns
    ----------
    pandas.DataFrame
    """
    BA_CODES = read_ba_codes()

    canadian_egrid_ids = {
        "BCHA": 9999991,
        "HQT": 9999992,
        "IESO": 9999993,
        "MHEB": 9999994,
        "NBSO": 9999995,
        "NEWL": 9999996,
    }
    input_map = {
        "air": False,
        "water": False,
        "input": True,
        "waste": False,
        "output": False,
        "ground": False,
    }

    logging.info("Generating inventory for Canadian balancing authority areas")
    canadian_csv, to_p = get_canadian_mix_file(gen_year)
    canadian_mix = read_canadian_mix(canadian_csv, to_p)
    canadian_mix = canadian_mix.query("Year == %d" % gen_year).copy()

    if "input" in us_inventory.columns:
        us_inventory.loc[us_inventory["input"].isna(), "input"] = us_inventory[
            "Compartment"
        ].map(input_map)
    else:
        us_inventory["input"] = us_inventory["Compartment"].map(input_map)

    # If there are no upstream inventories, then the quantity column will
    # not exist, so let's create it.
    if "quantity" not in list(us_inventory.columns):
        us_inventory["quantity"] = float("nan")
    us_inventory_summary = us_inventory.groupby(
        by=[
            "FuelCategory",
            "FlowName",
            "FlowUUID",
        ]
    )[["FlowAmount", "quantity"]].sum()
    us_inventory_electricity = (
        us_inventory.drop_duplicates(
            subset=[
                "FuelCategory",
                "FlowName",
                "FlowUUID",
                "Unit",
                "Electricity",
            ]
        )
        .groupby(
            by=[
                "FuelCategory",
                "FlowName",
                "FlowUUID",
            ]
        )["Electricity"]
        .sum()
    )
    us_inventory_summary = pd.concat(
        [us_inventory_summary, us_inventory_electricity], axis=1
    )
    us_inventory_summary = us_inventory_summary.reset_index()

    flowuuid_compartment_df = (
        us_inventory[[
            "FlowUUID",
            "Compartment",
            "input",
            "Compartment_path",
            "Unit"
        ]].drop_duplicates(subset=["FlowUUID"]).set_index("FlowUUID")
    )
    us_inventory_summary["Compartment"] = us_inventory_summary["FlowUUID"].map(
        flowuuid_compartment_df["Compartment"]
    )
    us_inventory_summary["input"] = us_inventory_summary["FlowUUID"].map(
        flowuuid_compartment_df["input"]
    )
    us_inventory_summary["Unit"] = us_inventory_summary["FlowUUID"].map(
        flowuuid_compartment_df["Unit"]
    )
    us_inventory_summary["Compartment_path"] = us_inventory_summary[
        "FlowUUID"
    ].map(flowuuid_compartment_df["Compartment_path"])

    ca_mix_list = list()
    for baa_code in canadian_mix["Code"].unique():
        filter_crit = canadian_mix["Code"] == baa_code
        ca_inventory = pd.merge(
            left=us_inventory_summary,
            right=canadian_mix.loc[filter_crit, :],
            left_on="FuelCategory",
            right_on="FuelCategory",
            how="left",
            suffixes=["","ca_elec"]
        )
        ca_inventory.dropna(subset=["FuelCategory_fraction"], inplace=True)
        # HOTFIX: don't append empty data frames [2024-03-12; TWD]
        if len(ca_inventory) > 0:
            ca_mix_list.append(ca_inventory)

    # HOTFIX: check if ca_mix_list is empty, if so, ca_mix_inventory is just
    # blank_df; otherwise, concatenate the data frames.
    if len(ca_mix_list) == 0:
        ca_mix_inventory = pd.DataFrame(columns=us_inventory.columns)
    else:
        ca_mix_inventory = pd.concat(ca_mix_list, ignore_index=True)

    ca_mix_inventory[
        ["Electricity", "FlowAmount", "quantity"]
    ] = ca_mix_inventory[["Electricity", "FlowAmount", "quantity"]].multiply(
        ca_mix_inventory["FuelCategory_fraction"], axis="index"
    )
    ca_mix_inventory.drop(
        columns=["Province", "FuelCategory_fraction"], inplace=True
    )
    ca_mix_inventory = ca_mix_inventory.groupby(
        by=[
            "Code",
            "Balancing Authority Name",
            "Compartment",
            "Compartment_path",
            "FlowName",
            "FlowUUID",
            "input",
            "Unit",
        ],
        as_index=False,
    )[["Electricity", "FlowAmount", "quantity"]].sum()
    ca_mix_inventory["stage_code"] = "life cycle"
    ca_mix_inventory.sort_values(
        by=["Code", "Compartment", "FlowName", "stage_code"], inplace=True
    )
    ca_mix_inventory.rename(
        columns={"Code": "Balancing Authority Code"}, inplace=True
    )
    ca_mix_inventory["Source"] = "netlca"
    ca_mix_inventory["Year"] = us_inventory["Year"].mode().to_numpy()[0]
    ca_mix_inventory["FuelCategory"] = "ALL"
    ca_mix_inventory["eGRID_ID"] = ca_mix_inventory[
        "Balancing Authority Code"
    ].map(canadian_egrid_ids)
    ca_mix_inventory["FERC_Region"] = ca_mix_inventory[
        "Balancing Authority Code"
    ].map(BA_CODES["FERC_Region"])
    ca_mix_inventory["EIA_Region"] = ca_mix_inventory[
        "Balancing Authority Code"
    ].map(BA_CODES["EIA_Region"])
    ca_mix_inventory["DataReliability"] = 3
    ca_mix_inventory["TemporalCorrelation"] = add_temporal_correlation_score(
        ca_mix_inventory["Year"], model_specs.electricity_lci_target_year
    )
    ca_mix_inventory["GeographicalCorrelation"] = 4
    ca_mix_inventory["TechnologicalCorrelation"] = 3
    ca_mix_inventory["DataCollection"] = 5
    return ca_mix_inventory


def get_canadian_mix_file(gen_year):
    """Return the Canadian electricity generation mix file for a given
    generation year.

    Parameters
    ----------
    gen_year : int
        Generation year

    Returns
    -------
    tuple
        str :
            File path to the Canadian electricity generation CSV.
            Note for 2016 and earlier, the International_Electricity_Mix.csv
            data file is used. For later generation years, the Canadian
            Future Energy CSV is downloaded based on the global parameter,
            CA_ENERGY_FUTURES_URL.
        bool :
            Whether the file requires a pre-processing step in the
            :func:`read_canadian_mix` method.
    """
    # Define CSV
    if gen_year <= 2016:
        # NOTE: this file is also read by egrid_facilities.py
        ca_csv = os.path.join(data_dir, "International_Electricity_Mix.csv")
        process = False
    else:
        # Use the Canadian Energy Futures URL
        f_name = os.path.basename(CA_ENERGY_FUTURES_URL)
        out_dir = os.path.join(paths.local_path, "energyfutures")
        is_dir = check_output_dir(out_dir)

        process = True
        if is_dir:
            ca_csv = os.path.join(out_dir, f_name)
            if not os.path.exists(ca_csv):
                # Download CSV if not available
                r = download(CA_ENERGY_FUTURES_URL, ca_csv)
                if r:
                    logging.info("Downloaded Canadian Energy Futures CSV")
                else:
                    logging.error(
                        "Failed to download Canadian Energy Future "
                        "CSV: %s" % CA_ENERGY_FUTURES_URL)
        else:
            # Sub-directory does not exist and fails to create...
            ca_csv = os.path.join(paths.local_path, f_name)
            logging.warning("Writing Canadian mix to %s" % ca_csv)

    return (ca_csv, process)


def read_canadian_mix(file_path, pre_process=False):
    """Return a data frame read from a given CSV file.

    Parameters
    ----------
    file_path : str
        The source CSV file for Canadian electricity generation mixes.
    pre_process : bool, optional
        Whether to run pre-processing (for Canada Energy Futures CSV files)
        , by default False

    Returns
    -------
    pandas.DataFrame
        A data frame of annual, regional Canadian electricity mixes.
        Columns include:

        - 'Province' (str): Canadian province name (e.g. 'Quebec')
        - 'FuelCategory' (str): The eLCI primary fuel code (e.g. 'COAL')
        - 'FuelCategory_fraction' (float): Generation mix fraction
        - 'Year' (int): The generation year
        - 'Code' (str): The balancing authority abbreviation
        - 'Balancing Authority Name' (str): The balancing authority name

    Raises
    ------
    OSError
        Failed to find the CSV file sent.
    ValueError
        Failed to find the scenario column in the CSV file.

    Notes
    -----
    The original International_ELectricity_Mix.csv file has five columns
    and needs no preprocessing. These columns include:

    - Subregion (str)
    - FuelCategory (str)
    - Electricity (empty)
    - Generation_Ratio (float)
    - Year (int)

    If downloading raw from Canadian Energy Futures website,
    then it has the following columns and need pre-processing:

    - Scenario (str)
    - Region (str)
    - Variable (str): fuel categories
    - Type (str): "PrimaryFuel"
    - Year (int)
    - Value (float)
    - (Unit) only for 2021, should be GWh but we're calculating mix percents

    Other Canadian Energy Future CSV files may be found here:
    - `2020 <https://www.cer-rec.gc.ca/open/energy/energyfutures2020/electricity-generation-2020.csv>`_
    - `2021 <https://www.cer-rec.gc.ca/open/energy/energyfutures2021/electricity-generation-2021.csv>`_
    """
    if not os.path.isfile(file_path):
        raise OSError("Canadian mix file not found! %s" % file_path)

    df = pd.read_csv(file_path)
    if pre_process:
        # These steps are taken from NETL workbook,
        # electricity-generation-2021.xlsx, where a pivot table was
        # implemented to calculate the electricity mix percentages for each
        # variable (fuel category) from each year and region.

        # Filter the scenario
        if 'Current Policy' in df['Scenario'].values:
            # 2021
            q_val = 'Current Policy'
        elif 'Current Measures' in df['Scenario'].values:
            # 2023
            q_val = 'Current Measures'
        elif 'Reference' in df['Scenario'].values:
            # 2019, 2020
            q_val = 'Reference'
        else:
            raise ValueError("Failed to find reference scenario!")
        df = df.query("Scenario == '%s'" % q_val).copy()

        # Fix the variable column name (found in 2019 CSV)
        df = df.rename(columns={'Variable_English': 'Variable'})

        # Calculate the generation ratio
        # the fuel sums
        a = df.groupby(by=['Year', 'Region', 'Variable']).agg({'Value': "sum"})
        # the region sums
        b = df.groupby(by=['Year', 'Region']).agg({'Value': "sum"})
        b = b.rename(columns={'Value': 'Total'})
        # add region sums to region+fuel sums
        a = a.merge(b, left_index=True, right_index=True)
        # calculate percentages
        a['Generation_Ratio'] = a['Value']/a['Total']
        a = a.reset_index()

        # Map fuel category names
        f_cat_map = {
            'Biomass / Geothermal': 'BIOMASS',
            'Coal & Coke': 'COAL',
            'Hydro / Wave / Tidal': 'HYDRO',
            'Natural Gas':  'GAS',
            'Oil':  'OIL',
            'Solar': 'SOLAR',
            'Uranium':  'NUCLEAR',
            'Wind': 'WIND',
        }
        a['FuelCategory'] = a["Variable"].map(f_cat_map)
        if a['FuelCategory'].isna().sum() > 0:
            logging.warning("Failed to match all Canadian fuel categories!")

        # Clean-up unused columns:
        a = a.drop(columns="Variable")
        if "Value" in a.columns:
            a = a.drop(columns="Value")
        if "Total" in a.columns:
            a = a.drop(columns="Total")
        df = a.rename(columns={'Region': 'Subregion'}).copy()

    # Map subregion to BA codes
    # NOTE: Alberta, Saskatchewan (and Mexico and others) are not paired
    #       to a BA because they do not show up in the trading.
    # NOTE: 'Newfoundland and Labrador' has no BA name in BA_Codes.xlsx
    canada_subregion_map = {
        "British Columbia": "BCHA",
        "Quebec": "HQT",
        "Ontario": "IESO",
        "Manitoba": "MHEB",
        "New Brunswick": "NBSO",
        "Newfoundland and Labrador": "NEWL",
    }
    df['Code'] = df["Subregion"].map(canada_subregion_map)
    if df['Code'].isna().sum() > 0:
        m_regions = df[df["Code"].isna()]["Subregion"].unique()
        m_regions = [x for x in m_regions]
        m_regions = ", ".join(m_regions)
        logging.warning(
            "Failed to match Canadian regions (%s) to BA code!" % m_regions)

    # Map BA codes to their names.
    ba_codes = read_ba_codes()
    df["Balancing Authority Name"] = df["Code"].map(ba_codes["BA_Name"])

    # Clean-up step: rename columns, drop unused columns, and drop missing rows
    df = df.rename(
        columns={
            "Subregion": "Province",
            "Generation_Ratio": "FuelCategory_fraction",
        },
    )
    if "Electricity" in df.columns:
        df = df.drop(columns=["Electricity"])
    df = df.dropna(subset=["Code"])

    return df
