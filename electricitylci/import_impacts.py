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

Last updated:
    2024-03-08
"""
__all__ = [
    "generate_canadian_mixes",
]


logger = logging.getLogger("import_impacts")


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
    from electricitylci.combinator import BA_CODES

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

    canada_subregion_map = {
        "British Columbia": "BCHA",
        "Quebec": "HQT",
        "Ontario": "IESO",
        "Manitoba": "MHEB",
        "New Brunswick": "NBSO",
        "Newfoundland and Labrador": "NEWL",
    }

    logger.info("Generating inventory for Canadian balancing authority areas")
    # NOTE: this file is also read by egrid_facilities.py
    import_mix = pd.read_csv(
        os.path.join(data_dir, "International_Electricity_Mix.csv"),
    )
    # BUG: missing 2021 and beyond data.
    import_mix = import_mix.loc[import_mix["Year"] == gen_year, :]
    canadian_mix = import_mix
    canadian_mix["Code"] = canadian_mix["Subregion"].map(canada_subregion_map)
    baa_codes = list(canadian_mix["Code"].unique())
    canadian_mix["Balancing Authority Name"] = canadian_mix["Code"].map(
        BA_CODES["BA_Name"]
    )
    canadian_mix = canadian_mix.rename(
        columns={
            "Subregion": "Province",
            "Generation_Ratio": "FuelCategory_fraction",
        },
        errors="ignore",
    )
    canadian_mix = canadian_mix.drop(columns=["Electricity"],errors="ignore")
    canadian_mix = canadian_mix.dropna(subset=["Code"])

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
    for baa_code in baa_codes:
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
        ca_mix_list.append(ca_inventory)
    blank_df = pd.DataFrame(columns=us_inventory.columns)

    ca_mix_inventory = pd.concat(ca_mix_list + [blank_df], ignore_index=True)
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
    ca_mix_inventory["Source"] = "netl"
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

    return ca_mix_inventory


def read_canadian_mix(file_path, pre_process=False):
    # IN PROGRESS
    # The original International_ELectricity_Mix.csv has five columns and
    # need no preprocessing.
    # - Subregion (str)
    # - FuelCategory (str)
    # - Electricity (empty)
    # - Generation_Ratio (float)
    # - Year (int)
    #
    # If downloading raw from Canadian Energy Futures website,
    # then it has the following columns and need pre-processing.
    # - Scenario (str)
    # - Region (str)
    # - Variable (str): fuel categories
    # - Type (str): "PrimaryFuel"
    # - Year (int)
    # - Value (float)
    # - (Unit) only for 2021, should be GWh but we're calculating mix percents
    #
    if not os.path.isfile(file_path):
        raise OSError("Canadian mix file not found! %s" % file_path)

    df = pd.read_csv(file_path)
    if pre_process:
        # These steps are taken from NETL workbook, where a pivot table was
        # implemented to calculate the electricity mix percentages for each
        # variable (fuel category) from each year and region
        df = pd.read_csv(file_path)

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

        # Calculate the generation ratio
        # The fuel sums
        a = df.groupby(by=['Year', 'Region', 'Variable']).agg({'Value': "sum"})
        # The region sums
        b = df.groupby(by=['Year', 'Region']).agg({'Value': "sum"})
        b = b.rename(columns={'Value': 'Total'})
        # Add region sums to region+fuel sums
        a = a.merge(b, left_index=True, right_index=True)
        # Calculate percentages
        a['Generation_Ratio'] = a['Value']/a['Total']

        # TODO:
        # map fuel category names
        # map balancing authority names


if __name__ == '__main__':
    #
    # Because it is assumed that these data files are model results,
    # the most recent is probably the best option, even for historical
    # mixes.
    url20 = "https://www.cer-rec.gc.ca/open/energy/energyfutures2020/electricity-generation-2020.csv"
    url21 = "https://www.cer-rec.gc.ca/open/energy/energyfutures2021/electricity-generation-2021.csv"
    url23 = "https://www.cer-rec.gc.ca/open/energy/energyfutures2023/electricity-generation-2023.csv"

    from electricitylci.globals import paths
    from electricitylci.utils import download
    from electricitylci.utils import check_output_dir

    # User selects one of the URLs
    url = url23
    f_name = os.path.basename(url)
    out_dir = os.path.join(paths.local_path, "energyfutures")
    is_dir = check_output_dir(out_dir)
    if is_dir:
        out_path = os.path.join(out_dir, f_name)
        if not os.path.exists(out_path):
            # Download CSV if not available
            r = download(url, out_path)
            if r:
                logging.info("Downloaded Canadian Energy Futures CSV")
            else:
                logging.error(
                    "Failed to download Canadian Energy Future CSV: %s" % url)
        if os.path.exists(out_path):
            df = pd.read_csv(out_path)
