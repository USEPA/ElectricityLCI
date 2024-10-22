#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# wind_upstream.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import numpy as np
import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia923_generation import eia923_download_extract


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module generates the annual emissions of each flow type
for wind farm construction for each plant in EIA 923 based on upstream
contributions.

Last updated:
    2024-10-22
"""
__all__ = [
    "aggregate_wind",
    "fix_wind",
    "generate_upstream_wind",
    "get_wind_construction",
    "get_wind_generation",
    "get_wind_om",
]


##############################################################################
# GLOBALS
##############################################################################
RENEWABLE_VINTAGE = 2020


##############################################################################
# FUNCTIONS
##############################################################################
def fix_wind(df):
    """Apply data frame fixes for upstream wind LCI.

    1. Applies a constant value, "netlnrelwind", as Source.
    2. Sets 'Electricity' flows as inputs in units of MWh.
    3. Corrects negative water-to-water emissions as positive inputs.
    4. Assigns compartment paths.

    Parameters
    ----------
    df : pandas.DataFrame
        Life cycle inventory of upstream wind (e.g., construction + O&M).
        The data frame must have columns labeled 'FlowName', 'Compartment,
        'input', 'Unit', and 'FlowAmount', which are used as filters for
        querying and updating rows.

    Returns
    -------
    pandas.DataFrame
        The same data frame that was received with updated values.
    """
    # Give unique source code
    df["Source"] = "netlnrelwind"

    # Set the compartment paths (only to air and water)
    air_c = df['Compartment'] == 'air'
    water_c = df['Compartment'] == 'water'
    df['Compartment_path'] = ""
    df.loc[air_c, 'Compartment_path'] = "emission/air"
    df.loc[water_c, 'Compartment_path'] = "emission/water"

    # Set Electricity as input and correct its units.
    df.loc[df["FlowName"]=="Electricity", "input"] = True
    df.loc[df["FlowName"]=="Electricity", "Unit"] = "MWh"
    df.loc[df["FlowName"]=="Electricity", "Compartment_path"] = "input"

    # HOTFIX water as an input (Iss147).
    #   These are the negative water-to-water emissions.
    water_filter = (df['Compartment'] == 'water') & (
        df['FlowAmount'] < 0) & (df['FlowName'].str.startswith('Water'))
    df.loc[water_filter, 'input'] = True
    df.loc[water_filter, 'FlowAmount'] *= -1.0
    df.loc[water_filter, 'Compartment_path'] = "resource"

    return df


def get_wind_construction(year):
    """Generate wind farm construction inventory.

    The construction UP functional unit is normalized per year.

    Parameters
    ----------
    year : int
        The EIA generation year.

    Returns
    -------
    pandas.DataFrame
        Emissions inventory for wind farm construction.
        Returns NoneType for 2016 renewable vintage---the O&M emissions
        include the construction inventory.

    Raises
    ------
    ValueError
        If renewable vintage year is unsupported.
    """
    # Iss150, new construction and O&M LCIs
    if RENEWABLE_VINTAGE == 2020:
        logging.info("Reading 2020 upstream wind construction inventory.")
        wind_df = pd.read_csv(
            os.path.join(
                data_dir,
                "renewables",
                "2020",
                "wind_farm_construction_lci.csv"
                ),
            header=[0, 1],
            low_memory=False,
        )
    elif RENEWABLE_VINTAGE == 2016:
        logging.info(
            "The 2016 wind LCI does not separate construction and O&M."
            "Returning none.")
        return None
    else:
        raise ValueError("Renewable vintage %s undefined!" % RENEWABLE_VINTAGE)

    columns = pd.DataFrame(wind_df.columns.tolist())
    columns.loc[columns[0].str.startswith("Unnamed:"), 0] = np.nan
    columns[0] = columns[0].ffill()
    wind_df.columns = pd.MultiIndex.from_tuples(
        columns.to_records(index=False).tolist()
    )

    wind_df_t = wind_df.transpose()
    wind_df_t = wind_df_t.reset_index()
    new_columns = wind_df_t.loc[0, :].tolist()
    new_columns[0] = "Compartment"
    new_columns[1] = "FlowName"
    wind_df_t.columns = new_columns
    wind_df_t.drop(index=0, inplace=True)

    wind_df_t_melt = wind_df_t.melt(
        id_vars=["Compartment", "FlowName"],
        var_name="plant_id",
        value_name="FlowAmount",
    )

    # HOTFIX: remove the "State" and "County" rows
    # and drop the ~9 extra blank cols that were captured from the CSV
    to_drop = wind_df_t_melt[
        (wind_df_t_melt['FlowName'] == 'State') | (
            wind_df_t_melt['FlowName'] == 'County')
    ]
    wind_df_t_melt = wind_df_t_melt.drop(to_drop.index)
    wind_df_t_melt = wind_df_t_melt.dropna(axis=0, subset='FlowAmount')

    # Fix data types
    wind_df_t_melt = wind_df_t_melt.astype({
        'plant_id' : int,
        'FlowAmount': float,
    })

    wind_generation_data = get_wind_generation(year)
    wind_upstream = wind_df_t_melt.merge(
        right=wind_generation_data,
        left_on="plant_id",
        right_on="Plant Id",
        how="left",
    )
    wind_upstream.rename(
        columns={"Net Generation (Megawatthours)": "quantity"},
        inplace=True
    )
    wind_upstream["Electricity"] = wind_upstream["quantity"]
    wind_upstream.drop(
        columns=[
            "Plant Id",
            "NAICS Code",
            "Reported Fuel Type Code",
            "YEAR",
            "Total Fuel Consumption MMBtu",
            "State",
            "Plant Name",
        ],
        inplace=True,
    )

    # These emissions will later be aggregated with any inventory power plant
    # emissions because each facility has its own construction impacts.
    wind_upstream["stage_code"] = "wind_const"
    wind_upstream["fuel_type"] = "WIND"
    compartment_map = {"Air": "air", "Water": "water", "Energy": "input"}
    wind_upstream["Compartment"] = wind_upstream["Compartment"].map(
        compartment_map
    )
    wind_upstream["input"] = False
    wind_upstream["Unit"] = "kg"

    wind_upstream = fix_wind(wind_upstream)

    return wind_upstream


def get_wind_generation(year):
    """Return the EIA generation data for wind power plants.

    Parameters
    ----------
    year : int
        EIA generation year.

    Returns
    -------
    pandas.DataFrame
        EIA generation data for wind power plants.

        Columns include:

        -   'EIA Sector Number': Classification system for electricity
            generation sources (e.g., 1: 'coal', 2: 'natural gas',
            3: 'nuclear', 4: 'oil', 5--10: 'renewable').
        -   'Reported Prime Mover': The source of mechanical energy used to
            generate electricity (e.g., steam turbine powered by coal or
            natural gas or a wind turbine directly powering an electrical
            generator). 'WT' == wind turbine. 'WS' == solar + wind turbine.
    """
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data["Plant Id"] = eia_generation_data[
        "Plant Id"].astype(int)

    column_filt = eia_generation_data["Reported Fuel Type Code"] == "WND"
    df = eia_generation_data.loc[column_filt, :]

    return df


def get_wind_om(year):
    """Generate the operations and maintenance LCI for wind farm power plants.
    For 2016 electricity baseline, this data frame includes the construction
    inventory.

    Parameters
    ----------
    year : int
        EIA generation year.

    Returns
    -------
    pandas.DataFrame
        Emission inventory for wind farm power plant O&M.

    Raises
    ------
    ValueError
        If the renewable vintage is not defined or a valid year.
    """
    # Iss150, new construction and O&M LCIs
    if RENEWABLE_VINTAGE == 2020:
        wind_ops_df = pd.read_csv(
            os.path.join(
                data_dir,
                "renewables",
                "2020",
                "wind_farm_om_lci.csv"
                ),
            header=[0, 1],
            na_values=["#VALUE!", "#DIV/0!"],
        )
    elif RENEWABLE_VINTAGE == 2016:
        wind_ops_df = pd.read_csv(
            os.path.join(
                data_dir,
                "renewables",
                "2016",
                "wind_inventory.csv"
            ),
            header=[0,1],
        )
    else:
        raise ValueError("Renewable vintage %s undefined!" % RENEWABLE_VINTAGE)

    # Fix columns
    columns = pd.DataFrame(wind_ops_df.columns.tolist())
    columns.loc[columns[0].str.startswith("Unnamed:"), 0] = np.nan
    columns[0] = columns[0].ffill()
    wind_ops_df.columns = pd.MultiIndex.from_tuples(
        columns.to_records(index=False).tolist()
    )
    wind_ops_df.columns = pd.MultiIndex.from_tuples(
        columns.to_records(index=False).tolist()
    )

    # Make facilities the columns
    wind_ops_df_t = wind_ops_df.transpose()
    wind_ops_df_t = wind_ops_df_t.reset_index()
    new_columns = wind_ops_df_t.loc[0, :].tolist()
    new_columns[0] = "Compartment"
    new_columns[1] = "FlowName"
    wind_ops_df_t.columns = new_columns
    wind_ops_df_t.drop(index=0, inplace=True)

    # Make the rows the flows by facility
    wind_ops_df_t_melt = wind_ops_df_t.melt(
        id_vars=["Compartment", "FlowName"],
        var_name="plant_id",
        value_name="FlowAmount",
    )

    # HOTFIX: remove the "State" and "County" flows.
    # Also drop the NaNs, which correspond to facilities with no data.
    to_drop = wind_ops_df_t_melt[
        (wind_ops_df_t_melt['FlowName'] == 'State') | (
            wind_ops_df_t_melt['FlowName'] == 'County')
    ]
    wind_ops_df_t_melt = wind_ops_df_t_melt.drop(to_drop.index)
    wind_ops_df_t_melt = wind_ops_df_t_melt.dropna(axis=0, subset="FlowAmount")

    # Fix the data types
    wind_ops_df_t_melt = wind_ops_df_t_melt.astype({
        'plant_id' : int,
        'FlowAmount': float,
    })

    wind_generation_data = get_wind_generation(year)
    wind_ops = wind_ops_df_t_melt.merge(
        right=wind_generation_data,
        left_on="plant_id",
        right_on="Plant Id",
        how="left",
    )
    wind_ops.rename(
        columns={"Net Generation (Megawatthours)": "quantity"},
        inplace=True
    )
    wind_ops["Electricity"] = wind_ops["quantity"]

    # Unlike the construction inventory, operations are on the basis of
    # per MWh, so in order for the data to integrate correctly with the
    # rest of the inventory, we need to multiply all inventory by electricity
    # generation (in MWh) for the target year.
    wind_ops["FlowAmount"] = wind_ops["FlowAmount"]*wind_ops["Electricity"]
    wind_ops.drop(
        columns=[
            "Plant Id",
            "NAICS Code",
            "Reported Fuel Type Code",
            "YEAR",
            "Total Fuel Consumption MMBtu",
            "State",
            "Plant Name",
        ],
        inplace=True,
    )

    wind_ops["stage_code"] = "Power plant"
    wind_ops["fuel_type"] = "WIND"
    compartment_map = {"Air": "air", "Water": "water", "Energy": "input"}
    wind_ops["Compartment"] = wind_ops["Compartment"].map(
        compartment_map
    )
    wind_ops["input"] = False
    wind_ops["Unit"]="kg"

    wind_ops = fix_wind(wind_ops)

    return wind_ops


def generate_upstream_wind(year):
    """
    Generate the annual emissions.

    For wind farm construction for each plant in EIA923. The emissions inventory
    file has already allocated the total emissions to construct the turbines and
    balance of system for the entire wind farm over the assumed 20 year life of
    the panels. So the emissions returned below represent 1/20th of the total
    site construction emissions.

    Notes
    -----
    Depends on the data file, wind_inventory.csv, which contains flow amounts
    for all emissions and waste streams at each wind facility in the U.S.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    pd.DataFrame
    """
    logging.info("Generating upstream wind inventories")
    wind_cons = get_wind_construction(year)
    wind_ops = get_wind_om(year)

    if wind_cons is not None:
        wind_df = pd.concat(
            [wind_cons, wind_ops],
            ignore_index=True
        )
    else:
        wind_df = wind_ops

    return wind_df


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    from electricitylci.globals import output_dir
    year = 2020
    wind_upstream = generate_upstream_wind(year)
    wind_upstream.to_csv(f"{output_dir}/upstream_wind_{year}.csv")
