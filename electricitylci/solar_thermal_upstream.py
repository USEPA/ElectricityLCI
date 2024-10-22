#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# solar_thermal_upstream.py
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
# ^^^ requires modelspecs


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module generates the annual emissions of each flow type
for the construction of each solar facility included in EIA 923 based on
solely the upstream contributions. Emissions from the construction of panels
are accounted for elsewhere.

Last updated:
    2024-10-11
"""
__all__ = [
    "generate_upstream_solarthermal",
]

##############################################################################
# GLOBALS
##############################################################################
RENEWABLE_VINTAGE = 2016


##############################################################################
# FUNCTIONS
##############################################################################
def _solar_construction(year):
    """Generate solar thermal construction inventory.

    Parameters
    ----------
    year : int
        The EIA generation year.

    Returns
    -------
    pandas.DataFrame
        Emissions inventory for solar thermal construction.
        Returns NoneType for 2016 renewables vintage---the O&M emissions
        include the construction inventory.

    Raises
    ------
    ValueError
        If renewable vintage year is unsupported.
    """
    # Iss150, new construction and O&M LCIs
    if RENEWABLE_VINTAGE == 2020:
        solar_df = pd.read_csv(
            os.path.join(
                data_dir,
                "renewables",
                "2020",
                "solar_thermal_construction_lci.csv"
                ),
            header=[0, 1]
        )
    elif RENEWABLE_VINTAGE == 2016:
        logging.debug(
            "The 2016 solar thermal LCI did not have separate construction "
            "and O&M.")
        return None
    else:
        raise ValueError("Renewable vintage %s undefined!" % RENEWABLE_VINTAGE)

    columns = pd.DataFrame(solar_df.columns.tolist())
    columns.loc[columns[0].str.startswith('Unnamed:'), 0] = np.nan
    columns[0] = columns[0].ffill()

    solar_df.columns = pd.MultiIndex.from_tuples(
        columns.to_records(index=False).tolist())
    solar_df_t = solar_df.transpose()
    solar_df_t = solar_df_t.reset_index()

    new_columns = solar_df_t.loc[0,:].tolist()
    new_columns[0] = 'Compartment'
    new_columns[1] = 'FlowName'
    solar_df_t.columns = new_columns
    solar_df_t.drop(index=0, inplace=True)

    solar_df_t_melt = solar_df_t.melt(
        id_vars=['Compartment','FlowName'],
        var_name='plant_id',
        value_name='FlowAmount'
    )

    # HOTFIX: drop row where flow name is state and the value is the
    # state abbreviation [241011; TWD]
    to_drop = solar_df_t_melt[solar_df_t_melt['FlowName'] == 'State']
    solar_df_t_melt = solar_df_t_melt.drop(to_drop.index)
    solar_df_t_melt = solar_df_t_melt.astype({
        'plant_id' : int,
        'FlowAmount': float,
    })

    solar_generation_data = _solar_generation(year)
    solarthermal_upstream = solar_df_t_melt.merge(
        right=solar_generation_data,
        left_on='plant_id',
        right_on='Plant Id',
        how='left'
    )
    solarthermal_upstream.rename(
        columns={'Net Generation (Megawatthours)': 'Electricity'},
        inplace=True
    )
    solarthermal_upstream["quantity"] = solarthermal_upstream["Electricity"]
    solarthermal_upstream.drop(
        columns=[
            'Plant Id',
            'NAICS Code',
            'Reported Fuel Type Code',
            'YEAR',
            'Total Fuel Consumption MMBtu',
            'State',
            'Plant Name',
        ],
        inplace=True
    )
    # These emissions will later be aggregated with any inventory power plant
    # emissions because each facility has its own construction impacts.
    # Iss150, new construction stage code
    solarthermal_upstream['stage_code'] = "solar_thermal_const"
    solarthermal_upstream['fuel_type'] = 'SOLARTHERMAL'
    compartment_map = {
        'Air':'air',
        'Water':'water',
        'Energy':'input'
    }
    solarthermal_upstream['Compartment'] = solarthermal_upstream[
        'Compartment'].map(compartment_map)
    solarthermal_upstream["Unit"] = "kg"
    solarthermal_upstream["input"] = False

    return solarthermal_upstream


def _solar_generation(year):
    """Return the EIA generation data for solar thermal power plants.

    For 2016 renewables vintage, construction emissions are included.

    Parameters
    ----------
    year : int
        EIA generation year.

    Returns
    -------
    pandas.DataFrame
        EIA generation data for solar thermal power plants.
    """
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data['Plant Id'] = eia_generation_data[
        'Plant Id'].astype(int)

    column_filt = (eia_generation_data['Reported Fuel Type Code'] == 'SUN')
    df = eia_generation_data.loc[column_filt, :]

    return df


def _solar_om(year):
    """Generate the operations and maintenance LCI for solar thermal power
    plants. For 2016 electricity baseline, this data frame includes the
    construction inventory.

    Parameters
    ----------
    year : int
        EIA generation year.

    Returns
    -------
    pandas.DataFrame
        Emission inventory for solar thermal power plant O&M.

    Raises
    ------
    ValueError
        If the renwables vintage is not defined or a valid year.
    """
    # Iss150, new construction and O&M LCIs
    if RENEWABLE_VINTAGE == 2020:
        solar_ops_df = pd.read_csv(
            os.path.join(
                data_dir,
                "renewables",
                "2020",
                "solar_thermal_om_lci.csv"
                ),
            header=[0, 1],
            na_values=["#VALUE!", "#DIV/0!"],
        )
    elif RENEWABLE_VINTAGE == 2016:
        solar_ops_df = pd.read_csv(
            os.path.join(
                data_dir,
                "renewables",
                "2016",
                "solar_thermal_inventory.csv"
                ),
            header=[0, 1]
        )
    else:
        raise ValueError("Renewable vintage %s undefined!" % RENEWABLE_VINTAGE)

    # Correct the columns
    columns = pd.DataFrame(solar_ops_df.columns.tolist())
    columns.loc[columns[0].str.startswith('Unnamed:'), 0] = np.nan
    columns[0] = columns[0].ffill()

    solar_ops_df.columns = pd.MultiIndex.from_tuples(
        columns.to_records(index=False).tolist())
    solar_ops_df_t = solar_ops_df.transpose()
    solar_ops_df_t = solar_ops_df_t.reset_index()

    # Make facilities the columns
    new_columns = solar_ops_df_t.loc[0,:].tolist()
    new_columns[0] = 'Compartment'
    new_columns[1] = 'FlowName'
    solar_ops_df_t.columns = new_columns
    solar_ops_df_t.drop(index=0, inplace=True)

    # Make the rows flows by facility
    solar_ops_df_t_melt = solar_ops_df_t.melt(
        id_vars=['Compartment','FlowName'],
        var_name='plant_id',
        value_name='FlowAmount'
    )
    # HOTFIX: drop row where flow name is state and the value is the
    # state abbreviation [241011; TWD]
    to_drop = solar_ops_df_t_melt[solar_ops_df_t_melt['FlowName'] == 'State']
    solar_ops_df_t_melt = solar_ops_df_t_melt.drop(to_drop.index)
    solar_ops_df_t_melt = solar_ops_df_t_melt.astype({
        'plant_id' : int,
        'FlowAmount': float,
    })

    # Unlike the construction inventory, operations are on the basis of
    # per MWh, so in order for the data to integrate correctly with the
    # rest of the inventory, we need to multiply all inventory by electricity
    # generation (in MWh) for the target year.
    solar_generation_data = _solar_generation(year)
    solarthermal_ops = solar_ops_df_t_melt.merge(
        right=solar_generation_data,
        left_on='plant_id',
        right_on='Plant Id',
        how='left'
    )
    solarthermal_ops.rename(columns={
        'Net Generation (Megawatthours)': 'Electricity'},
        inplace=True,
    )
    solarthermal_ops["quantity"] = solarthermal_ops["Electricity"]
    solarthermal_ops["FlowAmount"] = (
        solarthermal_ops["FlowAmount"]*solarthermal_ops["Electricity"]
    )
    solarthermal_ops.drop(
        columns=[
            'Plant Id',
            'NAICS Code',
            'Reported Fuel Type Code',
            'YEAR',
            'Total Fuel Consumption MMBtu',
            'State',
            'Plant Name'],
        inplace=True
    )

    solarthermal_ops['stage_code']="Power plant"
    solarthermal_ops['fuel_type']='SOLARTHERMAL'
    compartment_map={
        'Air':'air',
        'Water':'water',
        'Energy':'input'
    }
    solarthermal_ops['Compartment'] = solarthermal_ops['Compartment'].map(
        compartment_map)
    solarthermal_ops["Unit"] = "kg"
    solarthermal_ops["input"] = False

    return solarthermal_ops


def generate_upstream_solarthermal(year):
    """
    Generate the annual emissions.

    For solar thermal plant construction for each plant in EIA923. The emissions
    inventory file has already allocated the total emissions to construct the
    entire power plant over its assumed 30 year life. So the emissions returned
    below represent 1/30th of the total site construction emissions.

    Notes
    -----
    Depends on the data file, solar_thermal_inventory.csv, which contains
    emissions and waste streams for each facility in the United States as of
    2016.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    pandas.DataFrame
    """
    logging.info("Generating upstream solar thermal inventories")
    solarthermal_ops = _solar_om(year)
    solarthermal_cons = _solar_construction(year)
    if solarthermal_cons is not None:
        solarthermal_df = pd.concat(
            [solarthermal_cons, solarthermal_ops],
            ignore_index=True
        )
    else:
        solarthermal_df = solarthermal_ops

    # HOTFIX: add unique source code for renewables
    solarthermal_df["Source"] = "netlsolarthermal"

    # HOTFIX the electricity column as an input.
    solarthermal_df.loc[
        solarthermal_df["FlowName"]=="Electricity", "input"] = True
    # HOTFIX electricity resource units.
    solarthermal_df.loc[
        solarthermal_df["FlowName"]=="Electricity", "Unit"] = "MWh"
    # HOTFIX water as an input (Iss147).
    #   These are the negative water-to-water emissions.
    water_filter = (solarthermal_df['Compartment'] == 'water') & (
        solarthermal_df['FlowAmount'] < 0) & (
            solarthermal_df['FlowName'].str.startswith('Water'))
    solarthermal_df.loc[water_filter, 'input'] = True
    solarthermal_df.loc[water_filter, 'FlowAmount'] *= -1.0

    return solarthermal_df


##############################################################################
# MAIN
##############################################################################
if __name__=='__main__':
    from electricitylci.globals import output_dir
    year = 2020
    df = generate_upstream_solarthermal(year)
    df.to_csv(f'{output_dir}/upstream_solarthermal_{year}.csv')
