#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# power_plant_construction.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia860_facilities import eia860_generator_info


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module maps per year/per MW impacts from power plant
construction to the fossil power generators in the United States and produces
a data frame that permits the calculation of total construction impact.

Last edited:
    2024-10-22
"""
__all__ = [
    "generate_power_plant_construction",
]


##############################################################################
# FUNCTIONS
##############################################################################
def generate_power_plant_construction(year, incl_renew=False):
    # IN PROGRESS --- TODO: add remaining construction plants
    # Pull the original coal and ngcc power plant construction processes.
    construction_df = get_coal_ngcc_const(year)

    # NEW: Issue#150; pull in renewable power plant construction.
    if incl_renew:
        # Wind has 'EIA Sector Number', 'Reported Prime Mover', and
        # 'Electricity' and does not have 'technology'---the latter is
        # not used.
        from electricitylci.wind_upstream import get_wind_construction
        wind_const = get_wind_construction(year)
        wind_const = wind_const.drop(
            columns=[
                'EIA Sector Number',
                'Reported Prime Mover',
                'Electricity',
            ]
        )
        wind_const['technology'] = ''
        wind_const['fuel_type'] = 'Construction'
        construction_df =  pd.concat(
            [construction_df, wind_const], ignore_index=True)

    return construction_df


def get_coal_ngcc_const(year):
    """
    An NETL study used to generate the life cycle inventory for power plant construction using an economic input output model.

    Two types of plants are considered:

    -   sub-critical pulverized coal and
    -   a natural gas combined cycle plant.

    The inventory provided by the study is for an entire plant. This
    inventory is divided by the net generation capacity of those plants
    to place the inventory on the basis of a MW and then divided by an
    assumed plant life of 30 years, which is a conservative assumption
    considering the lifetime of these plants is typically much longer.
    These per year/per MW impacts are mapped to the fossil power generators
    in the U.S. where they are scaled by the net generating capacity of
    the plants (as provided by EIA data). These impacts are eventually
    divided by the generation for the year in MWh to provide the
    construction impacts on the basis of the functional unit.

    Notes
    -----
    Depends on data file, plant_construction_inventory.csv, which contains
    life cycle flow amounts for both SCPC and NGCC power plant construction.

    Parameters
    ----------
    year : int
        Year of EIA data to use to provide net generating capacity

    Returns
    -------
    pandas.DataFrame
        This dataframe provides construction inventory for each power plant
        reporting to EIA. Columns include:

        - 'plant_id' (int): EIA860 plant identifier
        - 'technology' (str): coal, nat. gas, petroleum, or other plant type
        - 'quantity' (float): nameplate capacity, MW
        - 'FlowAmount' (float): flow amount
        - 'Unit' (str): units of flow
        - 'Compartment_path' (str): resource or emission path (to air, soil)
        - 'FlowName' (str): flow name
        - 'Compartment' (str): resource, air, water, or soil
        - 'stage_code' (str): 'coal_const' or 'ngcc_const'
        - 'input' (bool): true for resources; false otherwise
        - 'fuel_type' (str): 'Construction'
    """
    gen_columns=[
        "plant_id",
        "generator_id",
        "technology",
        "prime_mover",
        "nameplate_capacity_mw",
        "energy_source_1"
    ]
    energy_sources=[
        "NG",
        'BIT',
        'DFO',
        'LIG',
        'SUB',
        'RC',
        'KER',
        'RFO',
        'PC',
        'WC'
    ]
    compartment_mapping={
        'resource/in ground':"resource",
        'resource':"resource",
        'resource/in water':"resource",
        'resource/in air':"resource",
        'air/unspecified':"emission/air",
        'resource/land':"resource",
        'water/unspecified':"emission/water",
        'air/low population density':"emission/air",
        'soil/groundwater':"emission/water",
        'air/unspecified/2,4':"emission/air",
        'soil/unspecified':"emission/soil",
        'soil/industrial':"emission/soil",
        'soil/unspecified/2,4':"emission/soil",
        'water/unspecified/2,4':"emission/water",
        '/':"",
        'resource/groundwater':"resource",
        'resource/surface water':"resource",
        'water/surface water':"resource"
    }
    gas_prime = ["GT","IC","OT","CT","CS","CE","CA","ST"]
    coal_type = ["BIT","SUB","LIG","WC","RC"]

    # Read EIA generator info for the given year---use this to
    # query relevant facilities to be linked to plant construction.
    gen_df = eia860_generator_info(year)
    gen_df = gen_df.loc[
        gen_df["energy_source_1"].isin(energy_sources), gen_columns]

    # Correct data type for merging.
    gen_df["plant_id"] = gen_df["plant_id"].astype(int)

    # Get facility-fuel-mover total nameplate capacities
    groupby_cols = ["plant_id", "technology", "energy_source_1", "prime_mover"]
    gen_df_group = gen_df.groupby(
        by=groupby_cols,
        as_index=False)["nameplate_capacity_mw"].sum()

    # Determine how many prime movers there are for each plant-technology.
    prime_energy_combo = gen_df_group.groupby(
        by=["prime_mover", "energy_source_1"]
    ).size().reset_index().rename(columns={0: 'count'})

    # Assign the construction fuel types (coal and ngcc)
    prime_energy_combo["const_type"] = "coal"
    gas_const_criteria = (
        prime_energy_combo["prime_mover"].isin(gas_prime)) & (
        ~prime_energy_combo["energy_source_1"].isin(coal_type))
    prime_energy_combo.loc[gas_const_criteria, "const_type"] = "ngcc"

    # Add construction type to the grouped database.
    gen_df_group = gen_df_group.merge(
        prime_energy_combo[['prime_mover', 'energy_source_1', 'const_type']],
        on=["prime_mover","energy_source_1"],
        how="left"
    )

    # Read the construction LCI (for coal and ngcc) and expand the columns.
    inventory = pd.read_csv(
        os.path.join(data_dir, "plant_construction_inventory.csv"),
        low_memory=False
    )
    inventory = pd.concat(
        [
            inventory,
            inventory["Flow"].str.rsplit("/", n=1, expand=True)
        ],
        axis=1
    ).drop(columns=["Flow"]).rename(columns={0:"Flow", 1:"Unit"})
    inventory = pd.concat(
        [
            inventory,
            inventory["Flow"].str.rsplit('/', n=1, expand=True)
        ],
        axis=1
    ).drop(columns=["Flow"]).rename(
        columns={0:"Compartment_path", 1:"FlowName"})
    inventory = pd.concat(
        [
            inventory,
            inventory["Compartment_path"].str.split('/', n=1, expand=True)
        ],
        axis=1
    ).rename(
        columns={0:"Compartment", 1:"delete"}).drop(columns="delete")

    # Get the coal power plant emissions, divide by capacity (550 MW) and
    # lifetime (30 yr)
    scpc_inventory = inventory[[
        'SCPC_550_MW',
        'Unit',
        'Compartment_path',
        'FlowName',
        'Compartment'
    ]]
    scpc_inventory["const_type"] = "coal"
    scpc_inventory["stage_code"] = "coal_const"
    scpc_inventory.rename(columns={"SCPC_550_MW":"FlowAmount"}, inplace=True)
    scpc_inventory["FlowAmount"] = scpc_inventory["FlowAmount"]/30/550

    # Get the natural gas plant emissions, divide by capacity (630 MW) and
    # lifetime (30 yr)
    ngcc_inventory = inventory[[
        'NGCC_630_MW',
        'Unit',
        'Compartment_path',
        'FlowName',
        'Compartment'
    ]]
    ngcc_inventory["const_type"] = "ngcc"
    ngcc_inventory["stage_code"] = "ngcc_const"
    ngcc_inventory.rename(columns={"NGCC_630_MW":"FlowAmount"}, inplace=True)
    ngcc_inventory["FlowAmount"] = ngcc_inventory["FlowAmount"]/30/630

    # Concatenate the coal and ngcc inventories, correct compartments & inputs
    inventory = pd.concat([scpc_inventory, ngcc_inventory])
    inventory["Compartment_path"] = inventory["Compartment_path"].map(
        compartment_mapping)
    inventory["input"] = False
    input_list=["resource" in x for x in inventory["Compartment"]]
    inventory["input"] = input_list

    # Merge facility-level data with construction data.
    construction_df = gen_df_group.merge(
        inventory,
        on="const_type",
        how="left"
    )

    # Scale emissions to each individual facility (actual/modeled)
    construction_df["FlowAmount"] = construction_df[
        "FlowAmount"] * construction_df["nameplate_capacity_mw"]
    construction_df.rename(
        columns={"nameplate_capacity_mw": "quantity"},
        inplace=True)
    construction_df.drop(
        columns=["const_type", "energy_source_1", "prime_mover"],
        inplace=True)
    construction_df["fuel_type"] = "Construction"
    construction_df["Unit"] = construction_df["Unit"].str.replace(
        "mj","MJ", regex=False)
    construction_df["Source"]="netlconst"

    return construction_df


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    year=2016
    df = generate_power_plant_construction(year)
