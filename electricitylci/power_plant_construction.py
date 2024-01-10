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
    2024-01-10
"""
__all__ = [
    "generate_power_plant_construction",
]


##############################################################################
# FUNCTIONS
##############################################################################
def generate_power_plant_construction(year):
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
        reporting to EIA.
    """
    gen_df = eia860_generator_info(year)
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

    gen_df = gen_df.loc[
        gen_df["energy_source_1"].isin(energy_sources), gen_columns]
    gen_df["plant_id"] = gen_df["plant_id"].astype(int)
    groupby_cols = ["plant_id", "technology", "energy_source_1", "prime_mover"]
    gen_df_group = gen_df.groupby(
        by=groupby_cols,
        as_index=False)["nameplate_capacity_mw"].sum()
    prime_energy_combo = gen_df_group.groupby(
        by=["prime_mover", "energy_source_1"]
    ).size().reset_index().rename(columns={0: 'count'})

    prime_energy_combo["const_type"] = "coal"
    gas_const_criteria = (
        prime_energy_combo["prime_mover"].isin(gas_prime)) & (
        ~prime_energy_combo["energy_source_1"].isin(coal_type))
    prime_energy_combo.loc[gas_const_criteria, "const_type"] = "ngcc"
    gen_df_group = gen_df_group.merge(
        prime_energy_combo[['prime_mover', 'energy_source_1', 'const_type']],
        on=["prime_mover","energy_source_1"],
        how="left"
    )

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

    inventory = pd.concat([scpc_inventory, ngcc_inventory])
    inventory["Compartment_path"] = inventory["Compartment_path"].map(
        compartment_mapping)
    inventory["input"] = False
    input_list=["resource" in x for x in inventory["Compartment"]]
    inventory["input"] = input_list

    construction_df = gen_df_group.merge(
        inventory,
        on="const_type",
        how="left"
    )
    construction_df["FlowAmount"] = construction_df[
        "FlowAmount"] * construction_df["nameplate_capacity_mw"]
    construction_df.rename(
        columns={"nameplate_capacity_mw":"quantity"},
        inplace=True)
    construction_df.drop(
        columns=["const_type","energy_source_1","prime_mover"],
        inplace=True)
    construction_df["fuel_type"] = "Construction"
    construction_df["Unit"] = construction_df["Unit"].str.replace(
        "mj","MJ", regex=False)

    return construction_df


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    year=2016
    df = generate_power_plant_construction(year)
