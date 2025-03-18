#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# natural_gas_upstream.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import copy
import os

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia923_generation import eia923_download_extract
from electricitylci.generation import add_temporal_correlation_score
from electricitylci.model_config import model_specs
##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module generates the annual upstream emissions from the
extraction, processing, and transportation of uranium for each nuclear plant
in EIA-923.

Created:
    2019-05-31
Last updated:
    2024-01-10
"""
__all__ = [
    "generate_upstream_nuc",
]


##############################################################################
# FUNCTIONS
##############################################################################
def generate_upstream_nuc(year):
    """
    Generate the annual uranium extraction, processing and transportation
    emissions (in kg) for each plant in EIA923.

    Notes
    -----
    Depends on data file, nuclear_lci.csv, which contains the upstream
    emission impacts of a kilogram of uranium.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    pandas.DataFrame
    """
    # Get the EIA generation data for the specified year, this dataset includes
    # the fuel consumption for generating electricity for each facility
    # and fuel type. Filter the data to only include NG facilities and on
    # positive fuel consumption. Group that data by Plant Id as it is possible
    # to have multiple rows for the same facility and fuel based on different
    # prime movers (e.g., gas turbine and combined cycle).
    eia_generation_data = eia923_download_extract(year)

    column_filt = (eia_generation_data["Reported Fuel Type Code"] == "NUC") & (
        eia_generation_data["Net Generation (Megawatthours)"] > 0
    )
    nuc_generation_data = eia_generation_data[column_filt]

    nuc_generation_data = (
        nuc_generation_data.groupby("Plant Id")
        .agg({"Net Generation (Megawatthours)": "sum"})
        .reset_index()
    )
    nuc_generation_data["Plant Id"] = nuc_generation_data[
        "Plant Id"].astype(int)

    # Read the nuclear LCI file
    nuc_lci = pd.read_csv(
        os.path.join(data_dir, "nuclear_lci.csv"),
        index_col=0,
        low_memory=False
    )
    nuc_lci.dropna(subset=["compartment"],inplace=True)

    # There is no column to merge the inventory and generation data on,
    # so we iterate through the plants and make a new column in the lci
    # dataframe using the plant id. And add a copy of that dataframe to a
    # running list. Finally we concatenate all of the dataframes in the list
    # together for a final merged dataframe.
    merged_list = list()
    for _, plant in nuc_generation_data.iterrows():
        nuc_lci["Plant Id"] = plant["Plant Id"]
        merged_list.append(copy.copy(nuc_lci))
    nuc_lci = pd.concat(merged_list)
    nuc_merged = pd.merge(
        left=nuc_lci,
        right=nuc_generation_data,
        left_on=["Plant Id"],
        right_on=["Plant Id"],
        how="left",
    )
    nuc_merged.rename(
        columns={"Net Generation (Megawatthours)": "quantity"},
        inplace=True
    )

    # Convert the inventory per MWh emissions to an annual emission
    nuc_merged["FlowAmount"] = (
        nuc_merged["FlowAmount"] * nuc_merged["quantity"]
    )
    nuc_merged["Electricity"] = nuc_merged["quantity"]

    # Filling out some columns to be consistent with other upstream dataframes
    nuc_merged["fuel_type"] = "Nuclear"
    nuc_merged["stage"] = "mine-to-plant"
    nuc_merged["stage_code"] = "NUC"
    nuc_merged.rename(columns={"unit":"Unit"}, inplace=True)
    nuc_merged.rename(
        columns={"compartment": "Compartment", "Plant Id": "plant_id"},
        inplace=True,
    )
    input_dict={"emission": False, "resource": True}
    nuc_merged["directionality"] = nuc_merged["directionality"].map(input_dict)
    nuc_merged.rename(columns={"directionality":"input"}, inplace=True)
    nuc_merged["Source"]="netlnuceiafuel"

    # Issue #296 - adding DQI information for upstream processes
    # Setting year to be equal to the year that the costs were generated
    # to develop this USEEIO-based inventory
    nuc_merged["Year"] = 2016
    nuc_merged["FlowReliability"] = 3
    nuc_merged["TemporalCorrelation"] = add_temporal_correlation_score(
        nuc_merged["Year"], model_specs.electricity_lci_target_year
    )
    nuc_merged["GeographicalCorrelation"] = 3
    nuc_merged["TechnologicalCorrelation"] = 3
    nuc_merged["DataCollection"] = 4

    return nuc_merged


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    from electricitylci.globals import output_dir
    year = 2016
    df = generate_upstream_nuc(year)
    df.to_csv(output_dir + "/nuclear_emissions_{}.csv".format(year))
