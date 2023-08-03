# -*- coding: utf-8 -*-
"""
Created on Fri May 31 16:10:00 2019

@author: Matt Jamieson
"""
import copy

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia923_generation import eia923_download_extract


def generate_upstream_nuc(year):
    """
    Generate the annual uranium extraction, processing and transportation
    emissions (in kg) for each plant in EIA923.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    dataframe
    """

    # Get the EIA generation data for the specified year, this dataset includes
    # the fuel consumption for generating electricity for each facility
    # and fuel type. Filter the data to only include NG facilities and on positive
    # fuel consumption. Group that data by Plant Id as it is possible to have
    # multiple rows for the same facility and fuel based on different prime
    # movers (e.g., gas turbine and combined cycle).
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
    nuc_generation_data["Plant Id"] = nuc_generation_data["Plant Id"].astype(
        int
    )

    # Read the nuclear LCI excel file
    nuc_lci = pd.read_csv(data_dir + "/nuclear_lci.csv", index_col=0,low_memory=False)
    nuc_lci.dropna(subset=["compartment"],inplace=True)
    # There is no column to merge the inventory and generation data on,
    # so we iterate through the plants and make a new column in the lci
    # dataframe using the plant id. And add a copy of that dataframe to a
    # running list. Finally we concatenate all of the dataframes in the list
    # together for a final merged dataframe.
    merged_list = list()
    for idx1, plant in nuc_generation_data.iterrows():
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
        columns={"Net Generation (Megawatthours)": "quantity"}, inplace=True
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
    nuc_merged.rename(columns={"unit":"Unit"},inplace=True)
    nuc_merged.rename(
        columns={"compartment": "Compartment", "Plant Id": "plant_id"},
        inplace=True,
    )
    input_dict={"emission":False,"resource":True}
    nuc_merged["directionality"]=nuc_merged["directionality"].map(input_dict)
    nuc_merged.rename(columns={"directionality":"input"},inplace=True)
    return nuc_merged


if __name__ == "__main__":
    from electricitylci.globals import output_dir
    year = 2016
    df = generate_upstream_nuc(year)
    df.to_csv(output_dir + "/nuclear_emissions_{}.csv".format(year))
