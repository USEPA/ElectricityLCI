# -*- coding: utf-8 -*-
"""
Created on Fri May 31 16:10:00 2019

@author: Matt Jamieson
"""
import pandas as pd
from electricitylci.globals import data_dir, output_dir

from electricitylci.eia923_generation import eia923_download_extract

import copy


def generate_upstream_geo(year):
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

    geothermal_state_dict = {
        "california": "CA",
        "hawaii": "HI",
        "idaho": "ID",
        "new_mexico": "NM",
        "nevada": "NV",
        "oregon": "OR",
        "utah": "UT",
        "us_average": "US",
    }
    geothermal_states = [
        "california",
        "hawaii",
        "idaho",
        "new_mexico",
        "nevada",
        "oregon",
        "utah",
        "us_average",
    ]
    # Get the EIA generation data for the specified year, this dataset includes
    # the fuel consumption for generating electricity for each facility
    # and fuel type. Filter the data to only include NG facilities and on positive
    # fuel consumption. Group that data by Plant Id as it is possible to have
    # multiple rows for the same facility and fuel based on different prime
    # movers (e.g., gas turbine and combined cycle).
    eia_generation_data = eia923_download_extract(year)

    column_filt = (eia_generation_data["Reported Fuel Type Code"] == "GEO") & (
        eia_generation_data["Net Generation (Megawatthours)"] > 0
    )
    geo_generation_data = eia_generation_data[column_filt]

    geo_generation_data = (
        geo_generation_data.groupby(["Plant Id", "State"])
        .agg({"Net Generation (Megawatthours)": "sum"})
        .reset_index()
    )
    geo_generation_data["Plant Id"] = geo_generation_data["Plant Id"].astype(
        int
    )

    # Read the nuclear LCI excel file
    geo_lci = pd.read_csv(data_dir + "/geothermal_lci.csv", index_col=0)
    geo_lci = geo_lci.melt(
        id_vars=["FlowName", "Compartment"],
        value_vars=geothermal_states,
        var_name="stage_code",
        value_name="FlowAmount",
    )
    geo_lci["stage_code"] = geo_lci["stage_code"].map(geothermal_state_dict)

    geo_merged = pd.merge(
        left=geo_generation_data,
        right=geo_lci,
        left_on=["State"],
        right_on=["stage_code"],
        how="left",
    )

    geo_merged.rename(
        columns={"Net Generation (Megawatthours)": "quantity"}, inplace=True
    )

    # Convert the inventory per MWh emissions to an annual emission
    geo_merged["FlowAmount"] = (
        geo_merged["FlowAmount"] * geo_merged["quantity"]
    )
    geo_merged["Electricity"] = geo_merged["quantity"]
    # Filling out some columns to be consistent with other upstream dataframes
    geo_merged["fuel_type"] = "Geothermal"
    geo_merged["stage_code"] = "Power plant"
    #    geo_merged.drop(columns=['unit'])
    geo_merged.rename(
        columns={"compartment": "Compartment", "Plant Id": "plant_id"},
        inplace=True,
    )
    return geo_merged


if __name__ == "__main__":
    year = 2016
    df = generate_upstream_geo(year)
    df.to_csv(output_dir + "/geothermal_emissions_{}.csv".format(year))
