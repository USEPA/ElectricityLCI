#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# geothermal.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os

import pandas as pd

from electricitylci.eia923_generation import eia923_download_extract #modelspecs
from electricitylci.globals import data_dir
from electricitylci.globals import output_dir

from electricitylci.solar_upstream import fix_renewable


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses LCA emissions data to calculate the emissions
of geothermal power generation for every plant in EIA-923. The output
data frame organizes the emissions by plant ID, scaled by the electricity
generated at that plant. The data frame is saved as a CSV file, geothermal_emissions_YEAR.csv.

Created:
    2019-05-31
Last updated:
    2025-01-22
"""


##############################################################################
# FUNCTIONS
##############################################################################
def generate_upstream_geo(year):
    """
    Generate the annual emissions from geothermal power plants. These emissions
    are from an NETL-developed model that takes into account the insoluble gases
    present in the geothermal fluid and the type of geothermal power to develop
    emission factors for those gases.

    Notes
    -----
    Depends on the data file, geothermal_lci.csv, which contains LCA impact
    species determined for geothermal generation in the U.S., grouped by state,
    in addition to a U.S. average.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    pandas.DataFrame
        A data frame with the following columns.

        -   plant_id (int)
        -   EIA Sector Number (str)
        -   quantity (float), same as 'Electricity'
        -   FlowName (str)
        -   Compartment (str)
        -   Unit (str)
        -   input (bool)
        -   stage_code (str), 'Power plant'
        -   FlowAmount (float)
        -   Electricity (float), net generation (MWh)
        -   fuel_type (str)
        -   Source (str)
        -   Compartment_path
    """
    # Mapping dictionaries for geothermal LCI
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

    # Read the state-level geothermal LCI file
    # Columns include 'california', 'hawaii', 'idaho', 'new_mexico', 'nevada',
    # 'utah', and 'us_average' (see geothermal_states list); as well as
    # 'Directionality' (e.g., resource or emission), and 'Compartment'
    # (e.g., resource, ground, air, soil).
    geo_lci = pd.read_csv(
        os.path.join(data_dir, "geothermal_lci.csv"),
        index_col=0
    )

    # Turn the state-based columns into stage_code; convert state names to
    # state abbreviations and use these for mapping
    geo_lci = geo_lci.melt(
        id_vars=["FlowName", "Compartment", "unit", "Directionality"],
        value_vars=geothermal_states,
        var_name="stage_code",
        value_name="FlowAmount",
    )
    geo_lci["stage_code"] = geo_lci["stage_code"].map(geothermal_state_dict)

    # Pull generation data and aggregate across prime movers [25.01.22; TWD]
    # NOTE: there are 3--5 duplicated facilities for years 2016, 2020-2022.
    geo_generation_data = get_geo_generation(year)
    geo_generation_data = geo_generation_data.groupby(
        by='Plant Id').agg({
            'State': 'first',
            'EIA Sector Number': 'first',
            'Net Generation (Megawatthours)': 'sum'}).reset_index()
    geo_merged = pd.merge(
        left=geo_generation_data,
        right=geo_lci,
        left_on="State",
        right_on="stage_code",
        how="left",
    )
    geo_merged.rename(
        columns={"Net Generation (Megawatthours)": "quantity"},
        inplace=True
    )

    # The inventory is on the basis of per MWh; therefore, multiply all
    # inventory flows by electricity generation (in MWh) for the target year.
    geo_merged["FlowAmount"] *= geo_merged["quantity"]

    # Clean up unwanted columns
    d_cols = [
        'NAICS Code',
        'Reported Fuel Type Code',
        'YEAR',
        'Total Fuel Consumption MMBtu',
        'State',
        'Plant Name',
    ]
    d_cols = [x for x in d_cols if x in geo_merged.columns]
    if len(d_cols) > 0:
        geo_merged.drop(
            columns=d_cols,
            inplace=True
        )

    # NOTE
    # Compartments are already lowercase (e.g., 'resource', 'water', 'air',
    # 'ground'), so no additional mapping needed [25.01.15; TWD].

    # Fill out columns to be consistent with other upstream data frames
    geo_merged["Electricity"] = geo_merged["quantity"]
    geo_merged["fuel_type"] = "GEOTHERMAL"
    geo_merged["stage_code"] = "Power plant"

    # Map directionality to resources (true) or emissions (false)
    input_dict = {"emission": False, "resource": True}
    geo_merged["Directionality"] = geo_merged["Directionality"].map(input_dict)
    geo_merged.rename(
        columns={
            "Plant Id": "plant_id",
            "unit": "Unit",
            "Directionality": "input",
        },
        inplace=True,
    )

    geo_merged = fix_renewable(geo_merged, "netlgeot")

    return geo_merged


def get_geo_generation(year):
    """Return positive EIA generation data for geothermal power plants.

    Parameters
    ----------
    year : int
        EIA generation year.

    Returns
    -------
    pandas.DataFrame
        EIA generation data for wind power plants.

        Columns of interest include:

        -   'EIA Sector Number': Classification system for electricity
            generation sources (e.g., 1: 'coal', 2: 'natural gas',
            3: 'nuclear', 4: 'oil', 5--10: 'renewable').
        -   'Reported Prime Mover': The source of mechanical energy used to
            generate electricity (e.g., steam turbine powered by coal or
            natural gas or a wind turbine directly powering an electrical
            generator). 'GEO' == geothermal.
    """
    # Get the EIA generation data for the specified year, this dataset includes
    # the fuel consumption for generating electricity for each facility
    # and fuel type. Filter the data to only include NG facilities and on
    # positive fuel consumption.
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data["Plant Id"] = eia_generation_data[
        "Plant Id"].astype(int)

    column_filt = (eia_generation_data["Reported Fuel Type Code"] == "GEO") & (
        eia_generation_data["Net Generation (Megawatthours)"] > 0
    )
    df = eia_generation_data[column_filt]
    return df


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    year = 2016
    df = generate_upstream_geo(year)
    df.to_csv(output_dir + "/geothermal_emissions_{}.csv".format(year))
