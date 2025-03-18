#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# natural_gas_upstream.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia923_generation import eia923_download_extract
import electricitylci.PhysicalQuantities as pq
from electricitylci.generation import add_temporal_correlation_score
from electricitylci.model_config import model_specs
##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses LCA emissions data to calculate the upstream
component of natural gas power plant operation (extraction, processing, and
transportation) for every plant in EIA-923.

Created:
    2019-02-18
Last updated:
    2024-01-10
"""
__all__ = [
    "generate_upstream_ng",
]


##############################################################################
# FUNCTIONS
##############################################################################
def generate_upstream_ng(year):
    """
    Generate the annual gas extraction, processing and transportation
    emissions (in kg) for each plant in EIA923.

    Notes
    -----
    Depends on the data file, gas_supply_basin_mapping.csv, which includes the
    identification information for every natural gas plant in the U.S.
    Once imported, this data frame is simplified to contain just the plant
    code and its NG_LCI_Name.

    Also depends on the data file, NG_LCI.csv, which includes the LCA impact
    species determined for every natural gas basin in the U.S.
    Flows are separated by specific upstream process: production, gathering
    & boosting, processing, transmission, storage, and pipeline.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    pandas.DataFrame
    """
    logging.info("Generating natural gas inventory")

    # Get the EIA generation data for the specified year, this dataset includes
    # the fuel consumption for generating electricity for each facility
    # and fuel type. Filter the data to only include NG facilities and on
    # positive fuel consumption. Group that data by Plant Id as it is possible
    # to have multiple rows for the same facility and fuel based on different
    # prime movers (e.g., gas turbine and combined cycle).
    eia_generation_data = eia923_download_extract(year)

    column_filt = ((eia_generation_data['Reported Fuel Type Code'] == 'NG') &
                   (eia_generation_data['Total Fuel Consumption MMBtu'] > 0))
    ng_generation_data = eia_generation_data[column_filt]

    ng_generation_data = ng_generation_data.groupby('Plant Id').agg(
        {'Total Fuel Consumption MMBtu':'sum'}).reset_index()
    ng_generation_data['Plant Id'] = ng_generation_data['Plant Id'].astype(int)

    # Import the mapping file which has the source gas basin for each Plant Id.
    # NOTE:
    #   This is a 2 MB file that provides about 100 kB of info!
    ng_basin_mapping = pd.read_csv(
        os.path.join(data_dir, 'gas_supply_basin_mapping.csv')
    )
    subset_cols = ['Plant Code', 'NG_LCI_Name']
    ng_basin_mapping = ng_basin_mapping[subset_cols]

    # Merge with ng_generation dataframe.
    ng_generation_data_basin = pd.merge(
        left = ng_generation_data,
        right = ng_basin_mapping,
        left_on = 'Plant Id',
        right_on = 'Plant Code'
    )
    ng_generation_data_basin = ng_generation_data_basin.drop(
        columns=['Plant Code']
    )

    # Read the NG LCI excel file
    ng_lci = pd.read_csv(
        os.path.join(data_dir, "NG_LCI.csv"),
        index_col=[0,1,2,3,4,5]
    )
    ng_lci_columns=[
        "Compartment",
        "FlowName",
        "FlowUUID",
        "Unit",
        "FlowType",
        "input",
        "Basin",
        "FlowAmount"
    ]
    ng_lci_stack = pd.DataFrame(ng_lci.stack()).reset_index()
    ng_lci_stack.columns=ng_lci_columns

    # Merge basin data with LCI dataset
    ng_lci_basin = pd.merge(
        ng_lci_stack,
        ng_generation_data_basin,
        left_on = 'Basin',
        right_on = 'NG_LCI_Name',
        how='left'
    )

    # Multiplying with the EIA 923 fuel consumption; conversion factor is
    # for MMBtu to MJ
    btu_to_MJ = pq.convert(10**6,'Btu','MJ')
    ng_lci_basin["FlowAmount"]=(
        ng_lci_basin["FlowAmount"]
        * ng_lci_basin['Total Fuel Consumption MMBtu']
        * btu_to_MJ
    )

    ng_lci_basin = ng_lci_basin.rename(
        columns={'Total Fuel Consumption MMBtu':'quantity'})
    ng_lci_basin["quantity"]=ng_lci_basin["quantity"]*btu_to_MJ

    # Output is kg emission for the specified year by facility Id,
    # not normalized to electricity output

    ng_lci_basin['FuelCategory'] = 'GAS'
    ng_lci_basin.rename(
        columns={
            'Plant Id':'plant_id',
            'NG_LCI_Name':'stage_code',
            'Stage':'stage'},
        inplace=True
    )
    ng_lci_basin["Year"] = year
    ng_lci_basin["Source"] = "netlgaseiafuel"
    ng_lci_basin["ElementaryFlowPrimeContext"] = "emission"
    ng_lci_basin.loc[
        ng_lci_basin["Compartment"].str.contains("resource/"),
        "ElementaryFlowPrimeContext"] = "resource"
    ng_lci_basin.loc[
        ng_lci_basin["Compartment"].str.contains("Technosphere/"),
        "ElementaryFlowPrimeContext"] = "technosphere"
    # Issue #296 - adding DQI information for upstream processes
    ng_lci_basin["Year"] = 2016
    ng_lci_basin["FlowReliability"] = 3
    ng_lci_basin["TemporalCorrelation"] = add_temporal_correlation_score(
        ng_lci_basin["Year"], model_specs.electricity_lci_target_year
    )
    ng_lci_basin["GeographicalCorrelation"] = 1
    ng_lci_basin["TechnologicalCorrelation"] = 1
    ng_lci_basin["DataCollection"] = 1
    return ng_lci_basin


##############################################################################
# MAIN
##############################################################################
if __name__=='__main__':
    from electricitylci.globals import output_dir
    year=2016
    df = generate_upstream_ng(year)
    df.to_csv(output_dir+'/ng_emissions_{}.csv'.format(year))
