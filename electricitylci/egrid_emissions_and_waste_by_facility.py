#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# egrid_emissions_and_waste_by_facility.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import pandas as pd

from stewicombo import getInventory
from stewicombo import saveInventory
from stewicombo import combineInventoriesforFacilitiesinBaseInventory as cbi


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module populates the data frame variable,
`emissions_and_wastes_by_facility` that contains the combined inventories for
all facilities from all of the specified sources.

This module will either use an existing specified combined inventory file
specified by the 'stewicombo_file' parameter, or will create one from the
'inventories_of_interest' specified in the configuration file by calling the
`stewicombo` package from Standardized Emission and Waste Inventories (StEWI).

If there is no existing parquet file stored in the local data directory,
stewicombo will generate one.

Last edited:
    2025-04-16
"""
__all__ = [
    'get_combined_stewicombo_file',
]


##############################################################################
# FUNCTIONS
##############################################################################
def get_combined_stewicombo_file(model_specs):
    """Loads, or if not available generates the combined inventory file from
    stewicombo.

    Parameters
    ----------
    model_specs : object of class ModelSpecs

    Returns
    -------
    df
        The combined inventories for all facilities from all of the
        specified sources.
    """
    # Initialize the return data frame
    df = None

    if model_specs.stewicombo_file is not None:
        df = getInventory(model_specs.stewicombo_file, download_if_missing=True)
    else:
        df = getInventory(model_specs.model_name)

    # If the stewicombo file can't be found, generate it
    # Define the preferred priority list of facilities to base inventories upon.
    if df is None:
        if "eGRID" in model_specs.inventories_of_interest.keys():
            base_inventory = "eGRID"
        elif "NEI" in model_specs.inventories_of_interest.keys():
            base_inventory = "NEI"
        elif "TRI" in model_specs.inventories_of_interest.keys():
            base_inventory = "TRI"
        elif "RCRAInfo" in model_specs.inventories_of_interest.keys():
            base_inventory = "RCRAInfo"

        # HOTFIX: work-around ParseError [2024-02-12; TWD]
        # NOTE: Hi! If you are here, then you might be experiencing trouble with
        # stewi and stewicombo data files. If this is your only project that
        # relies on stewicombo, then delete the stewi and stewicombo data store
        # folders on your computer (see globals.get_datastore_dir), then run
        # try again. This will pull StEWI's pre-processed data. See GitHub
        # issue: https://github.com/USEPA/standardizedinventories/issues/151
        df = cbi(
            base_inventory = base_inventory,
            inventory_dict = model_specs.inventories_of_interest,
            filter_for_LCI=True,
            remove_overlap=True,
            keep_sec_cntx=False,
            download_if_missing=True,
        )
        # Drop SRS fields and Electricitiy flow
        df = (df
              .drop(columns=['SRS_ID', 'SRS_CAS'])
              .query('FlowName != "Electricity"')
              )
        # Save stewicombo processed file for later access
        saveInventory(
            name = model_specs.model_name,
            combinedinventory_df = df,
            inventory_dict = model_specs.inventories_of_interest
        )
    return df


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    from electricitylci.model_config import build_model_class
    model_config = build_model_class('ELCI_2020')

    emissions_and_wastes_by_facility = get_combined_stewicombo_file(
        model_config)
    len(emissions_and_wastes_by_facility)
    # for 'ELCI_2020': 88005 [250416; TWD]
    # for 'ELCI_1': 106284 (recorded as 88310 [250416;TWD])

    # Get a list of unique years in the emissions data
    years_list = emissions_and_wastes_by_facility['Year'].unique().tolist()
