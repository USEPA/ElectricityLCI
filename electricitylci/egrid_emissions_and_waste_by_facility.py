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
from electricitylci.model_config import model_specs


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module populates the data frame variable,
`emissions_and_wastes_by_facility` that contains the combined inventories for
all facilities from all of the specified sources.

This module will either use an existing inventory determined from the
'inventories_of_interest' specified in the configuration file or create one by
calling the `stewicombo` package from Standardized Emission and Waste
Inventories (StEWI).

If there is no existing CSV file, the call to stewicombo to generate the
inventory can take some time. It should be noted that this module is executed
immediately upon import, which may cause some unexpected delays if the CSV file
is not present.

Last edited:
    2023-12-01
"""
__all__ = [
    'base_inventory',
    'emissions_and_wastes_by_facility',
]


##############################################################################
# GLOBALS
##############################################################################
emissions_and_wastes_by_facility = None
'''pandas.DataFrame : Facility-level inventory. Defaults to none.'''

base_inventory = ""
'''str : Inventory of interest name. Defaults to empty string.'''

if model_specs.stewicombo_file is not None:
    emissions_and_wastes_by_facility = getInventory(model_specs.stewicombo_file)
    if "eGRID_ID" in emissions_and_wastes_by_facility.columns:
        base_inventory = "eGRID"
    elif "NEI_ID" in model_specs.inventories_of_interest.keys():
        base_inventory = "NEI"
    elif "TRI" in model_specs.inventories_of_interest.keys():
        base_inventory = "TRI"
    elif "RCRAInfo" in model_specs.inventories_of_interest.keys():
        base_inventory = "RCRAInfo"

# Define the preferred priority list of facilities to base inventories upon.
if emissions_and_wastes_by_facility is None:
    if "eGRID" in model_specs.inventories_of_interest.keys():
        base_inventory = "eGRID"
    elif "NEI" in model_specs.inventories_of_interest.keys():
        base_inventory = "NEI"
    elif "TRI" in model_specs.inventories_of_interest.keys():
        base_inventory = "TRI"
    elif "RCRAInfo" in model_specs.inventories_of_interest.keys():
        base_inventory = "RCRAInfo"

    emissions_and_wastes_by_facility = cbi(
        base_inventory,
        model_specs.inventories_of_interest,
        filter_for_LCI=True
    )
    # Drop SRS fields
    emissions_and_wastes_by_facility = emissions_and_wastes_by_facility.drop(
        columns=['SRS_ID', 'SRS_CAS'])
    # Drop 'Electricity' flow
    emissions_and_wastes_by_facility = emissions_and_wastes_by_facility[
        emissions_and_wastes_by_facility['FlowName'] != 'Electricity']
    # Save stewicombo processed file
    saveInventory(
        model_specs.model_name,
        emissions_and_wastes_by_facility,
        model_specs.inventories_of_interest
    )


##############################################################################
# GLOBALS
##############################################################################
if __name__ == "__main__":
    len(emissions_and_wastes_by_facility)
    # with egrid 2016, tri 2016, nei 2016, rcrainfo 2015: 106284

    # Get a list of unique years in the emissions data
    years_in_emissions_and_wastes_by_facility = list(
        pd.unique(emissions_and_wastes_by_facility['Year'])
    )
