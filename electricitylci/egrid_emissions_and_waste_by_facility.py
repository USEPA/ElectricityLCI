#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# egrid_emissions_and_waste_by_facility.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
"""Add docstring."""

import pandas as pd
import stewicombo
from electricitylci.model_config import model_specs


##############################################################################
# FUNCTIONS
##############################################################################
"""Add docstring."""

emissions_and_wastes_by_facility = None
if model_specs.stewicombo_file is not None:
    emissions_and_wastes_by_facility = stewicombo.getInventory(model_specs.stewicombo_file)
    if "eGRID_ID" in emissions_and_wastes_by_facility.columns:
        base_inventory = "eGRID"
    elif "NEI_ID" in model_specs.inventories_of_interest.keys():
        base_inventory = "NEI"
    elif "TRI" in model_specs.inventories_of_interest.keys():
        base_inventory = "TRI"
    elif "RCRAInfo" in model_specs.inventories_of_interest.keys():
        base_inventory = "RCRAInfo"
if emissions_and_wastes_by_facility is None:
    if "eGRID" in model_specs.inventories_of_interest.keys():
        base_inventory = "eGRID"
    elif "NEI" in model_specs.inventories_of_interest.keys():
        base_inventory = "NEI"
    elif "TRI" in model_specs.inventories_of_interest.keys():
        base_inventory = "TRI"
    elif "RCRAInfo" in model_specs.inventories_of_interest.keys():
        base_inventory = "RCRAInfo"
    emissions_and_wastes_by_facility = stewicombo.combineInventoriesforFacilitiesinBaseInventory(base_inventory, model_specs.inventories_of_interest, filter_for_LCI=True)
    # drop SRS fields
    emissions_and_wastes_by_facility = emissions_and_wastes_by_facility.drop(columns=['SRS_ID', 'SRS_CAS'])
    # drop 'Electricity' flow
    emissions_and_wastes_by_facility = emissions_and_wastes_by_facility[emissions_and_wastes_by_facility['FlowName'] != 'Electricity']
    # save stewicombo processed file
    stewicombo.saveInventory(model_specs.model_name, emissions_and_wastes_by_facility, model_specs.inventories_of_interest)

len(emissions_and_wastes_by_facility)
# with egrid 2016, tri 2016, nei 2016, rcrainfo 2015: 106284

# Get a list of unique years in the emissions data
years_in_emissions_and_wastes_by_facility = list(pd.unique(emissions_and_wastes_by_facility['Year']))
