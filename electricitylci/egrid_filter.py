#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# egrid_filter.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import warnings
warnings.filterwarnings("ignore")

import pandas as pd

from electricitylci.model_config import model_specs
from electricitylci.egrid_facilities import (
    egrid_facilities,
    list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min
)
from electricitylci.egrid_energy import (
    list_egrid_facilities_with_positive_generation,
    list_egrid_facilities_in_efficiency_range,
    egrid_net_generation
)
from electricitylci.egrid_emissions_and_waste_by_facility import (
    get_combined_stewicombo_file
)
from electricitylci.egrid_FRS_matches import list_FRS_ids_filtered_for_NAICS


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """Responsible for producing a data frame that contains emissions and
waste flows for eGRID facilities. This data frame is organized by flow type and
facility ID. Emissions data are sourced from eGRID waste/emissions information.

This module compiles two data frames and then concatenates them to form the
data frame described above.

Both are derived from egrid_facilities_selected_on_generation, filtered to only
contain facilities that adhere to the following:

1.  have positive generation
2.  operate within desired efficiency range, and
3.  operate primarily with just one fuel type.

The first set contains the facilities whose waste/emissions flows are pulled
from eGRID. The second set is the remainder of the original data frame, which
is subsequently filtered based on list_FRS_ids_filtered_for_NAICS(), which
returns only the facilities that are in the power generation sector (NAICS
codes: 2211 or 5622).

The outputs are `electricity_for_selected_egrid_facilities` and
`emissions_and_waste_for_selected_egrid_facilities`, the latter of which is a
data frame of size (78885, 10).

Last edited: 2023-10-03
"""
__all__ = [
    "electricity_for_selected_egrid_facilities",
    "emissions_and_waste_for_selected_egrid_facilities",
]


##############################################################################
# GLOBALS
##############################################################################
# Get lists of egrid facilities
all_egrid_facility_ids = list(egrid_facilities['FacilityID'])
# Sanity check: len(all_egrid_facility_ids) for ELCI_1: 9709

# Facility filtering
# Start with facilities with a not null generation value
egrid_facilities_selected_on_generation = list(
    egrid_net_generation['FacilityID']
)
# Replace this list with just net positive generators if true
if model_specs.include_only_egrid_facilities_with_positive_generation:
    egrid_facilities_selected_on_generation = list_egrid_facilities_with_positive_generation()
# Sanity check: len(egrid_facilities_selected_on_generation) for ELCI_1: 7538

# Get facilities in efficiency range
egrid_facilities_in_desired_efficiency_range = all_egrid_facility_ids
if model_specs.filter_on_efficiency:
    egrid_facilities_in_desired_efficiency_range = list_egrid_facilities_in_efficiency_range(
        model_specs.egrid_facility_efficiency_filters['lower_efficiency'],
        model_specs.egrid_facility_efficiency_filters['upper_efficiency']
    )
# Sanity check: len(egrid_facilities_in_desired_efficiency_range), ELCI_1: 7407

# Get facilities with percent generation over threshold
# from the fuel category they are assigned to:
egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min = all_egrid_facility_ids
if (model_specs.filter_on_min_plant_percent_generation_from_primary_fuel
        and not model_specs.keep_mixed_plant_category):
    egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min = list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min()
# Sanity check: len(egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min) for ELCI_1: 7095

# Use a python set to find the intersection
egrid_facilities_to_include = list(
    set(egrid_facilities_selected_on_generation)
    & set(egrid_facilities_in_desired_efficiency_range)
    & set(
        egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min)
)
# Sanity check: len(egrid_facilities_to_include) for ELCI_1:7001

# Get the generation data for these facilities only
# TO FIX: make this a dataframe, not a slice? [2023-12-21; TWD]
electricity_for_selected_egrid_facilities = egrid_net_generation[
     egrid_net_generation['FacilityID'].isin(egrid_facilities_to_include)
 ]

# Emissions and wastes filtering
# Start with all emissions and wastes; these are in this file
emissions_and_wastes_by_facility = get_combined_stewicombo_file(model_specs)
emissions_and_waste_for_selected_egrid_facilities = emissions_and_wastes_by_facility[
     emissions_and_wastes_by_facility['eGRID_ID'].isin(
         egrid_facilities_to_include)
 ]

# NAICS Filtering
# Apply only to the non-egrid data
# Pull egrid data out first
egrid_emissions_for_selected_egrid_facilities = emissions_and_waste_for_selected_egrid_facilities[
    emissions_and_waste_for_selected_egrid_facilities['Source'] == 'eGRID']
# Sanity check: 2016: 22842

# Separate out non-egrid emissions and wastes
nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities = emissions_and_waste_for_selected_egrid_facilities[
    emissions_and_waste_for_selected_egrid_facilities['Source'] != 'eGRID']

# Includes only the non_egrid_emissions for facilities not filtered out
# with NAICS
if model_specs.filter_non_egrid_emission_on_NAICS:
    # Get list of facilities meeting NAICS criteria
    frs_ids_meeting_NAICS_criteria = list_FRS_ids_filtered_for_NAICS()
    nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities = nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities[nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities['FRS_ID'].isin(frs_ids_meeting_NAICS_criteria)]

# Join the datasets back together
emissions_and_waste_for_selected_egrid_facilities = pd.concat(
    [egrid_emissions_for_selected_egrid_facilities,
     nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities]
)
# Sanity check: len(emissions_and_waste_for_selected_egrid_facilities)
# for egrid 2016,TRI 2016,NEI 2016,RCRAInfo 2015: 90792
