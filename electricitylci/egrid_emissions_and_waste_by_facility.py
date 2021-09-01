import pandas as pd
import stewicombo
from electricitylci.model_config import model_specs

emissions_and_wastes_by_facility = None
if model_specs.stewicombo_file is not None:
    emissions_and_wastes_by_facility = stewicombo.getInventory(model_specs.stewicombo_file)

if emissions_and_wastes_by_facility is None:
    emissions_and_wastes_by_facility = stewicombo.combineInventoriesforFacilitiesinBaseInventory("eGRID", model_specs.inventories_of_interest, filter_for_LCI=True)
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
