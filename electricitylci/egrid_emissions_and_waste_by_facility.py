import pandas as pd
import stewicombo
import os
from electricitylci.globals import data_dir
from electricitylci.model_config import model_specs

# Check to see if the stewicombo output of interest is stored as a csv
stewicombooutputfile = ''
for k, v in model_specs.inventories_of_interest.items():
    stewicombooutputfile = stewicombooutputfile+"{}_{}_".format(k, v)
stewicombooutputfile = stewicombooutputfile + 'fromstewicombo.csv'

if os.path.exists(data_dir+"/"+stewicombooutputfile):
    emissions_and_wastes_by_facility = pd.read_csv(data_dir+"/"+stewicombooutputfile, header=0, dtype={"FacilityID": "str", "Year": "int", "eGRID_ID": "str"})
else:
    emissions_and_wastes_by_facility = stewicombo.combineInventoriesforFacilitiesinOneInventory("eGRID", model_specs.inventories_of_interest, filter_for_LCI=True)
    # drop SRS fields
    emissions_and_wastes_by_facility = emissions_and_wastes_by_facility.drop(columns=['SRS_ID', 'SRS_CAS'])
    # drop 'Electricity' flow
    emissions_and_wastes_by_facility = emissions_and_wastes_by_facility[emissions_and_wastes_by_facility['FlowName'] != 'Electricity']
    # Save it to a csv for the next call
    emissions_and_wastes_by_facility.to_csv(data_dir+"/"+stewicombooutputfile, index=False)

len(emissions_and_wastes_by_facility)
# with egrid 2016, tri 2016, nei 2016, rcrainfo 2015: 106284

# Get a list of unique years in the emissions data
years_in_emissions_and_wastes_by_facility = list(pd.unique(emissions_and_wastes_by_facility['Year']))
