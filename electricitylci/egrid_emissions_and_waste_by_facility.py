import pandas as pd
import stewicombo
from electricitylci.globals import inventories_of_interest

emissions_and_wastes_by_facility = stewicombo.combineInventoriesforFacilitiesinOneInventory("eGRID",inventories_of_interest)
len(emissions_and_wastes_by_facility)
#with egrid 2016, tri 2016, nei 2016, rcrainfo 2015: 156855

#Get a list of unique years in the emissions data
years_in_emissions_and_wastes_by_facility = list(pd.unique(emissions_and_wastes_by_facility['Year']))
