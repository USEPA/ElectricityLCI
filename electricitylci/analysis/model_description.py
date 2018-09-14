import pandas as pd

import stewicombo
from electricitylci.globals import inventories_of_interest

#Send one on more process dictionary from a model
def count_processes(process_dict):
    return len(process_dict)

def count_facility_matches()
    inventory_no_overlap = stewicombo.combineInventoriesforFacilitiesinOneInventory("eGRID", inventories_of_interest, filter_for_LCI=True,
                                                             remove_overlap=False)
    #drop duplicate Facility_IDs
    inventory_no_overlap_unique_ids = inventory_no_overlap.drop_duplicates(subset=['FacilityID','Source'])
    facility_counts_by_inventory = inventory_no_overlap_unique_ids.groupby('Source')['FacilityID'].count().reset_index()
    return facility_counts_by_inventory

#Get emissions df's in filtering steps
def count_emissions_wastes_by_step():
    steps = []
    s0 = stewicombo.combineInventoriesforFacilitiesinOneInventory("eGRID", inventories_of_interest,filter_for_LCI=False,remove_overlap=False)
    s1 = stewicombo.combineInventoriesforFacilitiesinOneInventory("eGRID", inventories_of_interest,filter_for_LCI=True,remove_overlap=False)
    from electricitylci.egrid_emissions_and_waste_by_facility import emissions_and_wastes_by_facility
    s2 = emissions_and_wastes_by_facility
    #get filtered list to filter emissions by step
    from electricitylci.egrid_filter import egrid_facilities_selected_on_generation,egrid_facilities_in_desired_efficiency_range,\
        egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min,emissions_and_waste_for_selected_egrid_facilities

    s3 = s2[s2['eGRID_ID'].isin(egrid_facilities_selected_on_generation)]
    s4 = s3[s3['eGRID_ID'].isin(egrid_facilities_in_desired_efficiency_range)]
    s5 = s4[s4['eGRID_ID'].isin(egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min)]
    s6 = emissions_and_waste_for_selected_egrid_facilities
    steps = [s0,s1,s2,s3,s4,s5,s6]
    all_counts = pd.DataFrame()
    stepcount = 0
    for df in steps:
        counts_by_source =  df.groupby(['Source'])['FlowAmount'].count().reset_index()
        counts_by_source.index = [stepcount] * len(counts_by_source)
        #cast it
        counts_by_source_pivot = counts_by_source.pivot(index=None,columns='Source',values='FlowAmount')
        counts_by_source_pivot['Step'] = stepcount
        all_counts = pd.concat([all_counts,counts_by_source_pivot])
        stepcount = stepcount+1
    return all_counts





