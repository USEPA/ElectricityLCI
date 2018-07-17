###Creates the data for electricity generation processes by fuel type and eGRID subregion
###Uses global variables set in the globals file that define filters


from electricitylci.globals import include_only_egrid_facilities_with_positive_generation
from electricitylci.globals import egrid_facility_efficiency_filters
from electricitylci.globals import egrid_year
from electricitylci.globals import electricity_lci_target_year
from electricitylci.dqi import lookup_temporal_score
from electricitylci.egrid_facilities import egrid_facilities
from electricitylci.egrid_facilities import list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min
from electricitylci.egrid_energy import list_egrid_facilities_with_positive_generation
from electricitylci.egrid_energy import list_egrid_facilities_in_efficiency_range
from electricitylci.egrid_emissions_and_waste_by_facility import emissions_and_wastes_by_facility
from electricitylci.egrid_emissions_and_waste_by_facility import years_in_emissions_and_wastes_by_facility
from electricitylci.egrid_FRS_matches import list_FRS_ids_filtered_for_NAICS
from electricitylci.elci_database_generator import create_process_dict




#Get lists of egrid facilities
all_egrid_facility_ids = list(egrid_facilities['FacilityID'])
len(all_egrid_facility_ids)
#2016: 9709

##Facility filtering
#Start with all egrid facilities
egrid_facilities_selected_on_generation = all_egrid_facility_ids
#Replace this list with just net positive generators if true
if include_only_egrid_facilities_with_positive_generation:
    egrid_facilities_selected_on_generation = list_egrid_facilities_with_positive_generation()
len(egrid_facilities_selected_on_generation)
#2016: 7538

#Get facilities in efficiency range
egrid_facilities_in_desired_efficiency_range = list_egrid_facilities_in_efficiency_range(egrid_facility_efficiency_filters['lower_efficiency'],
                                          egrid_facility_efficiency_filters['upper_efficiency'])
len(egrid_facilities_in_desired_efficiency_range)
#2016: 7407

#Get facilities with percent generation over threshold from the fuel category they are assigned to
egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min = list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min()
##len(egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min)
#2016: 7095

#Use a python set to find the intersection
egrid_facilities_to_include = list(set(egrid_facilities_selected_on_generation)
                                   & set(egrid_facilities_in_desired_efficiency_range)
                                   & set(egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min))
len(egrid_facilities_to_include)
#2016:7001

###Emissions and wastes filtering
#Start with all emissions and wastes; these are in this file
emissions_and_waste_by_facility_for_selected_egrid_facilities = emissions_and_wastes_by_facility[emissions_and_wastes_by_facility['eGRID_ID'].isin(egrid_facilities_to_include)]
#for egrid 2016,TRI 2016,NEI 2016,RCRAInfo 2015: 118415

generation_process_dict = create_process_dict(emissions_and_waste_by_facility_for_selected_egrid_facilities)


'''


NAICS Filtering part is left for Wes to Finish

#keep only air emission from egrid
egrid_emissions_for_selected_egrid_facilities = egrid_emissions_for_selected_egrid_facilities[egrid_emissions_for_selected_egrid_facilities['Compartment'] == 'air']
#2016: 22842

#Separate out nonegrid emissions and wastes
nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities = emissions_and_waste_by_facility_for_selected_egrid_facilities[emissions_and_waste_by_facility_for_selected_egrid_facilities['Source'] != 'eGRID']

#Get list of facilities meeting NAICS criteria
frs_ids_meeting_NAICS_criteria = list_FRS_ids_filtered_for_NAICS()

#includes only the non_egrid_emissions for facilities not filtered out with NAICS
nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities_NAICS_filtered = nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities[nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities['FRS_ID'].isin(frs_ids_meeting_NAICS_criteria)]




#TJ to take over to further divide by source

#Get a unique set of egrid ids for inventory source to see which are included
#egrid_ids_by_source = {}
#for s in list(pd.unique(nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities["Source"])):
#    egrid_ids_for_source = nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities[nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities["Source"]==s]['eGRID_ID']
#    egrid_ids_by_source[s] = sorted(list(pd.unique(egrid_ids_for_source)))



#Merge them back together, then join with generation data
#Gets generation data from EIA_923 if needed because it doesn't match
#if years_in_emissions_and_wastes_by_facility != egrid_year



##Add data quality scores
#Temporal correlation score
#generation_inventory['Year'] = generation_inventory['Year']
#age_of_data = (electricity_lci_target_year - generation_inventory['Year']).astype("int",errors='ignore')

#generation_inventory['dqi_temporal'] = age_of_data.apply(lookup_temporal_score)

#Geographic correlation score: All facilities within egrid subregion so assign 1
#generation_inventory['dqi_geographic'] = 1

#Data collection score  - Sarah C to provide guidance


#map_inventory_flows_to_FedCommons_elementary_flows
#leave this for Wes


#Write generation inventory into dictionaries


'''