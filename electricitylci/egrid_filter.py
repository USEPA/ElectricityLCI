###Creates the data for electricity generation processes by fuel type and eGRID subregion
###Uses global variables set in the globals file that define fifile:///C:/Users/TGhosh/Dropbox/Electricity_LCI/2. Electric_Refactor after July/ElectricityLCI/electricitylci/%23%23_____code%23for%23developer.pylters
import warnings
import pandas as pd
warnings.filterwarnings("ignore")

from electricitylci.model_config import egrid_facility_efficiency_filters, \
    include_only_egrid_facilities_with_positive_generation
from electricitylci.egrid_facilities import egrid_facilities,list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min
from electricitylci.egrid_energy import list_egrid_facilities_with_positive_generation, list_egrid_facilities_in_efficiency_range,egrid_net_generation
from electricitylci.egrid_emissions_and_waste_by_facility import emissions_and_wastes_by_facility
from electricitylci.egrid_FRS_matches import list_FRS_ids_filtered_for_NAICS

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
len(egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min)
#2016: 7095

#Use a python set to find the intersection
egrid_facilities_to_include = list(set(egrid_facilities_selected_on_generation)
                                   & set(egrid_facilities_in_desired_efficiency_range)
                                   & set(egrid_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min))
len(egrid_facilities_to_include)
#2016:7001

#Get the generation data for these facilities only
electricity_for_selected_egrid_facilities = egrid_net_generation[egrid_net_generation['FacilityID'].isin(egrid_facilities_to_include)]

###Emissions and wastes filtering
#Start with all emissions and wastes; these are in this file
emissions_and_waste_for_selected_egrid_facilities = emissions_and_wastes_by_facility[emissions_and_wastes_by_facility['eGRID_ID'].isin(egrid_facilities_to_include)]

#len(emissions_and_waste_by_facility_for_selected_egrid_facilities.drop_duplicates())

#emissions_and_waste_by_facility_for_selected_egrid_facilities['eGRID_ID'] = emissions_and_waste_by_facility_for_selected_egrid_facilities['eGRID_ID'].apply(pd.to_numeric, errors = 'coerce')

#NAICS Filtering
#Apply only to the non-egrid data
#Pull egrid data out first
egrid_emissions_for_selected_egrid_facilities = emissions_and_waste_for_selected_egrid_facilities[emissions_and_waste_for_selected_egrid_facilities['Source'] == 'eGRID']
#2016: 22842

#Separate out nonegrid emissions and wastes
nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities = emissions_and_waste_for_selected_egrid_facilities[emissions_and_waste_for_selected_egrid_facilities['Source'] != 'eGRID']

#Get list of facilities meeting NAICS criteria
frs_ids_meeting_NAICS_criteria = list_FRS_ids_filtered_for_NAICS()

#includes only the non_egrid_emissions for facilities not filtered out with NAICS
nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities_NAICS_filtered = nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities[nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities['FRS_ID'].isin(frs_ids_meeting_NAICS_criteria)]

#Join the datasets back together
emissions_and_waste_for_selected_egrid_facilities = pd.concat([egrid_emissions_for_selected_egrid_facilities, nonegrid_emissions_and_waste_by_facility_for_selected_egrid_facilities_NAICS_filtered])
len(emissions_and_waste_for_selected_egrid_facilities)
#for egrid 2016,TRI 2016,NEI 2016,RCRAInfo 2015: 90792

#emissions_and_waste_by_facility_for_selected_egrid_facilities.to_excel('Mainfile.xlsx')
#generation_process_dict,generation_mix_dict = create_process_dict(emissions_and_waste_for_selected_egrid_facilities)

#gen_process_template_generator(generation_process_dict)