import numpy as np

from electricitylci.egrid_flowbyfacilty import egrid_flowbyfacility

#Get flow by facility data for egrid
egrid_net_generation = egrid_flowbyfacility[egrid_flowbyfacility['FlowName']=='Electricity']
#Convert flow amount to MWh
egrid_net_generation['Electricity'] = egrid_net_generation['FlowAmount']*0.00027778
#drop unneeded columns
egrid_net_generation.drop(columns=['ReliabilityScore','FlowName','FlowAmount','Compartment','Unit'],inplace=True)
#Now just has 'FacilityID' and 'Electricity' in MWh

#Get length
len(egrid_net_generation)
#2016:7715

#Returns list of egrid ids with positive_generation
def list_egrid_facilities_with_positive_generation():
    egrid_net_generation_above_min = egrid_net_generation[egrid_net_generation['Electricity'] > 0]
    return list(egrid_net_generation_above_min['FacilityID'])

egrid_efficiency = egrid_flowbyfacility[egrid_flowbyfacility['FlowName'].isin(['Electricity','Heat'])]
egrid_efficiency = egrid_efficiency.pivot(index = 'FacilityID',columns = 'FlowName',values = 'FlowAmount').reset_index()
egrid_efficiency.sort_values(by='FacilityID',inplace=True)
egrid_efficiency['Efficiency']= egrid_efficiency['Electricity']*100/egrid_efficiency['Heat']
egrid_efficiency = egrid_efficiency.replace([np.inf, -np.inf], np.nan)
egrid_efficiency.dropna(inplace=True)
egrid_efficiency.head(50)
len(egrid_efficiency)

def list_egrid_facilities_in_efficiency_range(min_efficiency,max_efficiency):
    egrid_efficiency_pass = egrid_efficiency[(egrid_efficiency['Efficiency'] >= min_efficiency) & (egrid_efficiency['Efficiency'] <= max_efficiency)]
    return list(egrid_efficiency_pass['FacilityID'])











# for r in egrid_facilities_fuel_cat_per_gen.iterrows():
#         #print(r['FuelCategory'])
#         plant_fuel_category = r['FuelCategory']
#
#         percent_gen_from_fuel = r[fuel_cat_to_per_gen[plant_fuel_category]]
#         if percent_gen_from_fuel >= min_plant_percent_generation_from_primary_fuel_category:
#             facility_ids_passing.append(r['FacilityID'])






