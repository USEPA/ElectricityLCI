import pandas as pd
import facilitymatcher
from electricitylci.model_config import inventories
from electricitylci.egrid_facilities import egrid_facilities

#get egrid program matches from FRS from facility matcher
egrid_FRS_matches = facilitymatcher.get_matches_for_inventories(["eGRID"])
egrid_FRS_matches.head()

#Get NAICS info for inventories we're potentially interested in
egrid_frs_ids = list(pd.unique(egrid_FRS_matches['FRS_ID']))
egrid_FRS_NAICS = facilitymatcher.get_FRS_NAICSInfo_for_facility_list(egrid_frs_ids,inventories)

get_first_4 = lambda x: x[0:4]
egrid_FRS_NAICS['NAICS_4'] =  egrid_FRS_NAICS['NAICS'].map(get_first_4)

#import egrid_facilities
egrid_facilities_w_ids_subregions_fuels = egrid_facilities[['FacilityID','Subregion','PrimaryFuel','FuelCategory']]
#Merge egrid facilities with facility ids
egrid_facilities_with_FRS = pd.merge(egrid_facilities_w_ids_subregions_fuels,egrid_FRS_matches,on='FacilityID',how='left')
#Drop records with no FRS
egrid_facilities_with_FRS = egrid_facilities_with_FRS[egrid_facilities_with_FRS['FRS_ID'].notnull()]
len(egrid_facilities_with_FRS)
#2016:7042

egrid_facilities_with_FRS_NAICS = pd.merge(egrid_facilities_with_FRS,egrid_FRS_NAICS,on='FRS_ID')

def list_FRS_ids_filtered_for_NAICS():
    egrid_facilities_with_FRS_NAICS_filtered = egrid_facilities_with_FRS_NAICS[((egrid_facilities_with_FRS_NAICS['NAICS_4'] == '5622')
                                                                                & (egrid_facilities_with_FRS_NAICS['FuelCategory'] == 'BIOMASS')
                                                                                & (egrid_facilities_with_FRS_NAICS['PRIMARY_INDICATOR'] == 'PRIMARY'))
                                                                               |
                                                                               ((egrid_facilities_with_FRS_NAICS['NAICS_4'] == '2211')
                                                                                & (egrid_facilities_with_FRS_NAICS['PRIMARY_INDICATOR'] == 'PRIMARY'))]
    frs_ids = list(pd.unique(egrid_facilities_with_FRS_NAICS_filtered['FRS_ID']))
    return frs_ids


#FRS_NAICS_conditions = [{"NAICS_4":"2211","PRIMARY_INDICATOR":"PRIMARY"},{"NAICS_4":"5622","FuelCategory":"BIOMASS","PRIMARY_INDICATOR":"PRIMARY"}]
# def list_FRS_ids_filtered_for_NAICS():
#     #create conditions
#     all_conditions=[]
#     a = 0
#     for i in FRS_NAICS_conditions:
#         and_conditions = ''
#         ind=0
#         for k,v in i.items():
#             condition = '(egrid_facilities_with_FRS_NAICS[\'' + k + '\'] == \'' + v + '\')'
#             if ind==0:
#                 and_conditions = condition
#             else:
#                 and_conditions = and_conditions + ' & ' + condition
#             ind = ind+1
#             #Add parens around it
#             and_conditions = "(" + and_conditions + ")"
#             all_conditions.append(and_conditions)
#     statement = ''
#     for c in all_conditions:
#         if a==0:
#             statement = c + ' or '
#         else:
#             statement = statement + c
