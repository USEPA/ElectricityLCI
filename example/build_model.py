#Must set model_name in globals.py to model of interest
import electricitylci

from electricitylci.globals import output_dir
from electricitylci.model_config import model_name
from electricitylci.utils import fill_default_provider_uuids

#Optionally create a US avg generation database
#US_generation_db = electricitylci.get_generation_process_df(regions='US')
#Write it to a csv
#US_generation_db.to_csv(output_dir+model_name+'_US_gen_db.csv',index=False)

#Now create the database with generation for each egrid subregion
all_generation_db = electricitylci.get_generation_process_df(regions='all')
len(all_generation_db)
#Save to pickle for easier loading if desired
all_generation_db.to_pickle('work/'+model_name+'all_gen.pk')
#import pandas as pd
#all_generation_db = pd.read_pickle('work/'+model_name+'all_gen.pk')

#Write it to csv for further analysis or review
all_generation_db.to_csv(output_dir + model_name+'_all_gen_db.csv',index=False)
#Write it to a dictionary
all_gen_dict = electricitylci.write_generation_process_database_to_dict(all_generation_db)

#Create the generation mix dictionary
all_gen_mix_db = electricitylci.get_generation_mix_process_df(source='egrid',regions='all')
#Save to pickle for easier loading if desired
#all_gen_mix_db.to_pickle('work/'+model_name+'_all_gen_mix_db.pk')
#import pandas as pd
#all_gen_mix_db = pd.read_pickle('work/ELCA_1_all_gen_mix_db.pk')
#Write it to csv for further analysis or review
all_gen_mix_db.to_csv(output_dir+model_name+'_all_gen_mix_db.csv')
#Write it to a dictionary
all_gen_dict=electricitylci.write_process_dicts_to_jsonld(all_gen_dict)
all_gen_mix_dict = electricitylci.write_generation_mix_database_to_dict(all_gen_mix_db,all_gen_dict,regions='all')

#Get surplus and consumption mix dictionary
sur_con_mix_dict = electricitylci.write_surplus_pool_and_consumption_mix_dict()
#Get dist dictionary
dist_dict = electricitylci.write_distribution_dict()

#Write them all to json-ld
all_gen_mix_dict = electricitylci.write_process_dicts_to_jsonld(all_gen_mix_dict)
sur_con_mix_dict = fill_default_provider_uuids(sur_con_mix_dict,all_gen_mix_dict)
sur_con_mix_dict = electricitylci.write_process_dicts_to_jsonld(sur_con_mix_dict)
sur_con_mix_dict = fill_default_provider_uuids(sur_con_mix_dict,sur_con_mix_dict,all_gen_mix_dict)
sur_con_mix_dict = electricitylci.write_process_dicts_to_jsonld(sur_con_mix_dict)
dist_dict = fill_default_provider_uuids(dist_dict,sur_con_mix_dict)
dist_dict = electricitylci.write_process_dicts_to_jsonld(dist_dict)