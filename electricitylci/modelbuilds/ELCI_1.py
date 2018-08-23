
import electricitylci
from electricitylci.globals import output_dir

model_name = 'ELCI_1'

#First create a US avg generation database
US_generation_db = electricitylci.get_generation_process_df(regions='US')
#Write it to a csv
US_generation_db.to_csv(output_dir+model_name+'_US_gen_db.csv',index=False)

#Now create the database with generation for each egrid subregion
all_generation_db = electricitylci.get_generation_process_df(regions='all')
len(generation_db)
#all_generation_db.to_pickle('work/'+model_name+'all_gen.pk')
#all_generation_db.to_csv(output_dir + model_name+'_all_gen_db.csv',index=False)


#Make post-processing gen db changes here

#Write it to a dictionary
#camx_gen_dict = electricitylci.write_generation_process_database_to_dict(camx_gen)
