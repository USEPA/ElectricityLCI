import pickle

def get_generation_process_df(source='egrid', regions='all'):
    from electricitylci.egrid_filter import electricity_for_selected_egrid_facilities,emissions_and_waste_for_selected_egrid_facilities
    from electricitylci.egrid_generation_database_builder import create_generation_process_df
    generation_process_df = create_generation_process_df(electricity_for_selected_egrid_facilities,emissions_and_waste_for_selected_egrid_facilities,subregion=regions)
    return generation_process_df
    
    
    
def get_generation_mix_process_df(source='egrid',regions='all'):
    from electricitylci.egrid_filter import electricity_for_selected_egrid_facilities
    from electricitylci.egrid_generation_database_builder import create_generation_mix_process_df
    generation_mix_process_df = create_generation_mix_process_df(electricity_for_selected_egrid_facilities)
    return generation_mix_process_df

if  __name__ == '__main__':
		#write_generation_templates()
		#from electricitylci.egrid_template_builder import *
		#gen_mix_template_generator(generation_mix_dict)
   #a = get_generation_process_df(source='egrid', regions='all')
   #b = get_generation_mix_process_df(source='egrid', regions='all')
   from electricitylci.egrid_generation_database_builder import olcaschema_genprocess
   from electricitylci.egrid_generation_database_builder import olcaschema_genmix
   from electricitylci.egrid_template_builder import gen_process_template_generator
   from electricitylci.egrid_template_builder import gen_mix_template_generator
   
   generation_process_dict = olcaschema_genprocess(get_generation_process_df(source='egrid', regions='all'))
   generation_mix_dict = olcaschema_genmix(get_generation_mix_process_df(source='egrid', regions='all'))
   
   gen_process_template_generator(generation_process_dict)
   gen_mix_template_generator(generation_mix_dict)
   
		#pickle.dump(a, open( "gen.p", "wb" ) )
		#pickle.dump(b, open( "gen_mix.p", "wb" ) )

