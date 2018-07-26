

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

#write_generation_templates()
#from electricitylci.egrid_template_builder import *
#gen_mix_template_generator(generation_mix_dict)

