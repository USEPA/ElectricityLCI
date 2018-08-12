
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


def get_consumption_surplus_distribution_df():
    from electricitylci.NERC_consumption_surplus_distribution_builder import surplus_dict
    from electricitylci.NERC_consumption_surplus_distribution_builder import consumption_dict
    from electricitylci.NERC_consumption_surplus_distribution_builder import distribution_dict
    from electricitylci.Canada_mexico_process_builder import trade_dict

def write_generation_process_database_to_dict(gen_database,regions='all'):
    from electricitylci.egrid_generation_database_builder import olcaschema_genprocess
    gen_dict = olcaschema_genprocess(gen_database,subregion=regions)
    return gen_dict

def write_generation_process_dict_to_template(gen_dict):
    from electricitylci.egrid_template_builder import gen_process_template_generator
    gen_process_template_generator(gen_dict)
