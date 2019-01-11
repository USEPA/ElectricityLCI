from electricitylci.model_config import model_name

def get_generation_process_df(source='egrid', regions='all'):
    """Create a dataframe of inputs and outputs to electricity generation processes."""
    from electricitylci.egrid_filter import electricity_for_selected_egrid_facilities,emissions_and_waste_for_selected_egrid_facilities
    from electricitylci.generation import create_generation_process_df
    generation_process_df = create_generation_process_df(electricity_for_selected_egrid_facilities,emissions_and_waste_for_selected_egrid_facilities,subregion=regions)
    return generation_process_df
    
def get_generation_mix_process_df(source='egrid',regions='all'):
    from electricitylci.egrid_filter import electricity_for_selected_egrid_facilities
    from electricitylci.generation_mix import create_generation_mix_process_df_from_model_generation_data,create_generation_mix_process_df_from_egrid_ref_data
    from electricitylci.model_config import gen_mix_from_model_generation_data
    if gen_mix_from_model_generation_data:
        generation_mix_process_df = create_generation_mix_process_df_from_model_generation_data(electricity_for_selected_egrid_facilities, regions)
    else:
        generation_mix_process_df = create_generation_mix_process_df_from_egrid_ref_data(regions)
    return generation_mix_process_df

def write_generation_process_database_to_dict(gen_database,regions='all'):
    from electricitylci.generation import olcaschema_genprocess
    gen_dict = olcaschema_genprocess(gen_database,subregion=regions)
    return gen_dict

def write_generation_mix_database_to_dict(genmix_database,regions='all'):
    from electricitylci.generation_mix import olcaschema_genmix
    genmix_dict = olcaschema_genmix(genmix_database,subregion=regions)
    return genmix_dict

#Currently only valid for all egrid subregions
def write_surplus_pool_and_consumption_mix_dict():
    from electricitylci.consumption_mix import surplus_dict
    from electricitylci.consumption_mix import consumption_dict
    surplus_pool_and_con_mix = {**surplus_dict,**consumption_dict}
    return surplus_pool_and_con_mix

def write_distribution_dict():
    from electricitylci.distribution import distribution_mix_dictionary
    return distribution_mix_dictionary()

#Send one or more process dictionaries to be written to json-ld
def write_process_dicts_to_jsonld(*process_dicts):
    from electricitylci.olca_jsonld_writer import write
    from electricitylci.globals import output_dir
    all_process_dicts = dict()
    for d in process_dicts:
        all_process_dicts={**all_process_dicts,**d}
    write(all_process_dicts,output_dir+model_name+'_jsonld.zip')



