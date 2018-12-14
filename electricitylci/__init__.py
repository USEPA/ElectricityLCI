
def get_generation_process_df(source='egrid', regions='all'):
    """
    Create a dataframe of emissions from power generation by fuel type in each region.
    
    Parameters
    ----------
    source : str, optional
        Currently unused. (the default is 'egrid', which [default_description])
    regions : str, optional
        Regions to include in the analysis (the default is 'all', which uses all
        eGRID subregions.)
    
    Returns
    -------
    DataFrame
        Each row represents information about a single emission from a fuel category
        in a single region. Columns are:

       'Subregion', 'FuelCategory', 'FlowName', 'FlowUUID', 'Compartment',
       'Year', 'Source', 'Unit', 'ElementaryFlowPrimeContext',
       'TechnologicalCorrelation', 'TemporalCorrelation', 'DataCollection',
       'Emission_factor', 'Reliability_Score', 'GeographicalCorrelation',
       'GeomMean', 'GeomSD', 'Maximum', 'Minimum'
    """
    from electricitylci.egrid_filter import (
        electricity_for_selected_egrid_facilities,
        emissions_and_waste_for_selected_egrid_facilities
    )
    from electricitylci.generation import create_generation_process_df
    generation_process_df = create_generation_process_df(
        electricity_for_selected_egrid_facilities,
        emissions_and_waste_for_selected_egrid_facilities,
        subregion=regions)
    return generation_process_df


def get_generation_mix_process_df(source='egrid',regions='all'):
    """
    Create a dataframe of generation mixes by fuel type in each subregion.

    This function imports and uses the parameter 'gen_mix_from_model_generation_data'
    from globals.py. If the value is False it cannot currently handle regions
    other than 'all', 'NERC', 'US', or a single eGRID subregion.
    
    Parameters
    ----------
    source : str, optional
        Not currently used (the default is 'egrid', which [default_description])
    regions : str, optional
        Which regions to include (the default is 'all', which includes all eGRID
        subregions)
    
    Returns
    -------
    DataFrame
        Sample output:
        >>> all_gen_mix_db.head()
            Subregion FuelCategory   Electricity  NERC  Generation_Ratio
        0        AKGD         COAL  5.582922e+05  ASCC          0.116814
        22       AKGD          OIL  3.355753e+05  ASCC          0.070214
        48       AKGD          GAS  3.157474e+06  ASCC          0.660651
        90       AKGD        HYDRO  5.477350e+05  ASCC          0.114605
        114      AKGD      BIOMASS  5.616577e+04  ASCC          0.011752
    """
    from electricitylci.egrid_filter import electricity_for_selected_egrid_facilities
    from electricitylci.generation_mix import (
        create_generation_mix_process_df_from_model_generation_data,
        create_generation_mix_process_df_from_egrid_ref_data
    )
    from electricitylci.globals import gen_mix_from_model_generation_data

    if gen_mix_from_model_generation_data:
        generation_mix_process_df = \
            create_generation_mix_process_df_from_model_generation_data(
                electricity_for_selected_egrid_facilities,
                regions)
    else:
        generation_mix_process_df = \
            create_generation_mix_process_df_from_egrid_ref_data(regions)
    return generation_mix_process_df


def write_generation_process_database_to_dict(gen_database, regions='all'):
    """
    Create olca formatted dictionaries of individual processes
    
    Parameters
    ----------
    gen_database : DataFrame
        Each row represents information about a single emission from a fuel category
        in a single region.
    regions : str, optional
        Not currently used (the default is 'all', which [default_description])
    
    Returns
    -------
    dict
        A dictionary of dictionaries, each of which contains information about
        emissions from a single fuel type in a single region.
    """
    from electricitylci.generation import olcaschema_genprocess
    gen_dict = olcaschema_genprocess(gen_database, subregion=regions)
    return gen_dict


def write_generation_mix_database_to_dict(genmix_database, regions='all'):
    from electricitylci.generation_mix import olcaschema_genmix
    genmix_dict = olcaschema_genmix(genmix_database, subregion=regions)
    return genmix_dict


def write_surplus_pool_and_consumption_mix_dict():
    """
    Currently only valid for all egrid subregions
    
    Returns
    -------
    [type]
        [description]
    """
    from electricitylci.consumption_mix import surplus_dict
    from electricitylci.consumption_mix import consumption_dict
    surplus_pool_and_con_mix = {**surplus_dict, **consumption_dict}
    return surplus_pool_and_con_mix


def write_distribution_dict():
    from electricitylci.distribution import distribution_mix_dictionary
    return distribution_mix_dictionary()


def write_process_dicts_to_jsonld(*process_dicts):
    """
    Send one or more process dictionaries to be written to json-ld
    
    """
    from electricitylci.olca_jsonld_writer import write
    from electricitylci.globals import output_dir, model_name
    all_process_dicts = dict()
    for d in process_dicts:
        all_process_dicts = {**all_process_dicts, **d}
    write(all_process_dicts, output_dir+model_name+'_jsonld.zip')


def write_generation_process_dict_to_template(gen_dict):
    from electricitylci.fedlcacommons_template_builder import gen_process_template_generator
    gen_process_template_generator(gen_dict)


def write_generation_mix_dict_to_template(genmix_dict):
    from electricitylci.fedlcacommons_template_builder import gen_mix_template_generator
    gen_mix_template_generator(genmix_dict)


