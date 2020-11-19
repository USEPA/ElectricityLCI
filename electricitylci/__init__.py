import pandas as pd
import logging

import electricitylci.model_config as config


formatter = logging.Formatter(
    "%(levelname)s:%(filename)s:%(funcName)s:%(message)s"
)
logging.basicConfig(
    format="%(levelname)s:%(filename)s:%(funcName)s:%(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("electricitylci")


def get_generation_process_df(regions=None, **kwargs):
    """
    Create a dataframe of emissions from power generation by fuel type in each
    region. kwargs would include the upstream emissions dataframe (upstream_df) if
    upstream emissions are being included.

    Parameters
    ----------
    regions : str, optional
        Regions to include in the analysis (the default is None, which uses the value
        read from a settings YAML file). Other options include "eGRID", "NERC", "BA",
        "US", "FERC", and "EIA"

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
    from electricitylci.generation import create_generation_process_df
    from electricitylci.combinator import concat_clean_upstream_and_plant
    if config.model_specs.include_renewable_generation is True:
        generation_process_df=get_gen_plus_netl()
    else:
        generation_process_df = create_generation_process_df()
    if config.model_specs.include_netl_water is True:
        import electricitylci.plant_water_use as water
        water_df = water.generate_plant_water_use(config.model_specs.eia_gen_year)
        generation_process_df=concat_clean_upstream_and_plant(generation_process_df,water_df)
    
    if config.model_specs.include_upstream_processes is True:
        try:
            upstream_df = kwargs['upstream_df']
            upstream_dict = kwargs['upstream_dict']
        except KeyError:
            print(
                "A kwarg named 'upstream_dict' must be included if include_upstream_processes"
                "is True"
            )
#        upstream_dict = write_upstream_process_database_to_dict(
#            upstream_df
#        )
#        upstream_dict = write_upstream_dicts_to_jsonld(upstream_dict)
        combined_df, canadian_gen = combine_upstream_and_gen_df(
                generation_process_df, upstream_df
        )
        gen_plus_fuels = add_fuels_to_gen(
                generation_process_df, upstream_df, canadian_gen, upstream_dict
        )
    else:
        import electricitylci.import_impacts as import_impacts
        canadian_gen_df = import_impacts.generate_canadian_mixes(generation_process_df)
        generation_process_df = pd.concat([generation_process_df, canadian_gen_df], ignore_index=True)
        gen_plus_fuels=generation_process_df
        #This change has been made to accomodate the new method of generating
        #consumption mixes for FERC regions. They now pull BAs to provide
        #a more accurate inventory. The tradeoff here is that it's no longer possible
        #to make a FERC region generation mix and also provide the consumption mix.
        #Or it could be possible but would requir running through aggregate twice.
#        generation_process_df = aggregate_gen(
#            gen_plus_fuels, subregion=regions
#        )
    if regions is None:
        regions = config.model_specs.regional_aggregation
    if regions in ["BA","FERC","US"]:
        generation_process_df = aggregate_gen(
            gen_plus_fuels, subregion="BA"
        )
    else:
        generation_process_df = aggregate_gen(
            gen_plus_fuels, subregion=regions
        )
    return generation_process_df


def get_generation_mix_process_df(regions=None):
    """
    Create a dataframe of generation mixes by fuel type in each subregion.

    This function imports and uses the parameter 'replace_egrid' and 
    'gen_mix_from_model_generation_data' from model_config.py. If 'replace_egrid'
    is true or the specified 'regions' is true, then the generation mix will 
    come from EIA 923 data. If 'replace_egrid' is false then the generation
    mix will either come from the eGRID reference data 
    ('gen_mix_from_model_generation_data' is false) or from the generation data
    from this model ('gen_mix_from_model_generation_data' is true).

    Parameters
    ----------
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
    from electricitylci.egrid_filter import (
        electricity_for_selected_egrid_facilities,
    )
    from electricitylci.generation_mix import (
        create_generation_mix_process_df_from_model_generation_data,
        create_generation_mix_process_df_from_egrid_ref_data,
    )
    from electricitylci.eia923_generation import build_generation_data
    
    if regions is None:
        regions = config.model_specs.regional_aggregation

    if config.model_specs.replace_egrid or regions in ["BA","FERC","US"]:
        # assert regions == 'BA' or regions == 'NERC', 'Regions must be BA or NERC'
        if regions in ["BA","FERC","US"] and not config.model_specs.replace_egrid:
            logger.info(
                f"EIA923 generation data is being used for the generation mix "
                f"despite replace_egrid = False. The reference eGrid electricity "
                f"data cannot be reorgnznied to match BA or FERC regions. For "
                f"the US region, the function for generating US mixes does not "
                f"support aggregating to the US."
                )
        print("EIA923 generation data is used when replacing eGRID")
        generation_data = build_generation_data(
            generation_years=[config.model_specs.eia_gen_year]
        )
        generation_mix_process_df = create_generation_mix_process_df_from_model_generation_data(
            generation_data, regions
        )
    else:
        if config.model_specs.gen_mix_from_model_generation_data:
            generation_mix_process_df = create_generation_mix_process_df_from_model_generation_data(
                electricity_for_selected_egrid_facilities, regions
            )
        else:
            generation_mix_process_df = create_generation_mix_process_df_from_egrid_ref_data(
                regions
            )
    return generation_mix_process_df

def write_generation_process_database_to_dict(gen_database, regions=None):
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

    if regions is None:
        regions = config.model_specs.regional_aggregation

    gen_dict = olcaschema_genprocess(gen_database, subregion=regions)

    return gen_dict


def write_generation_mix_database_to_dict(
    genmix_database, gen_dict, regions=None
):
    from electricitylci.generation_mix import olcaschema_genmix
    if regions is None:
        regions = config.model_specs.regional_aggregation
    if regions in ["FERC","US","BA"]:
        genmix_dict = olcaschema_genmix(
                genmix_database, gen_dict, subregion="BA"
        )
    else:
        genmix_dict = olcaschema_genmix(
            genmix_database, gen_dict, subregion=regions
        )
    return genmix_dict


def write_fuel_mix_database_to_dict(
    genmix_database, gen_dict, regions=None
):
    from electricitylci.generation_mix import olcaschema_usaverage
    if regions is None:
        regions = config.model_specs.regional_aggregation
    if regions in ["FERC","US","BA"]:
        usaverage_dict = olcaschema_usaverage(
                genmix_database, gen_dict, subregion="BA"
        )
    else:
        usaverage_dict = olcaschema_usaverage(
            genmix_database, gen_dict, subregion=regions
        )
    return usaverage_dict


def write_international_mix_database_to_dict(
    genmix_database, usfuelmix_dict, regions=None
):
    from electricitylci.generation_mix import olcaschema_international;
    if regions is None:
        regions = config.model_specs.regional_aggregation
    if regions in ["FERC","US","BA"]:
        international_dict = olcaschema_international(
                genmix_database, usfuelmix_dict, subregion="BA"
        )
    else:
        international_dict = olcaschema_international(
            genmix_database, usfuelmix_dict, subregion=regions
        )
    return international_dict


def write_surplus_pool_and_consumption_mix_dict():
    """
    Create olca formatted dictionaries for the consumption mix as calculated by
    consumption_mix.py. Note that this funcion directly pulls the dataframes,
    converts the data into the dictionary and then returns the dictionary.

    Returns
    -------
    dictionary
        The surplus pool and consumption mixes for the various regions.
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
    
    all_process_dicts = dict()
    for d in process_dicts:
        all_process_dicts = {**all_process_dicts, **d}

    olca_dicts = write(all_process_dicts, config.model_specs.namestr)
    return olca_dicts


def get_upstream_process_df(eia_gen_year):
    """
    Automatically load all of the upstream emissions data from the various
    modules. Will return a dataframe with upstream emissions from
    coal, natural gas, petroleum, nuclear, and plant construction.
    """
    import electricitylci.coal_upstream as coal
    import electricitylci.natural_gas_upstream as ng
    import electricitylci.petroleum_upstream as petro
    import electricitylci.nuclear_upstream as nuke
    import electricitylci.power_plant_construction as const
    from electricitylci.combinator import concat_map_upstream_databases
    
    print("Generating upstream inventories...")
    coal_df = coal.generate_upstream_coal(eia_gen_year)
    ng_df = ng.generate_upstream_ng(eia_gen_year)
    petro_df = petro.generate_petroleum_upstream(eia_gen_year)
    nuke_df = nuke.generate_upstream_nuc(eia_gen_year)
    const = const.generate_power_plant_construction(eia_gen_year)
    #coal and ng already conform to mapping so no mapping needed
    upstream_df = concat_map_upstream_databases(eia_gen_year,
        petro_df, nuke_df, const
    )
    upstream_df=pd.concat([upstream_df,coal_df,ng_df],sort=False,ignore_index=True)
    return upstream_df


def write_upstream_process_database_to_dict(upstream_df):
    """
    Convert the upstream dataframe generated by get_upstream_process_df to
    dictionaries to be written to json-ld.

    Parameters
    ----------
    upstream_df : dataframe
        Combined dataframe as generated by gen_upstream_process_df

    Returns
    -------
    dictionary
    """
    import electricitylci.upstream_dict as upd

    print("Writing upstream processes to dictionaries")
    upstream_dicts = upd.olcaschema_genupstream_processes(upstream_df)
    return upstream_dicts


def write_upstream_dicts_to_jsonld(upstream_dicts):
    """
    Write the upstream dictionary to jsonld.

    Parameters
    ----------
    upstream_dicts : dictionary
        The dictioanary of upstream unit processes generated by
        electricitylci.write_upstream_database_to_dict.
    """
    upstream_dicts = write_process_dicts_to_jsonld(upstream_dicts)
    return upstream_dicts


def combine_upstream_and_gen_df(gen_df, upstream_df):
    """
    Combine the generation and upstream dataframes into a single dataframe.
    The emissions represented here are the annutal emissions for all power
    plants. This dataframe would be suitable for further analysis.

    Parameters
    ----------
    gen_df : dataframe
        The generator dataframe, generated by get_gen_plus_netl or
        get_generation_process_df. Note that get_generation_process_df returns
        two dataframes. The intention would be to send the second returned
        dataframe (plant-level emissions) to this routine.
    upstream_df : dataframe
        The upstream dataframe, generated by get_upstream_process_df
    """

    import electricitylci.combinator as combine
    import electricitylci.import_impacts as import_impacts

    print("Combining upstream and generation inventories")
    combined_df = combine.concat_clean_upstream_and_plant(gen_df, upstream_df)
    canadian_gen = import_impacts.generate_canadian_mixes(combined_df)
    combined_df = pd.concat([combined_df, canadian_gen], ignore_index=True)
    return combined_df, canadian_gen


def get_gen_plus_netl():
    """
    This will combine the netl life cycle data for solar, solar thermal, 
    geothermal, wind, and hydro and will include impacts from construction, etc.
    that would be omitted from the regular sources of emissions. 
    It then generates power plant emissions. The two different dataframes are
    combined to provide a single dataframe representing annual emissions or
    life cycle emissions apportioned over the appropriate number of years for
    all reporting power plants.

    Returns
    -------
    dataframe
    """
    import electricitylci.generation as gen
    from electricitylci.combinator import (
        concat_map_upstream_databases,
        concat_clean_upstream_and_plant,
    )
    import electricitylci.geothermal as geo
    import electricitylci.solar_upstream as solar
    import electricitylci.wind_upstream as wind
    import electricitylci.hydro_upstream as hydro
    import electricitylci.solar_thermal_upstream as solartherm
    
    eia_gen_year = config.model_specs.eia_gen_year
    print(
        "Generating inventories for geothermal, solar, wind, hydro, and solar thermal..."
    )
    geo_df = geo.generate_upstream_geo(eia_gen_year)
    solar_df = solar.generate_upstream_solar(eia_gen_year)
    wind_df = wind.generate_upstream_wind(eia_gen_year)
    hydro_df = hydro.generate_hydro_emissions()
    solartherm_df = solartherm.generate_upstream_solarthermal(eia_gen_year)
    netl_gen = concat_map_upstream_databases(eia_gen_year,
        geo_df, solar_df, wind_df, solartherm_df,
    )
    netl_gen["DataCollection"] = 5
    netl_gen["GeographicalCorrelation"] = 1
    netl_gen["TechnologicalCorrelation"] = 1
    netl_gen["ReliabilityScore"] = 1
    netl_gen=pd.concat([netl_gen,hydro_df[netl_gen.columns]],ignore_index=True,sort=False)
    print("Getting reported emissions for generators...")
    gen_df = gen.create_generation_process_df()
    combined_gen = concat_clean_upstream_and_plant(gen_df, netl_gen)
    return combined_gen


def aggregate_gen(gen_df, subregion="BA"):
    """
    Runs the aggregation routine to place all emissions and fuel
    inputs on the basis of a MWh generated at the power plant gate. This is
    in preparation for generating power plant unit processes for openLCA.

    Parameters
    ----------
    gen_df : dataframe
        The generation dataframe as generated by get_gen_plus_netl
        or get_generation_process_df.

    subregion : str, optional
        The level of subregion that the data will be aggregated to. Choices
        are 'eGRID', 'NERC', 'FERC', 'BA', 'US', by default 'BA', by default "BA"
    """
    import electricitylci.generation as gen
    if subregion is None:
#        subregion = model_specs['regional_aggregation']
        #This change has been made to accomodate the new method of generating
        #consumption mixes for FERC regions. They now pull BAs to provide
        #a more accurate inventory. The tradeoff here is that it's no longer possible
        #to make a FERC region generation mix and also provide the consumption mix.
        #Or it could be possible but would requir running through aggregate twice.
        subregion="BA"
    print(f"Aggregating to subregion - {subregion}")
    aggregate_df = gen.aggregate_data(gen_df, subregion=subregion)
    return aggregate_df


def add_fuels_to_gen(gen_df, fuel_df, canadian_gen, upstream_dict):
    """
    Add the upstream fuels to the generation dataframe as fuel inputs.

    Parameters
    ----------
    gen_df : dataframe
        The generation dataframe, as generated by get_gen_plus_netl
        or get_generation_process_df.
    upstream_df : dataframe
        The combined upstream dataframe.
    upstream_dict : dictionary
        This is the dictionary of upstream "unit processes" as generated by
        electricitylci.upstream_dict after the upstream_dict has been written
        to json-ld. This is important because the uuids for the upstream
        "unit processes" are only generated when written to json-ld.
    """
    from electricitylci.combinator import add_fuel_inputs

    print("Adding fuel inputs to generator emissions...")
    gen_plus_fuel = add_fuel_inputs(gen_df, fuel_df, upstream_dict)
    gen_plus_fuel = pd.concat([gen_plus_fuel, canadian_gen], ignore_index=True)
    return gen_plus_fuel


def write_gen_fuel_database_to_dict(
    gen_plus_fuel_df, upstream_dict, subregion=None
):
    """
    Write the generation dataframe that has been augmented with fuel inputs
    to a dictionary for conversion to openlca.

    Parameters
    ----------
    gen_plus_fuel_df : dataframe
        The dataframe returned by add_fuels_to_gen
    upstream_dict : dictionary
        This is the dictionary of upstream "unit processes" as generated by
        electricitylci.upstream_dict after the upstream_dict has been written
        to json-ld. This is important because the uuids for the upstream
        "unit processes" are only generated when written to json-ld.
    subregion : str, optional
        The level of subregion that the data will be aggregated to. Choices
        are 'all', 'NERC', 'BA', 'US', by default 'BA', by default "BA"

    Returns
    -------
    dictionary
        A dictionary of generation unit processes ready to be written to
        openLCA.
    """
    from electricitylci.generation import olcaschema_genprocess
    if subregion is None:
        subregion = config.model_specs.regional_aggregation
        #Another change to accomodate FERC consumption pulling BAs.
    #Removing the statements below for now. This is preventing the generation
    #of dictionaries for other levels of aggregation. This logic will need to
    #be implemented in main.py so that FERC consumption mixes can be made
    #using the required BA aggregation.
    # if subregion in ["BA","FERC","US"]:    
    #     subregion="BA"
    print("Converting generator dataframe to dictionaries...")
    gen_plus_fuel_dict = olcaschema_genprocess(
        gen_plus_fuel_df, upstream_dict, subregion=subregion
    )
    return gen_plus_fuel_dict


def get_distribution_mix_df(combined_df, subregion=None):
    import electricitylci.eia_trans_dist_grid_loss as tnd
    if subregion is None:
        subregion = config.model_specs.regional_aggregation

    td_loss_df = tnd.generate_regional_grid_loss(
        combined_df, config.model_specs.eia_gen_year, subregion=subregion
    )
    return td_loss_df


def write_distribution_mix_to_dict(dist_mix_df, gen_mix_dict, subregion=None):
    import electricitylci.eia_trans_dist_grid_loss as tnd
    if subregion is None:
        subregion = config.model_specs.regional_aggregation

    dist_mix_dict = tnd.olca_schema_distribution_mix(
        dist_mix_df, gen_mix_dict, subregion=subregion
    )
    return dist_mix_dict


def get_consumption_mix_df(subregion=None, regions_to_keep=None):
    import electricitylci.eia_io_trading as trade
    if subregion is None:
        subregion = config.model_specs.regional_aggregation

    io_trade_df = trade.ba_io_trading_model(
        year=config.model_specs.eia_gen_year, subregion=subregion, regions_to_keep=regions_to_keep
    )
    return io_trade_df


def write_consumption_mix_to_dict(cons_mix_df, dist_mix_dict, subregion=None):
    import electricitylci.eia_io_trading as trade
    if subregion is None:
        subregion = config.model_specs.regional_aggregation

    cons_mix_dict = trade.olca_schema_consumption_mix(
        cons_mix_df, dist_mix_dict, subregion=subregion
    )
    return cons_mix_dict
