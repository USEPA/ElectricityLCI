from electricitylci.model_config import model_name
from electricitylci.globals import output_dir
import datetime
import pandas as pd
import logging

namestr = (
    f"{output_dir}/{model_name}_jsonld_"
    f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
)

formatter = logging.Formatter(
    "%(levelname)s:%(filename)s:%(funcName)s:%(message)s"
)
logging.basicConfig(
    format="%(levelname)s:%(filename)s:%(funcName)s:%(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("electricitylci")


def get_generation_process_df(source="egrid", regions="all"):
    """
    Create a dataframe of emissions from power generation by fuel type in each
    region.

    Possible changes:
    - Use the 'source' parameter to switch between eGRID and 923
    - Read the 'source' parameter from a config file and pass through the variable.

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
        egrid_facilities_to_include,
        emissions_and_waste_for_selected_egrid_facilities,
    )
    from electricitylci.eia923_generation import build_generation_data
    from electricitylci.generation import create_generation_process_df
    from electricitylci.model_config import replace_egrid
    from electricitylci.emissions_other_sources import integrate_replace_emissions

    if replace_egrid:
        # This is a dummy function that doesn't exist yet
        updated_emissions = build_new_emissions(year)

        generation_data = build_generation_data()
        emissions_and_waste_for_selected_egrid_facilities = (
            integrate_replace_emissions(
                updated_emissions,
                emissions_and_waste_for_selected_egrid_facilities
            )
        )
    else:
        generation_data = build_generation_data(
            egrid_facilities_to_include=egrid_facilities_to_include
        )

    generation_process_df = create_generation_process_df(
        # electricity_for_selected_egrid_facilities,
        generation_data,
        emissions_and_waste_for_selected_egrid_facilities,
        subregion=regions,
    )
    return generation_process_df


def get_generation_mix_process_df(source="egrid", regions="all"):
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
    from electricitylci.egrid_filter import (
        electricity_for_selected_egrid_facilities,
    )
    from electricitylci.generation_mix import (
        create_generation_mix_process_df_from_model_generation_data,
        create_generation_mix_process_df_from_egrid_ref_data,
    )
    from electricitylci.model_config import gen_mix_from_model_generation_data
    from electricitylci.model_config import replace_egrid
    from electricitylci.eia923_generation import build_generation_data
    from electricitylci.model_config import eia_gen_year

    if replace_egrid:
        # assert regions == 'BA' or regions == 'NERC', 'Regions must be BA or NERC'
        print("Actual generation data is used when replacing eGRID")
        generation_data = build_generation_data(
            generation_years=[eia_gen_year]
        )
        generation_mix_process_df = create_generation_mix_process_df_from_model_generation_data(
            generation_data, regions
        )
    else:
        if gen_mix_from_model_generation_data:
            generation_mix_process_df = create_generation_mix_process_df_from_model_generation_data(
                electricity_for_selected_egrid_facilities, regions
            )
        else:
            generation_mix_process_df = create_generation_mix_process_df_from_egrid_ref_data(
                regions
            )
    return generation_mix_process_df


# def get_consumption_mix_process_df(source="egrid", regions="all"):
#    """
#    Create a dataframe of consumption mixes by fuel type in each subregion.
#
#    This function imports and uses the parameter 'gen_mix_from_model_generation_data'
#    from globals.py. If the value is False it cannot currently handle regions
#    other than 'BA', 'NERC', 'US', or a single eGRID subregion.
#
#    Parameters
#    ----------
#    source : str, optional
#        Not currently used (the default is 'egrid', which [default_description])
#    regions : str, optional
#        Which regions to include (the default is 'all', which includes all eGRID
#        subregions)
#
#    Returns
#    -------
#    DataFrame
#        Sample output:
#        >>> all_gen_mix_db.head()
#            Subregion FuelCategory   Electricity  NERC  Generation_Ratio
#        0        AKGD         COAL  5.582922e+05  ASCC          0.116814
#        22       AKGD          OIL  3.355753e+05  ASCC          0.070214
#        48       AKGD          GAS  3.157474e+06  ASCC          0.660651
#        90       AKGD        HYDRO  5.477350e+05  ASCC          0.114605
#        114      AKGD      BIOMASS  5.616577e+04  ASCC          0.011752
#    """
#
#    import pandas as pd
#    from electricitylci.globals import data_dir, output_dir
#    df_gen_mix = get_generation_mix_process_df(source="egrid", regions = 'NERC')
#
#    regions = 'BA'
#
#    if regions == 'BA':
#        df_trade = pd.read_csv(data_dir + '/BAA_final_trade_2016.csv')
#    elif regions == 'NERC':
#        df_trade = pd.read_csv(data_dir + '/ferc_final_trade_2016.csv')
#
#
#    df_cons_mix = df_gen_mix.merge(df_trade, left_on = 'Subregion', right_on = 'import subregion')
#    df_cons_mix['subregion_fuel_fraction'] = df_cons_mix['Generation_Ratio']*df_cons_mix['fraction']


def write_generation_process_database_to_dict(gen_database, regions="all"):
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


def write_generation_mix_database_to_dict(
    genmix_database, gen_dict, regions="all"
):
    from electricitylci.generation_mix import olcaschema_genmix

    genmix_dict = olcaschema_genmix(
        genmix_database, gen_dict, subregion=regions
    )
    return genmix_dict


def write_surplus_pool_and_consumption_mix_dict():
    """
    [summary]

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

    all_process_dicts = dict()
    for d in process_dicts:
        all_process_dicts = {**all_process_dicts, **d}

    olca_dicts = write(all_process_dicts, namestr)
    return olca_dicts


def get_upstream_process_df():
    """
    Automatically load all of the upstream emissions data from the various
    modules. Will return a dataframe with upstream emissions from
    coal, natural gas, petroleum, and nuclear.
    """
    import electricitylci.coal_upstream as coal
    import electricitylci.natural_gas_upstream as ng
    import electricitylci.petroleum_upstream as petro
    import electricitylci.nuclear_upstream as nuke
    import electricitylci.power_plant_construction as const
    from electricitylci.combinator import concat_map_upstream_databases
    from electricitylci.model_config import eia_gen_year

    print("Generating upstream inventories...")
    coal_df = coal.generate_upstream_coal(eia_gen_year)
    ng_df = ng.generate_upstream_ng(eia_gen_year)
    petro_df = petro.generate_petroleum_upstream(eia_gen_year)
    nuke_df = nuke.generate_upstream_nuc(eia_gen_year)
    const = const.generate_power_plant_construction(eia_gen_year)
    upstream_df = concat_map_upstream_databases(
        coal_df, ng_df, petro_df, nuke_df, const
    )
    return upstream_df


def write_upstream_process_database_to_dict(upstream_df):
    """
    Conver the upstream dataframe generated by get_upstream_process_df to
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
        The generator dataframe, generated by get_alternate_gen_plus_netl or
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


def get_alternate_gen_plus_netl():
    """
    This will combine the netl life cycle data for solar, geothermal, and wind,
    which will include impacts from construction, etc. that would be omitted
    from the regular sources of emissions. It also uses the alternate generation
    module to get power plant emissions. The two different dataframes are 
    combined to provide a single dataframe representing annual emissions or
    life cycle emissions apportioned over the appropriate number of years for
    all reporting power plants.

    Returns
    -------
    dataframe
    """
    from electricitylci.model_config import eia_gen_year
    import electricitylci.alt_generation as alt_gen
    from electricitylci.combinator import (
        concat_map_upstream_databases,
        concat_clean_upstream_and_plant,
    )
    import electricitylci.geothermal as geo
    import electricitylci.solar_upstream as solar
    import electricitylci.wind_upstream as wind
    import electricitylci.plant_water_use as water
    import electricitylci.hydro_upstream as hydro
    import electricitylci.solar_thermal_upstream as solartherm

    print(
        "Generating inventories for geothermal, solar, wind, hydro, and solar thermal..."
    )
    geo_df = geo.generate_upstream_geo(eia_gen_year)
    solar_df = solar.generate_upstream_solar(eia_gen_year)
    wind_df = wind.generate_upstream_wind(eia_gen_year)
    hydro_df = hydro.generate_hydro_emissions()
    solartherm_df = solartherm.generate_upstream_solarthermal(eia_gen_year)
    netl_gen = concat_map_upstream_databases(
        geo_df, solar_df, wind_df, hydro_df, solartherm_df
    )
    netl_gen["DataCollection"] = 5
    netl_gen["GeographicalCorrelation"] = 1
    netl_gen["TechnologicalCorrelation"] = 1
    netl_gen["ReliabilityScore"] = 1
    print("Getting reported emissions for generators...")
    gen_df = alt_gen.create_generation_process_df()
    water_df = water.generate_plant_water_use(eia_gen_year)
    gen_df = pd.concat([gen_df, water_df, hydro_df], ignore_index=True)
    combined_gen = concat_clean_upstream_and_plant(gen_df, netl_gen)
    return combined_gen


def aggregate_gen(gen_df, subregion="BA"):
    """
    Runs the alternate aggregation routine to place all emissions and fuel
    inputs on the basis of a MWh generated at the power plant gate. This is 
    in preparation for generating power plant unit processes for openLCA.
    The alternate aggregation routine attempts to recreate the same math 
    as in electricity.generation but with some optimizations. 
    
    Parameters
    ----------
    gen_df : dataframe
        The generation dataframe as generated by get_alternate_gen_plus_netl
        or get_generation_process_df.

    subregion : str, optional
        The level of subregion that the data will be aggregated to. Choices
        are 'all', 'NERC', 'BA', 'US', by default 'BA', by default "BA"
    """
    import electricitylci.alt_generation as alt_gen

    print(f"Aggregating to subregion - {subregion}")
    aggregate_df = alt_gen.aggregate_data(gen_df, subregion=subregion)
    return aggregate_df


def add_fuels_to_gen(gen_df, fuel_df, canadian_gen, upstream_dict):
    """
    Add the upstream fuels to the generation dataframe as fuel inputs.
    
    Parameters
    ----------
    gen_df : dataframe
        The generation dataframe, as generated by get_alternate_gen_plus_netl
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
    gen_plus_fuel_df, upstream_dict, subregion="BA"
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
    from electricitylci.alt_generation import olcaschema_genprocess

    print("Converting generator dataframe to dictionaries...")
    gen_plus_fuel_dict = olcaschema_genprocess(
        gen_plus_fuel_df, upstream_dict, subregion=subregion
    )
    return gen_plus_fuel_dict


def get_distribution_mix_df(combined_df, subregion="BA"):
    import electricitylci.eia_trans_dist_grid_loss as tnd
    from electricitylci.model_config import eia_gen_year

    td_loss_df = tnd.generate_regional_grid_loss(
        combined_df, eia_gen_year, subregion=subregion
    )
    return td_loss_df


def write_distribution_mix_to_dict(dist_mix_df, gen_mix_dict, subregion="BA"):
    import electricitylci.eia_trans_dist_grid_loss as tnd

    dist_mix_dict = tnd.olca_schema_distribution_mix(
        dist_mix_df, gen_mix_dict, subregion=subregion
    )
    return dist_mix_dict


def get_consumption_mix_df(subregion="BA"):
    import electricitylci.eia_io_trading as trade
    from electricitylci.model_config import eia_gen_year

    io_trade_df = trade.ba_io_trading_model(
        year=eia_gen_year, subregion=subregion
    )
    return io_trade_df


def write_consumption_mix_to_dict(cons_mix_df, dist_mix_dict, subregion="BA"):
    import electricitylci.eia_io_trading as trade

    cons_mix_dict = trade.olca_schema_consumption_mix(
        cons_mix_df, dist_mix_dict, subregion=subregion
    )
    return cons_mix_dict
