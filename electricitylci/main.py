#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# main.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import argparse
import logging

import electricitylci
import electricitylci.model_config as config
from electricitylci.utils import fill_default_provider_uuids


##############################################################################
# FUNCTIONS
##############################################################################
def main():
    """This function will generate an openLCA-schema JSON-LD zip file containing
    life cycle inventory for US power plants based on the settings in the
    user-specified configuration file.
    """
    logger = logging.getLogger("main")
    if config.model_specs is None:
        config.model_specs = config.build_model_class()
    # There are essentially two paths - with and without upstream (i.e., fuel)
    # processes.
    if config.model_specs.include_upstream_processes is True:
        # Create dataframe with all generation process data. This will also
        # include upstream and Canadian data.
        logger.info("get generation process")
        upstream_df = electricitylci.get_upstream_process_df(
            config.model_specs.eia_gen_year)
        logger.info("write generation process to dict")
        # Where we left off. 2023-08-16; TWD
        upstream_dict = electricitylci.write_upstream_process_database_to_dict(
            upstream_df
        )
        # UUID's for upstream processes are created when converting to JSON-LD. This
        # has to be done here if the information is going to be included in final
        # outputs.
        upstream_dict = electricitylci.write_upstream_dicts_to_jsonld(upstream_dict)
        generation_process_df = electricitylci.get_generation_process_df(
            upstream_df=upstream_df, upstream_dict=upstream_dict
        )
    else:
        # Create dataframe with all generation process data. This will also
        # include upstream and Canadian data.
        upstream_dict={}
        upstream_df=None
        generation_process_df = electricitylci.get_generation_process_df(
            upstream_df=upstream_df
        )
    logger.info("write gen process to jsonld")
    if config.model_specs.regional_aggregation in ["FERC","US"]:
        generation_process_dict = electricitylci.write_gen_fuel_database_to_dict(
            generation_process_df, upstream_dict, subregion="BA"
        )
    else:
        generation_process_dict = electricitylci.write_gen_fuel_database_to_dict(
            generation_process_df, upstream_dict
        )
    generation_process_dict = electricitylci.write_process_dicts_to_jsonld(
        generation_process_dict
    )
    # We force the generation of BA aggregation if we're doing FERC, US, or BA
    # regions. This is because the consumption mixes are based on imports from
    # balancing authority areas.
    logger.info("get gen mix process")
    if config.model_specs.regional_aggregation in ["FERC","US"]:
        generation_mix_df = electricitylci.get_generation_mix_process_df("BA")
    else:
        generation_mix_df = electricitylci.get_generation_mix_process_df()
    logger.info("write gen mix to dict")
    generation_mix_dict = electricitylci.write_generation_mix_database_to_dict(
        generation_mix_df, generation_process_dict)
    logger.info("write gen mix to jsonld")
    generation_mix_dict = electricitylci.write_process_dicts_to_jsonld(
        generation_mix_dict
    )

    # At this point the two methods diverge from underlying functions enough that
    # it's just easier to split here.
    if config.model_specs.EPA_eGRID_trading is False:
        logger.info("using alt gen method for consumption mix")
        regions_to_keep=list(generation_mix_dict.keys())
        cons_mix_df_dict = electricitylci.get_consumption_mix_df(regions_to_keep=regions_to_keep)
        logger.info("write consumption mix to dict")
        cons_mix_dicts={}
        for subreg in cons_mix_df_dict.keys():
            # NEED TO FIND A WAY TO SPECIFY REGION HERE
            cons_mix_dicts[subreg] = electricitylci.write_consumption_mix_to_dict(
                cons_mix_df_dict[subreg], generation_mix_dict,subregion=subreg
            )
        logger.info("write consumption mix to jsonld")
        for subreg in cons_mix_dicts.keys():
            cons_mix_dicts[subreg] = electricitylci.write_process_dicts_to_jsonld(
                cons_mix_dicts[subreg])
        logger.info("get distribution mix")
        dist_mix_df_dict={}
        for subreg in cons_mix_dicts.keys():
            dist_mix_df_dict[subreg] = electricitylci.get_distribution_mix_df(
                generation_process_df,subregion=subreg)
        logger.info("write dist mix to dict")
        dist_mix_dicts={}
        for subreg in dist_mix_df_dict.keys():
            dist_mix_dicts[subreg] = electricitylci.write_distribution_mix_to_dict(
                dist_mix_df_dict[subreg], cons_mix_dicts[subreg],subregion=subreg
            )
        logger.info("write dist mix to jsonld")
        for subreg in dist_mix_dicts.keys():
            dist_mix_dicts[subreg] = electricitylci.write_process_dicts_to_jsonld(
                dist_mix_dicts[subreg])
    else:
        logger.info("us average mix to dict")
        usavegfuel_mix_dict = electricitylci.write_fuel_mix_database_to_dict(
        generation_mix_df, generation_process_dict)
        logger.info("write us average mix to jsonld")
        usavegfuel_mix_dict = electricitylci.write_process_dicts_to_jsonld(
            usavegfuel_mix_dict
        )
        logger.info("international average mix to dict")
        international_mix_dict = electricitylci.write_international_mix_database_to_dict(
        generation_mix_df, usavegfuel_mix_dict)
        international_mix_dict = electricitylci.write_process_dicts_to_jsonld(
        international_mix_dict
        )
        # Get surplus and consumption mix dictionary
        sur_con_mix_dict = electricitylci.write_surplus_pool_and_consumption_mix_dict()
        # Get dist dictionary
        dist_dict = electricitylci.write_distribution_dict()
        generation_mix_dict = electricitylci.write_process_dicts_to_jsonld(
            generation_mix_dict
        )
        logger.info('write surplus pool consumption mix to jsonld')
        sur_con_mix_dict = electricitylci.write_process_dicts_to_jsonld(sur_con_mix_dict)
        logger.info('Filling up UUID of surplus pool consumption mix')
        sur_con_mix_dict = fill_default_provider_uuids(
            sur_con_mix_dict, sur_con_mix_dict, generation_mix_dict, international_mix_dict
        )
        sur_con_mix_dict = electricitylci.write_process_dicts_to_jsonld(sur_con_mix_dict)
        dist_dict = fill_default_provider_uuids(dist_dict, sur_con_mix_dict)
        dist_dict = electricitylci.write_process_dicts_to_jsonld(dist_dict)
    logger.info(
        f'JSON-LD files have been saved in the "output" folder with the full path '
        f'{config.model_specs.namestr}'
    )


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    # Define a logger
    root_logger = logging.getLogger()
    root_handler = logging.StreamHandler()
    rec_format = (
        "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:"
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')
    root_handler.setFormatter(formatter)
    root_logger.addHandler(root_handler)
    root_logger.setLevel("INFO")

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--model_config", help="specify model configuration", default="")
    args=parser.parse_args()
    if args.model_config != "":
        config.model_specs=config.build_model_class(args.model_config)
    else:
        config.model_specs=None
    main()
