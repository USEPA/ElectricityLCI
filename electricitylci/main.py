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

from electricitylci import get_consumption_mix_df
from electricitylci import get_distribution_mix_df
from electricitylci import get_generation_mix_process_df
from electricitylci import get_generation_process_df
from electricitylci import get_upstream_process_df
from electricitylci import write_consumption_mix_to_dict
from electricitylci import write_distribution_dict
from electricitylci import write_distribution_mix_to_dict
from electricitylci import write_fuel_mix_database_to_dict
from electricitylci import write_gen_fuel_database_to_dict
from electricitylci import write_generation_mix_database_to_dict
from electricitylci import write_international_mix_database_to_dict
from electricitylci import write_process_dicts_to_jsonld
from electricitylci import write_surplus_pool_and_consumption_mix_dict
from electricitylci import write_upstream_process_database_to_dict
import electricitylci.model_config as config
from electricitylci.utils import fill_default_provider_uuids


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is intended to be the main program run by a user to
automatically generate a JSON-LD file for importing into openLCA. The options
for the analysis are contained in the configuration YAML files. This module
will perform the functions necessary to generate the JSON-LD according to
those options. The selection of configuration file will occur after the start
of this script.

Last updated:
    2023-11-17

Changelog:
    -   Remove 'write_upstream_dicts_to_jsonld' as a separate function; it
        simply is a redirect to 'write_process_dicts_to_jsonld,' which is
        used consistently throughout the rest of :func:`main`.
    -   Add "to_save" boolean to write-to-JSON-LD calls (limits the number
        of writes).

"""
__all__ = [
    "main",
]


##############################################################################
# FUNCTIONS
##############################################################################
def main():
    """Generate an openLCA-schema JSON-LD zip file containing the life cycle
    inventory for US power plants based on the settings in the user-specified
    configuration file. The basic workflow is as follows:

    1.  Generate upstream processes (if requested) and convert them to olca
        dictionary objects.
    2.  Generate facility-level processes, including renewables and water use,
        append with upstream processes, aggregate to the requested region
        (e.g., balancing authority or FERC), and convert them to olca
        dictionary objects .
    3.  Create the three-layered unit processes: generation mix, consumption
        mix (at grid), and consumption mix (at user); the latter is also called
        the distribution mix.
    """
    if config.model_specs is None:
        # Prompt user to select configuration option.
        # These are defined as YAML files in the modelconfig/ folder in the
        # eLCI package; you might have to search site-packages under lib.
        config.model_specs = config.build_model_class()

    # There are essentially two paths - with and without upstream processes.
    if config.model_specs.include_upstream_processes is True:
        # Create dataframe with all generation process data; includes
        # upstream and Canadian data.
        # NOTE: Only nuclear ('NUC') stage codes have electricity data;
        #       all others are nans.
        logging.info("get upstream process")
        upstream_df = get_upstream_process_df(config.model_specs.eia_gen_year)
        logging.info("write upstream process to dict")
        upstream_dict = write_upstream_process_database_to_dict(upstream_df)

        # NOTE: UUID's for upstream processes are created when converting to
        #       JSON-LD. This has to be done here if the information is
        #       going to be included in the final outputs.
        # NOTE: Use the parameter, to_save, in olca_jsonld_writer.write,
        #       to write the output JSON-LD once.
        upstream_dict = write_process_dicts_to_jsonld(False, upstream_dict)

        # NOTE: This method triggers an input request for EPA data API key;
        #       see https://github.com/USEPA/ElectricityLCI/issues/207
        # NOTE: This method runs aggregation and emission uncertainty
        #       calculations.
        # BUG   KeyError: add_fuel_inputs in combinator.py
        #       --> expand_fuel_df["q_reference_name"]
        #       Ran model a second time and it went away...
        logging.info("get aggregated generation process")
        generation_process_df = get_generation_process_df(
            upstream_df=upstream_df,
            upstream_dict=upstream_dict
        )
    else:
        # Create dataframe with only generation process data.
        upstream_dict = {}
        upstream_df = None
        logging.info("get aggregated generation process")
        generation_process_df = get_generation_process_df()

    logging.info("write generation process to dict")
    if config.model_specs.regional_aggregation in ["FERC", "US"]:
        generation_process_dict = write_gen_fuel_database_to_dict(
            generation_process_df, upstream_dict, subregion="BA"
        )
    else:
        generation_process_dict = write_gen_fuel_database_to_dict(
            generation_process_df, upstream_dict
        )

    # These 333 processes are the fuel-technology electricity generation at BA
    # for example, "Electricity - COAL - Tucson Electric Power"
    logging.info("write gen process to JSON-LD")
    generation_process_dict = write_process_dicts_to_jsonld(
        True, generation_process_dict)

    # Force the generation of BA aggregation if doing FERC, US, or BA regions.
    # This is because the consumption mixes are based on imports from
    # balancing authority areas.
    logging.info("get gen mix process")
    if config.model_specs.regional_aggregation in ["FERC","US"]:
        generation_mix_df = get_generation_mix_process_df("BA")
    else:
        generation_mix_df = get_generation_mix_process_df()

    logging.info("write gen mix to dict")
    generation_mix_dict = write_generation_mix_database_to_dict(
        generation_mix_df, generation_process_dict
    )

    logging.info("write gen mix to jsonld")
    generation_mix_dict = write_process_dicts_to_jsonld(
        True, generation_mix_dict)

    # At this point the two methods diverge from underlying functions enough
    # that it's just easier to split here.
    if config.model_specs.EPA_eGRID_trading is False:
        logging.info("using alt gen method for consumption mix")
        regions_to_keep = list(generation_mix_dict.keys())
        cons_mix_df_dict = get_consumption_mix_df(
            regions_to_keep=regions_to_keep
        )

        logging.info("write consumption mix to dict")
        cons_mix_dicts={}
        for subreg in cons_mix_df_dict.keys():
            cons_mix_dicts[subreg] = write_consumption_mix_to_dict(
                cons_mix_df_dict[subreg],
                generation_mix_dict,
                subregion=subreg
            )

        logging.info("write consumption mix to jsonld")
        for subreg in cons_mix_dicts.keys():
            cons_mix_dicts[subreg] = write_process_dicts_to_jsonld(
                True, cons_mix_dicts[subreg]
            )

        logging.info("get distribution mix")
        dist_mix_df_dict = {}
        for subreg in cons_mix_dicts.keys():
            dist_mix_df_dict[subreg] = get_distribution_mix_df(
                generation_process_df,
                subregion=subreg
            )

        logging.info("write dist mix to dict")
        dist_mix_dicts = {}
        for subreg in dist_mix_df_dict.keys():
            dist_mix_dicts[subreg] = write_distribution_mix_to_dict(
                dist_mix_df_dict[subreg],
                cons_mix_dicts[subreg],
                subregion=subreg
            )

        logging.info("write dist mix to jsonld")
        for subreg in dist_mix_dicts.keys():
            dist_mix_dicts[subreg] = write_process_dicts_to_jsonld(
                True, dist_mix_dicts[subreg]
            )
    else:
        # UNTESTED
        logging.info("us average mix to dict")
        usavegfuel_mix_dict = write_fuel_mix_database_to_dict(
            generation_mix_df,
            generation_process_dict
        )
        logging.info("write us average mix to jsonld")
        usavegfuel_mix_dict = write_process_dicts_to_jsonld(
            True, usavegfuel_mix_dict
        )
        logging.info("international average mix to dict")
        international_mix_dict = write_international_mix_database_to_dict(
            generation_mix_df,
            usavegfuel_mix_dict
        )
        international_mix_dict = write_process_dicts_to_jsonld(
            True, international_mix_dict
        )
        # Get surplus and consumption mix dictionary
        sur_con_mix_dict = write_surplus_pool_and_consumption_mix_dict()
        # Get dist dictionary
        dist_dict = write_distribution_dict()
        generation_mix_dict = write_process_dicts_to_jsonld(
            True, generation_mix_dict
        )
        logging.info('write surplus pool consumption mix to jsonld')
        sur_con_mix_dict = write_process_dicts_to_jsonld(
            False, sur_con_mix_dict)
        logging.info('Filling up UUID of surplus pool consumption mix')
        sur_con_mix_dict = fill_default_provider_uuids(
            sur_con_mix_dict,
            sur_con_mix_dict,
            generation_mix_dict,
            international_mix_dict
        )
        sur_con_mix_dict = write_process_dicts_to_jsonld(True, sur_con_mix_dict)
        dist_dict = fill_default_provider_uuids(dist_dict, sur_con_mix_dict)
        dist_dict = write_process_dicts_to_jsonld(True, dist_dict)

    #
    # TODO: add post-processes
    # (e.g., product system creation, remove untracked flows)
    #

    logging.info(
        'JSON-LD zip file has been saved in the "output" folder '
        f'with the full path {config.model_specs.namestr}'
    )


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    # Define a logger
    root_logger = logging.getLogger()
    root_handler = logging.StreamHandler()
    rec_format = (
        "%(asctime)s.%(msecs)03d:%(levelname)s:%(module)s:%(funcName)s:"
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')
    root_handler.setFormatter(formatter)
    root_logger.addHandler(root_handler)
    root_logger.setLevel("INFO")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--model_config", help="specify model configuration", default="")
    args = parser.parse_args()
    if args.model_config != "":
        config.model_specs = config.build_model_class(args.model_config)
    else:
        config.model_specs = None
    main()
