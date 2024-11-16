#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# main.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

from electricitylci import get_generation_mix_process_df
from electricitylci import get_generation_process_df
from electricitylci import get_upstream_process_df
from electricitylci import run_epa_trade
from electricitylci import run_net_trade
from electricitylci import run_post_processes
from electricitylci import write_gen_fuel_database_to_dict
from electricitylci import write_generation_mix_database_to_dict
from electricitylci import write_process_dicts_to_jsonld
from electricitylci import write_upstream_process_database_to_dict
import electricitylci.model_config as config


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is intended to be the main program run by a user to
automatically generate a JSON-LD file for importing into openLCA. The options
for the analysis are contained in the configuration YAML files. This module
performs the functions necessary to generate the JSON-LD according to those
options. The selection of configuration file will occur after the start
of this script or it may be passed following the command-line argument, '-c'.

Last updated:
    2024-10-28

Changelog:
    -   Address logging handler import for Python 3.12 compatibility.
    -   Remove 'write_upstream_dicts_to_jsonld' as a separate function; it
        simply is a redirect to 'write_process_dicts_to_jsonld,' which is
        used consistently throughout the rest of :func:`main`.
    -   Add first-level abstraction by separating out runs for generation and
        distribution; creates parallel structure with new run post-processes.
    -   Move `get_generation_process_df` from if-else for clarity.
    -   Reduce parameters passed between methods (i.e, the data frame).
    -   Test facility-level inventory generation.
    -   Make use of the post-processing configuration parameter.
    -   Make main() runnable (add ``is_set`` param)
"""
__all__ = [
    "main",
    "run_distribution",
    "run_generation",
]


##############################################################################
# FUNCTIONS
##############################################################################
def main(is_set=False):
    """Generate an openLCA-schema JSON-LD zip file containing the life cycle
    inventory for US power plants based on the settings in the user-specified
    configuration file. The basic workflow is as follows:

    1.  Run generation to:

        -   Generate upstream processes (if requested) and convert them to
            openLCA dictionary objects.
        -   Generate facility-level processes, including renewables and water
            use, append with upstream processes, aggregate to the requested
            region (e.g., balancing authority or FERC), and convert them to
            openLCA dictionary objects.

    2.  Run distribution to create the three-layered unit processes:

        -   generation mix (at balancing authority level),
        -   consumption mix (at grid), and
        -   consumption mix (at user); aka the distribution mix.

    3.  Run post-processes to:

        -   Remove zero-valued product flows from process exchanges.
        -   Remove untracked flows (i.e., flows not in a process exchange).
        -   Generate product systems for consumption mix (at user) processes.

    Examples
    --------
    >>> # To show where data files are exported:
    >>> from electricitylci.globals import output_dir
    >>> print(output_dir)
    >>> # To show the name of the JSON-LD export file:
    >>> import electricitylci.model_config as config
    >>> config.model_specs = config.build_model_class()
    >>> print(config.model_specs.namestr)
    """
    if not is_set or config.model_specs is None:
        # Prompt user to select configuration option.
        # These are defined as YAML files in the modelconfig/ folder in the
        # eLCI package; you might have to search site-packages under lib.
        config.model_specs = config.build_model_class()

    gen_dict = run_generation()
    run_distribution(gen_dict)

    if config.model_specs.run_post_processes:
        # Clean JSON-LD and generate product systems.
        run_post_processes()


def run_distribution(generation_process_dict):
    """Run the consumption and distribution data processes.

    This utilizes the generation data for matching regions and linking
    openLCA processes, flows, and other connections (e.g., locations, actors).

    Parameters
    ----------
    generation_process_dict : dict
        The same data found in the data frame, but formatted and updated with
        universally unique identifiers for use in openLCA.

    Returns
    -------
    dict
        The regionalized distribution mix processes in openLCA schema format.
    """
    # Force the generation of BA aggregation if doing FERC, US, or BA regions.
    # This is because the consumption mixes are based on imports from
    # balancing authority areas.
    logging.info("get gen mix process")
    if config.model_specs.regional_aggregation in ["FERC", "US"]:
        generation_mix_df = get_generation_mix_process_df("BA")
    else:
        generation_mix_df = get_generation_mix_process_df()

    # Create the "Electricity; at grid; generation mix" processes
    logging.info("write gen mix to dict")
    generation_mix_dict = write_generation_mix_database_to_dict(
        generation_mix_df, generation_process_dict,
    )

    logging.info("write gen mix to jsonld")
    generation_mix_dict = write_process_dicts_to_jsonld(
        generation_mix_dict)

    # At this point the two methods diverge
    if config.model_specs.EPA_eGRID_trading is False:
        # True for ELCI_1 & ELCI_2 (not ELCI_3)
        dist_dict = run_net_trade(generation_mix_dict)
    else:
        # ELC1_3
        # NOTE: replace eGRID configuration must be true
        # BUG:  keyerror in fill_default_provider_uuids in utils.py
        dist_dict = run_epa_trade(
            generation_mix_df, generation_mix_dict, generation_process_dict)

    return dist_dict


def run_generation():
    """Run the upstream and plant-level generation data processes.

    The facility-level emissions are aggregated to the specified region
    (e.g., balancing authority) as defined in the YAML of the configuration
    model selected (e.g., ELCI_1).

    Returns
    -------
    dict
        Aggregated generation data (including upstream and renewables) by
        region and fuel type formatted for openLCA.
    """
    # There are essentially two paths - with and without upstream processes.
    if config.model_specs.include_upstream_processes is True:
        # Create data frame with all upstream process data---
        # plant construction, natural gas/petroleum extraction & processing,
        # coal mining, and nuclear fuel extraction and processing.
        # NOTE: Only nuclear ('NUC') stage codes have electricity data;
        #       all others are nans.
        logging.info("get upstream process")
        upstream_df = get_upstream_process_df(config.model_specs.eia_gen_year)

        # Convert all upstream stage codes into exchange tables
        # via upstream_dict.py's `olcaschema_genupstream_processes`.
        logging.info("write upstream process to dict")

        # BUG: Issue 267, upstream product flows are converted to elementary flows.
        upstream_dict = write_upstream_process_database_to_dict(upstream_df)

        # Save each upstream stage code as an openLCA process---
        # these will be used as providers for downstream processes.
        # NOTE: This creates the UUID's for upstream processes
        # (and a few other required metadata fields in the dictionary)
        upstream_dict = write_process_dicts_to_jsonld(upstream_dict)
    else:
        # Create data frame with only generation process data.
        upstream_dict = {}
        upstream_df = None

    # NOTE: This method triggers an input request for EPA data API key;
    #       see https://github.com/USEPA/ElectricityLCI/issues/207
    # NOTE: This method runs aggregation and emission uncertainty
    #       calculations.
    # NOTE: Will import generation.py, which triggers a lot data into memory.
    logging.info("get aggregated generation process")
    generation_process_df = get_generation_process_df(
        upstream_df=upstream_df,
        upstream_dict=upstream_dict,
        to_agg=True
    )

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
        generation_process_dict)

    return generation_process_dict


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    import argparse
    from logging.handlers import RotatingFileHandler
    import os
    from electricitylci.globals import output_dir
    from electricitylci.utils import check_output_dir
    from electricitylci import get_facility_level_inventory

    # Define a root logger at lowest logging level.
    # Ref: https://stackoverflow.com/q/25187083
    log = logging.getLogger()
    log.setLevel("DEBUG")

    # Define log format
    rec_format = (
        "%(asctime)s.%(msecs)03d:%(levelname)s:%(module)s:%(funcName)s:"
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')

    # Create stream handler for info messages
    s_handler = logging.StreamHandler()
    s_handler.setLevel("INFO")
    s_handler.setFormatter(formatter)
    log.addHandler(s_handler)

    # Create file handler for debug messages
    log_filename = "elci.log"
    check_output_dir(output_dir)
    log_path = os.path.join(output_dir, log_filename)
    f_handler = RotatingFileHandler(log_path, backupCount=9, encoding='utf-8')
    f_handler.setLevel("DEBUG")
    f_handler.setFormatter(formatter)
    log.addHandler(f_handler)

    # Define argument parser and process specified configuration model
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--model_config", help="specify model configuration", default="")
    args = parser.parse_args()
    if args.model_config != "":
        config.model_specs = config.build_model_class(args.model_config)
    else:
        config.model_specs = None

    # Execute main; make is_set true in this block.
    try:
        main(True)
        #get_facility_level_inventory(True, False)
    except Exception as e:
        log.error("Crashed on main!\n%s" % repr(e))
    else:
        log.info(
            "Finished!\n"
            "You can find your JSON-LD in this folder: %s" % output_dir
        )
    finally:
        log.handlers[1].doRollover()
