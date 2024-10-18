#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# distribution.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
from electricitylci.process_dictionary_writer import (
    exchange,
    ref_exchange_creator,
    exchange_table_creation_input_con_mix,
    process_table_creation_distribution,
    electricity_at_user_flow,
)
from electricitylci.egrid_facilities import egrid_subregions
from electricitylci.model_config import model_specs


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """The distribution mix module.

The :func:`distribution_mix_dictionary` applies a static but configurable
loss factor to the consumption mix for each subregion. The output is an
openLCA-schema dictionary that will produce processes that account for the
electricity lost during transmission and distribution.

Last updated:
    2024-01-09
"""
__all__ = [
    "distribution_mix_dictionary",
]


##############################################################################
# FUNCTIONS
##############################################################################
def distribution_mix_dictionary():
    """Create an openLCA schema dictionary of consumption mix processes for
    each eGRID subregion that accounts for electricity losses during
    transmission and distribution based on the YAML configuration value of
    'efficiency of distribution grid.'

    Returns
    -------
    dict
        An openLCA formatted dictionary of distribution mix processes for each
        eGRID subregion.
    """
    distribution_dict = dict()
    for reg in egrid_subregions:
        exchanges_list = []
        exchange(
            ref_exchange_creator(electricity_at_user_flow),
            exchanges_list
        )
        exchange(
            exchange_table_creation_input_con_mix(
                1/model_specs.efficiency_of_distribution_grid,
                reg,
                ref_to_consumption=True
            ), exchanges_list
        )
        final = process_table_creation_distribution(reg, exchanges_list)
        distribution_dict['Distribution' + reg] = final;

    return distribution_dict
