#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dqi.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """The electricityLCI project uses the EPA Data Quality Assessment
for Life Cycle Inventory Data (2016).

This module provides the ranges to apply for different DQI scores (1-5) and a
function to apply those ranges on any given raw score.

Last updated: 2025-03-14
"""
__all__ = [
    'temporal_correlation_lower_bound_to_dqi',
    'data_collection_lower_bound_to_dqi',
    'technological_correlation_lower_bound_to_dqi',
    'lookup_score_with_bound_key',
]


##############################################################################
# GLOBALS
##############################################################################
flow_data_quality_fields = [
    'Reliability_Score',
    'TemporalCorrelation',
    'GeographicalCorrelation',
    'TechnologicalCorrelation',
    'DataCollection'
]

# These bounds are based on US EPA - Flow Pedigree Matrix temporal correlation
# values (http://dx.doi.org/10.1007/s11367-017-1348-1)
temporal_correlation_lower_bound_to_dqi = {
    3: 1,
    6: 2,
    10: 3,
    15: 4,
    None: 5
}

data_collection_lower_bound_to_dqi = {
    .4: 4,
    .6: 3,
    .8: 2,
    1: 1,
    None: 5
}

# This is a variation from USEPA 2016 flow indicators.
# Instead this is intended to represent fraction of generation
# coming from this intended fuel
technological_correlation_lower_bound_to_dqi = {
    .4: 4,
    .6: 3,
    .8: 2,
    1: 1,
    None: 5
}


##############################################################################
# FUNCTIONS
##############################################################################
def lookup_score_with_bound_key(raw_score, bound_to_dqi):
    """Map applicable ranges for scores and assign a DQI of 1-5.

    Parameters
    ----------
    raw_score : int or float
    bound_to_dqi : dict
        A dictionary where keys are the bounds (like a histogram) for DQI
        intervals and the values represent the DQI score for the bound.
        For example, `data_collection_lower_bound_to_dqi` variable.

    Returns
    -------
    int
        Data quality indicator score (1--5).

    Examples
    --------
    >>> lookup_score_with_bound_key(0.79, data_collection_lower_bound_to_dqi)
    2
    >>> lookup_score_with_bound_key(0.80, data_collection_lower_bound_to_dqi)
    2
    >>> lookup_score_with_bound_key(0.81, data_collection_lower_bound_to_dqi)
    1
    """
    breakpoints = list(bound_to_dqi.keys())
    if raw_score <= breakpoints[0]:
        score = bound_to_dqi[breakpoints[0]]
    elif (raw_score > breakpoints[0]) & (raw_score <= breakpoints[1]):
        score = bound_to_dqi[breakpoints[1]]
    elif (raw_score > breakpoints[1]) & (raw_score <= breakpoints[2]):
        score = bound_to_dqi[breakpoints[2]]
    elif (raw_score > breakpoints[2]) & (raw_score <= breakpoints[3]):
        score = bound_to_dqi[breakpoints[3]]
    elif (raw_score<0):
        logging.debug('Error: invalid dqi score')
    else:
        score = bound_to_dqi[None]
    return score
