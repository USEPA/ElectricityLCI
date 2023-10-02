#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# aggregation_selector.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
"""Add docstring."""

import logging

module_logger = logging.getLogger("aggregation_selector.py")


##############################################################################
# FUNCTIONS
##############################################################################
def subregion_col(subregion="BA"):
    """Add docstring."""
    available_options = ["eGRID", "NERC", "BA", "US", "FERC", "EIA"]
    if subregion not in available_options:
        module_logger.warning("Invalid subregion specified - US selected")
        region_agg = None
    if subregion == "all":
        region_agg = ["Subregion"]
    elif subregion == "NERC":
        region_agg = ["NERC"]
    elif subregion == "BA":
        region_agg = ["Balancing Authority Name"]
    elif subregion == "US":
        region_agg = None
    elif subregion == "FERC":
        region_agg = ["FERC_Region"]
    elif subregion == "EIA":
        region_agg = ["EIA_Region"]
    elif subregion == "eGRID":
        region_agg = ["Subregion"]
    return region_agg
