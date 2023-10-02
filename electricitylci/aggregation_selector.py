#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# aggregation_selector.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging


##############################################################################
# GLOBALS
##############################################################################
__doc__ = """Contains a single function to return the correct region for
results aggregation.

Various functions throughout this package accept a subregion or region
parameter; therefore, this module was created to provide consistency and reduce
the repetition. The the following choices are available:

-   'eGRID': eGRID regions
-   'EIA': EIA region
-   'FERC': Federal Energy Regulatory Commission (FERC) regions
-   'BA': Balancing Authority Areas
-   'NERC': National American Electric Reliability (NERC) regions
-   'US': National average

Last edited: 2023-10-02
"""
__all__ = [
    "subregion_col",
]
module_logger = logging.getLogger("aggregation_selector.py")


##############################################################################
# FUNCTIONS
##############################################################################
def subregion_col(subregion="BA"):
    """Corrects regions for results aggregation.

    Parameters
    ----------
    subregion : str, optional
        Subregion abbreviation.
        Valid options include: 'eGRID', 'NERC', 'BA', 'US', 'FERC' or 'EIA'.
        Defaults to "BA".

    Returns
    -------
    list
        A list of length one.
        The list item is the correct region name associated with the given
        region abbreviation; for use with pandas data frames.
    """
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
