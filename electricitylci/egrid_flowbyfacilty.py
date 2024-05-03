#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# egrid_flowbyfacilty.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
from electricitylci.model_config import model_specs
import stewi


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module calls the `getInventory` function from the
Standardized Emissions and Waste Inventories (stewi) python package, which
returns eGrid data for the year defined in the configuration model
(egrid_year).

This data is sorted by facility ID, and is structured as follows:

.. table: eGRID inventory flows by facility
    :widths: auto

    ==========  =============   ==========  ================  =========== ====
    FacilityID  FlowName        FlowAmount  ReliabilityScore  Compartment Unit
    ==========  =============   ==========  ================  =========== ====
    2	        Nitrous oxide   0.0         2.0               air         kg
    2	        Heat            0.0         5.0               input       MJ
    2	        Electricity     -1170000    1.0               product     MJ
    2	        Carbon dioxide  0.0         5.0               air         kg
    2	        Methane         0.0         2.0               air         kg
    ==========  =============   ==========  ================  =========== ====

Last updated:
    2023-11-20
"""
__all__ = [
    "egrid_flowbyfacility",
]


##############################################################################
# GLOBALS
##############################################################################
egrid_flowbyfacility = stewi.getInventory("eGRID", model_specs.egrid_year)
'''pandas.DataFrame : eGRID flow by inventory from StEWI for eGRID year.'''