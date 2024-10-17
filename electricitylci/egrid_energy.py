#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# egrid_energy.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os

import numpy as np
import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.model_config import model_specs
from electricitylci.egrid_flowbyfacilty import egrid_flowbyfacility
from electricitylci.egrid_facilities import make_egrid_subregion_ref


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module performs calculations on eGrid data from
`egrid_flowbyfacility.py` to calculate the efficiency of each facility.

Calculations and data input will execute immediately upon import, which
only happens if the `replace_egrid` configuration setting is set to false.

Last updated:
    2023-11-20
"""
__all__ = [
    "egrid_net_generation",
    "egrid_efficiency",
    "ref_egrid_subregion_generation_by_fuelcategory",
    "list_egrid_facilities_in_efficiency_range",
    "list_egrid_facilities_with_positive_generation",
]


##############################################################################
# GLOBALS
##############################################################################
egrid_net_generation = egrid_flowbyfacility[
    egrid_flowbyfacility['FlowName'] == 'Electricity'].copy()
'''pandas.DataFrame : Flow by facility for eGRID.'''

# Convert flow amount to MWh
egrid_net_generation.loc[
    :, 'Electricity'] = egrid_net_generation['FlowAmount']*0.00027778

# Drop unneeded columns.
# Should now only have 'FacilityID' and 'Electricity' in MWh
#   Sanity check: length (2016) = 7715
egrid_net_generation = egrid_net_generation.drop(
    columns=[
        'DataReliability',
        'FlowName',
        'FlowAmount',
        'Compartment',
        'Unit']
)

egrid_efficiency = egrid_flowbyfacility[
    egrid_flowbyfacility['FlowName'].isin(['Electricity', 'Heat'])].copy()
'''pandas.DataFrame : Efficiency by facility for eGRID.'''

egrid_efficiency = egrid_efficiency.pivot(
    index='FacilityID',
    columns='FlowName',
    values='FlowAmount').reset_index()
egrid_efficiency.sort_values(by='FacilityID', inplace=True)
egrid_efficiency['Efficiency'] = (
    egrid_efficiency['Electricity']*100 / egrid_efficiency['Heat']
)
egrid_efficiency = egrid_efficiency.replace([np.inf, -np.inf], np.nan)
egrid_efficiency.dropna(inplace=True)

# NOTE: this data frame is referenced in generation_mix.py
# TODO: if file doesn't exist, generate it!
#   See https://github.com/USEPA/ElectricityLCI/issues/211
# HOTFIX: add check for missing reference data [2023-11-20; TWD]
make_egrid_subregion_ref(model_specs.egrid_year)
path = os.path.join(
    data_dir,
    'egrid_subregion_generation_by_fuelcategory_reference_{}.csv'.format(
        model_specs.egrid_year)
)
ref_egrid_subregion_generation_by_fuelcategory = pd.read_csv(path)
'''pandas.DataFrame : eGRID generation reference data by subregion.'''

ref_egrid_subregion_generation_by_fuelcategory = ref_egrid_subregion_generation_by_fuelcategory.rename(
    columns={'Electricity': 'Ref_Electricity_Subregion_FuelCategory'}
)


##############################################################################
# FUNCTIONS
##############################################################################
def list_egrid_facilities_with_positive_generation():
    """Return list of facility IDs with a positive electricity product.

    Returns
    -------
    list
        List of facility IDs.
    """
    egrid_net_generation_above_min = egrid_net_generation[
        egrid_net_generation['Electricity'] > 0]
    return list(egrid_net_generation_above_min['FacilityID'])


def list_egrid_facilities_in_efficiency_range(min_efficiency, max_efficiency):
    """Return a list of facility IDs with efficiency within given range.

    Parameters
    ----------
    min_efficiency : int or float
        Minimum efficiency value.
    max_efficiency : int or float
        Maximum efficiency value.

    Returns
    -------
    list
        A list of facility IDs within the efficiency range.
    """
    egrid_efficiency_pass = egrid_efficiency[
        (egrid_efficiency['Efficiency'] >= min_efficiency) & (
            egrid_efficiency['Efficiency'] <= max_efficiency)]
    return list(egrid_efficiency_pass['FacilityID'])
