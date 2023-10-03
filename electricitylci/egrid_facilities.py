#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# egrid_facilities.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.model_config import model_specs
import stewi


##############################################################################
# FUNCTIONS
##############################################################################
def add_percent_generation_from_primary_fuel_category_col(x):
    """Add docstring."""
    plant_fuel_category = x['FuelCategory']
    x['PercentGenerationfromDesignatedFuelCategory'] = x[
        fuel_cat_to_per_gen[plant_fuel_category]
    ]
    return x


def list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min():
    """Add docstring."""
    passing_facilties = egrid_facilities_fuel_cat_per_gen[
        egrid_facilities_fuel_cat_per_gen[
            'PercentGenerationfromDesignatedFuelCategory'] > model_specs.min_plant_percent_generation_from_primary_fuel_category
    ]
    # Delete duplicates by creating a set
    facility_ids_passing = list(set(passing_facilties['FacilityID']))
    return facility_ids_passing


##############################################################################
# GLOBALS
##############################################################################
__doc__ = """The main purpose of this module is to modify the eGRID facility list to include the percent of generation from the plant's primary fuel type.

When called, this module simplifies the eGRID data frame to only contain the percent generation from the plant's primary fuel type. It creates an intermediate data frame to select and store the primary percentage while removing the percentages from the imported eGRID data frame. It returns this dataframe after merging the primary percentage to it.

Last edited: 2023-10-03
"""
__all__ = [
    "egrid_subregions",
    "egrid_facilities",
    "list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min",
]

# Get egrid facility file from stewi
egrid_facilities = stewi.getInventoryFacilities("eGRID", model_specs.egrid_year)
'''pandas.DataFrame : eGRID facility-level information.'''

egrid_facilities.rename(columns={
    'Plant primary coal/oil/gas/ other fossil fuel category': 'FuelCategory',
    'Plant primary fuel': 'PrimaryFuel',
    'eGRID subregion acronym': 'Subregion',
    'NERC region acronym': 'NERC'},
    inplace=True
)

# Remove NERC from original egrid output in stewi
# This is because there are mismatches in the original data
# with more than 1 NERC per egrid subregion.
egrid_facilities = egrid_facilities.drop(columns='NERC')

# Bring in eGRID subregion-NERC mapping
_egrid_nerc = pd.read_csv(
    os.path.join(data_dir, 'egrid_subregion_to_NERC.csv'),
    low_memory=False
)
egrid_facilities = pd.merge(
    egrid_facilities, _egrid_nerc, on='Subregion', how='left'
)
# Sanity check: len(egrid_facilities) = 9709 for 2016

egrid_subregions = list(pd.unique(egrid_facilities['Subregion']))
'''list : List of unique subregions for eGRID facilities.'''

# Remove nan if present
egrid_subregions = [x for x in egrid_subregions if str(x) != 'nan']
# Sanity check: len(egrid_subregions) = 26 (2016)

egrid_primary_fuel_categories = sorted(
    pd.unique(egrid_facilities['FuelCategory'].dropna())
)

# Correspondence between fuel category and percent_gen
fuel_cat_to_per_gen = {
    'BIOMASS': 'Plant biomass generation percent (resource mix)',
    'COAL': 'Plant coal generation percent (resource mix)',
    'GAS': 'Plant gas generation percent (resource mix)',
    'GEOTHERMAL': 'Plant geothermal generation percent (resource mix)',
    'HYDRO': 'Plant hydro generation percent (resource mix)',
    'NUCLEAR': 'Plant nuclear generation percent (resource mix)',
    'OFSL': 'Plant other fossil generation percent (resource mix)',
    'OIL': 'Plant oil generation percent (resource mix)',
    'OTHF': 'Plant other unknown / purchased fuel generation percent (resource mix)',
    'SOLAR': 'Plant solar generation percent (resource mix)',
    'WIND': 'Plant wind generation percent (resource mix)'
}

# Get subset of facility file with only these data
cols_to_keep = ['FacilityID', 'FuelCategory']
per_gen_cols = list(fuel_cat_to_per_gen.values())
cols_to_keep = cols_to_keep + per_gen_cols
egrid_facilities_fuel_cat_per_gen = egrid_facilities[cols_to_keep]
egrid_facilities_fuel_cat_per_gen = egrid_facilities_fuel_cat_per_gen[
    egrid_facilities_fuel_cat_per_gen['FuelCategory'].notnull()
]

# Add the percent generation from primary fuel cat to its own column
egrid_facilities_fuel_cat_per_gen[
    'PercentGenerationfromDesignatedFuelCategory'] = 0
egrid_facilities_fuel_cat_per_gen = egrid_facilities_fuel_cat_per_gen.apply(
    add_percent_generation_from_primary_fuel_category_col,
    axis=1
)
egrid_facilities_fuel_cat_per_gen = egrid_facilities_fuel_cat_per_gen.drop(
    columns=per_gen_cols
)
egrid_facilities = egrid_facilities.drop(columns=per_gen_cols)

# Merge back into facilities
egrid_facilities = pd.merge(
    egrid_facilities,
    egrid_facilities_fuel_cat_per_gen,
    on=['FacilityID', 'FuelCategory'],
    how='left'
)

# TODO: are these globals used anywhere?
international = pd.read_csv(data_dir+'/International_Electricity_Mix.csv')
international_reg = list(pd.unique(international['Subregion']))
