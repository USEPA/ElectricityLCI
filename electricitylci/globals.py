#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# globals.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os
import glob
from importlib.metadata import version

from esupy.processed_data_mgmt import Paths


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """Define paths, variables, and functions used across several
modules.

Last updated:
    2025-12-12
"""


##############################################################################
# GLOBALS
##############################################################################
try:
    modulepath = os.path.dirname(
        os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    modulepath = 'electricitylci/'

paths = Paths()
'''Paths : esupy class object to connect user's data directory.'''
paths.local_path = os.path.realpath(str(paths.local_path) + "/electricitylci")

# NOTE: output_dir used in a handful of modules (e.g., combinator)
# HOTFIX PosixPath in os.path.join [TWD; 2023-07-27]
output_dir = os.path.join(str(paths.local_path), 'output')
'''str : The ElectricityLCI local output folder (where models are saved).'''

data_dir = os.path.join(modulepath,  'data')
'''str : The ElectricityLCI Python package's data folder.'''

elci_version = "0.0.0"
'''str : The ElectricityLCI Python package version.'''
try:
    # HOTFIX: remove dependency on setuptools and its deprecated pkg_resources
    elci_version = version("ElectricityLCI")
except:
    elci_version = "2.1.0"

# ref Table 1.1 NERC report
electricity_flow_name_generation_and_distribution = (
    'Electricity, AC, 2300-7650 V')
electricity_flow_name_consumption = 'Electricity, AC, 120 V'

# EIA base URLs - need to add file name
EIA923_BASE_URL = 'https://www.eia.gov/electricity/data/eia923/'
'''str : The base URL for EIA Form 923 workbooks.'''
EIA860_BASE_URL = 'https://www.eia.gov/electricity/data/eia860/'
'''str : The base URL for EIA Form 860 workbooks.'''
NREL_REC_YEAR = 2024
'''int : See https://www.nrel.gov/analysis/renewable-power for pub years.'''
NREL_REC_URL = (
    "https://www.nrel.gov/"
    f"docs/libraries/analysis/nrel-green-power-data-v{NREL_REC_YEAR}.xlsx"
)
'''str : NREL voluntary renewable power procurement data sheet URL.'''

# EPA Clean Air Markets API URL
# https://www.epa.gov/power-sector/cam-api-portal
CAM_API_URL = (
    "https://api.epa.gov/easey"
    "/emissions-mgmt/emissions/apportioned/annual/by-facility"
)
'''str : EPA CEMS annual apportioned emissions by facility API URL'''

# Grouping of Reported fuel codes to EPA categories
FUEL_CAT_CODES = {
    'BIT': 'COAL',
    'SUB': 'COAL',
    'LIG': 'COAL',
    'RC': 'COAL',
    'ANT': 'COAL',
    'SGC': 'COAL',
    'SC': 'COAL',
    'NG': 'GAS',
    'NUC': 'NUCLEAR',
    'WND': 'WIND',
    'SUN': 'SOLAR',
    'DFO': 'OIL',
    'RFO': 'OIL',
    'WAT': 'HYDRO',
    # 'HPS': 'OTHF',
    'GEO': 'GEOTHERMAL',
    'WO': 'OIL',
    'KER': 'OIL',
    'JF': 'OIL',
    'PG': 'OIL',
    'BLQ': 'BIOMASS',
    'WDS': 'BIOMASS',
    'WDL': 'BIOMASS',
    'PC': 'OIL',
    'SGP': 'OIL',
    'MSB': 'BIOMASS',
    'MSN': 'OTHF',
    'LFG': 'BIOMASS',
    'WOC': 'COAL',
    'WH': 'OTHF',
    'MSN': 'OTHF',
    'OTH': 'OTHF',
    'TDF': 'OTHF',
    'PUR': 'OTHF',
    'MWH': 'OTHF',
    'AB': 'BIOMASS',
    'OBL': 'BIOMASS',
    'SLW': 'BIOMASS',
    'OBG': 'BIOMASS',
    'OBS': 'BIOMASS',
    'OG': 'OFSL',
    'BFG': 'OFSL',
    'WC': 'COAL'
}

US_STATES = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MP': 'Northern Mariana Islands',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NA': 'National',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

STATE_ABBREV = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}

API_SLEEP = 0.4
'''float : A courtesy sleep time between API calls.'''

COAL_MODEL_YEARS = [2020, 2023]
'''list : The valid coal model years for mining and transportation LCIs.'''

RENEWABLE_VINTAGES = [2016, 2020]
'''list : The valid years for renewable inventories (i.e., 2016 and 2020).'''

NG_MODEL_YEARS = [2016, 2020]
'''list : The valid years for natural gas model (i.e., 2016 and 2020).'''

GREEN_E = ['HYDRO', 'BIOMASS', 'SOLAR', 'SOLARTHERMAL', 'WIND', 'GEOTHERMAL']
'''list: Green or renewable energy categories for residual mixes.'''

OVERFLOW_E = ['MIXED', 'OTHF']
'''list: Non-green fuels that can lend overflow electricity for res. mixes.'''

REM_WEIGHT_METHODS = ['count',]
'''list : State-level REC sales to Balancing authority weighting methods.'''

NEG_REM_METHODS = ['zero', 'keep']
'''list : Accounting methods for negative renewable generation for REM.'''


##############################################################################
# FUNCTIONS
##############################################################################
def get_config_dir():
    """Convenience function to show where eLCI configuration YAMLs are located.

    Returns
    -------
    str
        Folder path to modelconfig directory.
    """
    return os.path.join(modulepath, 'modelconfig')


def get_datastore_dir():
    """Convenience function to show the path to ElectricityLCI data store."""
    return paths.local_path


def list_model_names_in_config():
    """Read YAML file names in modelconfig directory.

    Returns
    -------
    dict
        Dictionary with numeric keys (e.g., 1, 2, 3) and string values, where
        the values represent the ELCI model names found in the modelconfig
        directory.
    """
    configdir = get_config_dir()
    configfiles = glob.glob(os.path.join(configdir, '*_config.yml'))
    modelnames_dict = {}
    selection_num = 1
    # HOTFIX: lexicographically sort model names
    for f in sorted(configfiles):
        f = os.path.basename(f)
        f = f.replace('_config.yml', '')
        modelnames_dict[selection_num] = f
        selection_num += 1
    return modelnames_dict
