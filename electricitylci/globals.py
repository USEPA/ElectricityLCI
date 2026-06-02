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
    2026-03-27
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
    elci_version = "3.0.0"

# ref Table 1.1 NERC report
electricity_flow_name_generation_and_distribution = (
    'Electricity, AC, 2300-7650 V')
electricity_flow_name_consumption = 'Electricity, AC, 120 V'

# GitHub repo URL
GH_URL = "https://github.com/NETL-RIC/ElectricityLCI"
'''str : The web address for ElectricityLCI GitHub repository.'''

# EIA base URLs - need to add file name
EIA923_BASE_URL = 'https://www.eia.gov/electricity/data/eia923/'
'''str : The base URL for EIA Form 923 workbooks.'''
EIA860_BASE_URL = 'https://www.eia.gov/electricity/data/eia860/'
'''str : The base URL for EIA Form 860 workbooks.'''
NREL_REC_YEAR = 2024
'''int : See https://www.nlr.gov/analysis/renewable-power for pub years.'''
NREL_REC_URL = (
    "https://www.nlr.gov/"
    f"docs/libraries/analysis/nrel-green-power-data-v{NREL_REC_YEAR}.xlsx"
)
'''str : NLR voluntary renewable power procurement data sheet URL.'''

# EPA Clean Air Markets API URL
# https://www.epa.gov/power-sector/cam-api-portal
CAM_API_URL = (
    "https://api.epa.gov/easey"
    "/emissions-mgmt/emissions/apportioned/annual/by-facility"
)
'''str : EPA CEMS annual apportioned emissions by facility API URL'''

# Coal model constants
COAL_BASIN_CODES = {
    'Central Appalachia': 'CA',
    'Central Interior': 'CI',
    'Gulf Lignite': 'GL',
    'Illinois Basin': 'IB',
    'Lignite': 'L',
    'Northern Appalachia': 'NA',
    'Powder River Basin': 'PRB',
    'Rocky Mountain': 'RM',
    'Southern Appalachia': 'SA',
    'West/Northwest': 'WNW',
    'Import': 'IMP',
}
'''dict : A map between NETL coal basin names and their abbreviations.'''

COAL_TYPE_CODES = {
    'BIT': 'B',
    'LIG': 'L',
    'SUB': 'S',
    'WC': 'W',
    'RC' : 'RC',
}
'''dict : Map between EIA coal fuel source codes and NETL coal codes.'''

COAL_MINE_CODES = {
    'Surface': 'S',
    'Underground': 'U',
    'Facility': 'F',
    'Processing': 'P',
}
'''dict : A map between coal mine type and their abbreviation.'''

# Grouping of Reported fuel codes to EPA categories
FUEL_CAT_CODES = {
    'AB': 'BIOMASS',    # Agricultural byproducts
    'ANT': 'COAL',      # Anthracite coal
    'BFG': 'OFSL',      # Blast furnace gas
    'BIT': 'COAL',      # Bituminous coal
    'BLQ': 'BIOMASS',   # Black liquor
    'DFO': 'OIL',       # Distillate fuel oil (e.g., diesel)
    'GEO': 'GEOTHERMAL', # Geothermal
    # 'H2': 'HYDROGEN'   # Hydrogen
    'JF': 'OIL',        # Jet fuel
    'KER': 'OIL',       # Kerosene
    'LFG': 'BIOMASS',   # Landfill gas
    'LIG': 'COAL',      # Lignite coal
    'MSB': 'BIOMASS',   # Biogenic municipal solid waste
    'MSN': 'OTHF',      # Non-biogenic municipal solid waste
    'MWH': 'OTHF',      # Electricity for energy storage
    'NG': 'GAS',        # Natural gas
    'NUC': 'NUCLEAR',   # Nuclear (e.g., uranium, plutonium, thorium)
    'OBG': 'BIOMASS',   # Other biomass gas (e.g., digester)
    'OBL': 'BIOMASS',   # Other biomass liquids
    'OBS': 'BIOMASS',   # Other biomass solids
    'OG': 'OFSL',       # Other gas
    'OTH': 'OTHF',      # Other fuel
    'PC': 'OIL',        # Petroleum coke
    'PG': 'OIL',        # Gaseous propane
    'PUR': 'OTHF',      # Purchased steam
    'RC': 'COAL',       # Refined coal
    'RFO': 'OIL',       # Residual fuel oil
    'SC': 'COAL',       # Coal-derived synthesis fuel
    'SGC': 'COAL',      # Coal-derived synthesis gas
    'SGP': 'OIL',       # Synthesis gas from petroleum coke
    'SLW': 'BIOMASS',   # Sludge waste
    'SUB': 'COAL',      # Subbituminous coal
    'SUN': 'SOLAR',     # Solar
    'TDF': 'OTHF',      # Tire-derived fuels
    'WAT': 'HYDRO',     # Water (e.g., hydroelectric/hydrokinetic)
    'WC': 'COAL',       # Waste/other coal
    'WDL': 'BIOMASS',   # Wood waste liquids (excludes black liquor)
    'WDS': 'BIOMASS',   # Wood/wood waste solids
    'WH': 'OTHF',       # Waste heat (unattributed)
    'WND': 'WIND',      # Wind
    'WO': 'OIL',        # Waste/other oil
    'WOC': 'COAL',      # Waste coal
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

REM_WEIGHT_METHODS = ['count', 'gen']
'''list : State-level REC sales to Balancing authority weighting methods.'''

NEG_REM_METHODS = ['zero', 'keep']
'''list : Accounting methods for negative renewable generation for REM.'''

C2G_LCI_METHOD = "Attributional\nCradle-to-Gate process"
'''str : Metadata text for cradle-to-gate inventory method description'''

G2G_LCI_METHOD = "Attributional\nGate-to-Gate process"
'''str : Metadata text for gate-to-gate inventory method description'''


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
