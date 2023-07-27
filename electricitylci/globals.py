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
import pkg_resources  # part of setuptools

from esupy.processed_data_mgmt import Paths


##############################################################################
# GLOBALS
##############################################################################
try:
    modulepath = os.path.dirname(
        os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    modulepath = 'electricitylci/'

# BUG: why are we using this?
paths=Paths()
paths.local_path = os.path.realpath(str(paths.local_path) + "/electricitylci")
# hotfix PosixPath in os.path.join [TWD; 2023-07-27]
output_dir = os.path.join(str(paths.local_path), 'output')
data_dir = os.path.join(modulepath,  'data')

try:
    elci_version = pkg_resources.require("ElectricityLCI")[0].version
except:
    elci_version = "1"

# ref Table 1.1 NERC report
electricity_flow_name_generation_and_distribution = (
    'Electricity, AC, 2300-7650 V')
electricity_flow_name_consumption = 'Electricity, AC, 120 V'

# EIA923 download url - this is just the base, need to add
# extension and file name
EIA923_BASE_URL = 'https://www.eia.gov/electricity/data/eia923/'
EIA860_BASE_URL = 'https://www.eia.gov/electricity/data/eia860/'

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
    "newmexico": "nm",
    "newyork": "ny",
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


##############################################################################
# FUNCTIONS
##############################################################################
def list_model_names_in_config():
    configdir = modulepath + 'modelconfig/'
    configfiles = glob.glob(configdir + '*_config.yml')
    modelnames_dict = {}
    selection_num = 1
    for f in configfiles:
        f = os.path.basename(f)
        f = f.replace('_config.yml','')
        modelnames_dict[selection_num] = f
        selection_num += 1
    return modelnames_dict
