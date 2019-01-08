import os


def set_dir(directory):
    if not os.path.exists(directory): os.makedirs(directory)
    return directory

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'electricitylci/'
# modulepath = os.path.dirname(os.path.realpath("__file__"))
output_dir = os.path.join(modulepath, 'output')
data_dir = os.path.join(modulepath,  'data')


def join_with_underscore(items):
    type_cast_to_str = False
    for x in items:
        if not isinstance(x, str):
            # raise TypeError("join_with_underscore()  inputs must be string")
            type_cast_to_str = True
    if type_cast_to_str:
        items = [str(x) for x in items]

    return "_".join(items)

electricity_flow_name_generation_and_distribution = 'Electricity, AC, 2300-7650 V'  #ref Table 1.1 NERC report
electricity_flow_name_consumption = 'Electricity, AC, 120 V'

# EIA923 download url - this is just the base, need to add
# extension and file name
EIA923_BASE_URL = 'https://www.eia.gov/electricity/data/eia923/'
EIA860_BASE_URL = 'https://www.eia.gov/electricity/data/eia860/'


# 


##############################

# Quick and dirty grouping of fuel codes to named categories
# Need to get these lined up with eGRID?
FUEL_CAT_CODES = {
    'COL': 'Coal',
    'NG': 'Natural Gas',
    'NUC': 'Nuclear',
    'WND': 'Wind',
    'SUN': 'Solar',
    'DFO': 'Petroleum',
    'RFO': 'Petroleum',
    'HYC': 'Hydro',
    'HPS': 'Other',
    'GEO': 'Other',
    'WOO': 'Other', # Other oil
    'WWW': 'Biomass',
    'PC': 'Other',
    'MLG': 'Biomass',
    'WOC': 'Other',
    'OTH': 'Other',
    'ORW': 'Biomass',
    'OOG': 'Other'
}