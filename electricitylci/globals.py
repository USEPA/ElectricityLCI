import os
import glob

set_model_name_with_stdin = True
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

def list_model_names_in_config():
    configdir = modulepath + 'modelconfig/'
    configfiles = glob.glob(configdir + '*_config.yml')
    modelnames_dict = {}
    selection_num = 1
    for f in configfiles:
        f = f.split('/')[-1]
        f = f.strip('_config.yml')
        modelnames_dict[selection_num] = f
        selection_num += 1
    return modelnames_dict


# EIA923 download url - this is just the base, need to add
# extension and file name
EIA923_BASE_URL = 'https://www.eia.gov/electricity/data/eia923/'
EIA860_BASE_URL = 'https://www.eia.gov/electricity/data/eia860/'


#


##############################

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
}
