import os

set_model_name_with_stdin = True

def set_dir(directory):
    if not os.path.exists(directory): os.makedirs(directory)
    return directory

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'electricitylci/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'

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
    configfiles = os.listdir(configdir)
    modelnames_dict = {}
    selection_num = 1
    for f in configfiles:
        f = f.strip('_config.json')
        modelnames_dict[selection_num]=f
        selection_num+=1
    return modelnames_dict

