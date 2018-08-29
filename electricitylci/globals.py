import os
import pandas as pd
import json

def set_dir(directory):
    if not os.path.exists(directory): os.makedirs(directory)
    return directory

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'electricitylci/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'

#Reading the fuel name file
fuel_name = pd.read_csv(data_dir+'fuelname.csv')

#Set model_name here
model_name = 'ELCI_2'

#pull in model config vars
try:
    with open("electricitylci/modelbuilds/"+model_name+"_config.json") as cfg:
        model_specs = json.load(cfg)
except FileNotFoundError:
    print("Model specs not found. Create a model specs file for the model of interest.")

#Declare target year for LCI (recommend current year by default) and year of eGRID data to use
electricity_lci_target_year = model_specs["electricity_lci_target_year"]
#Year for egrid data
egrid_year = model_specs["egrid_year"]
#emissions data to include in LCI
inventories_of_interest = model_specs["inventories_of_interest"]
inventories = inventories_of_interest.keys()
#Criteria for filtering eGRID data to include
egrid_facility_efficiency_filters = model_specs["egrid_facility_efficiency_filters"]
include_only_egrid_facilities_with_positive_generation = model_specs["include_only_egrid_facilities_with_positive_generation"]
min_plant_percent_generation_from_primary_fuel_category = model_specs["min_plant_percent_generation_from_primary_fuel_category"]
#Global for efficiency
efficiency_of_distribution_grid = model_specs["efficiency_of_distribution_grid"]
#Trading methodology
net_trading = model_specs["net_trading"]
#Flow list
fedelemflowlist_version = model_specs["fedelemflowlist_version"]
#Determine whether or not to post-process the generation data
post_process_generation_emission_factors = model_specs["post_process_generation_emission_factors"]

#
def join_with_underscore(items):
    type_cast_to_str = False
    for x in items:
        if not isinstance(x, str):
            # raise TypeError("join_with_underscore()  inputs must be string")
            type_cast_to_str = True
    if type_cast_to_str:
        items = [str(x) for x in items]

    return "_".join(items)


