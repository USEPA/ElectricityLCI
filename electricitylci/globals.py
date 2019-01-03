import os
import pandas as pd
import json


def set_dir(directory):
    if not os.path.exists(directory): os.makedirs(directory)
    return directory

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'electricitylci/'
# modulepath = os.path.dirname(os.path.realpath("__file__"))
output_dir = os.path.join(modulepath, 'output')
data_dir = os.path.join(modulepath,  'data')


#Set model_name here
model_name = 'ELCI_2'

#pull in model config vars
try:
    fn = os.path.join(modulepath, 'modelbuilds',
                      '{}_config.json'.format(model_name))
    with open(fn) as cfg:
        model_specs = json.load(cfg)
except FileNotFoundError:
    print("Model specs not found. Create a model specs file for the model of interest.")

#Declare target year for LCI (recommend current year by default) and year of eGRID data to use
electricity_lci_target_year = model_specs["electricity_lci_target_year"]
#Year for egrid data
egrid_year = model_specs["egrid_year"]
#use fuel category for fuel

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

#Break down COAL into the primary fuel types
use_primaryfuel_for_coal = model_specs["use_primaryfuel_for_coal"]
#get the fuelname file
fuel_name_file = model_specs["fuel_name_file"]
#Reading the fuel name file
fuel_name = pd.read_csv(os.path.join(data_dir, fuel_name_file))

#Determine whether or not to post-process the generation data
post_process_generation_emission_factors = model_specs["post_process_generation_emission_factors"]

gen_mix_from_model_generation_data=False


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


electricity_flow_name_generation_and_distribution = 'Electricity, AC, 2300-7650 V'  #ref Table 1.1 NERC report
electricity_flow_name_consumption = 'Electricity, AC, 120 V'
