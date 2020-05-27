import json
import pandas as pd
from os.path import join
import yaml

from electricitylci.globals import modulepath, data_dir, set_model_name_with_stdin, list_model_names_in_config

#################
model_name_exists = False
try:
    model_name
    model_name_exists = True
except NameError:
    model_name_exists = False

if set_model_name_with_stdin and not model_name_exists:
    model_menu = list_model_names_in_config()
    print("Select a model number to use:")
    for k in model_menu.keys():
        print("\t"+str(k)+": "+model_menu[k])
    model_num = input()
    try:
        model_name = model_menu[int(model_num)]
        print("Model " + model_name + " selected.")
    except:
        print('You must select the menu number for an existing model')
elif not set_model_name_with_stdin:
    # Set model_name manually here
    model_name = 'ELCI_3'
#################

#pull in model config vars
def load_model_specs(model_name):

    path = join(modulepath, 'modelconfig', '{}_config.yml'.format(model_name))
    with open(path, 'r') as f:
        specs = yaml.safe_load(f)

    return specs


try:
    model_specs = load_model_specs(model_name)
    # with open(modulepath +'modelconfig/' + model_name + "_config.yml") as cfg:
    #     model_specs = yaml.safe_load(cfg)
except FileNotFoundError:
    print("Model specs not found. Create a model specs file for the model of interest.")

electricity_lci_target_year = model_specs["electricity_lci_target_year"]
egrid_year = model_specs["egrid_year"]

# use 923 and cems rather than egrid, but still use the egrid_year
# parameter to determine the data year
replace_egrid = model_specs["replace_egrid"]
# try:
#     region_column_name = model_specs["region_column_name"]
# except:
#     region_column_name = False
regional_aggregation = model_specs["regional_aggregation"]
eia_gen_year = model_specs["eia_gen_year"]
inventories_of_interest = model_specs["inventories_of_interest"]
inventories = inventories_of_interest.keys()
include_only_egrid_facilities_with_positive_generation = model_specs["include_only_egrid_facilities_with_positive_generation"]
filter_on_efficiency = model_specs['filter_on_efficiency']
egrid_facility_efficiency_filters = model_specs["egrid_facility_efficiency_filters"]
filter_on_min_plant_percent_generation_from_primary_fuel = model_specs['filter_on_min_plant_percent_generation_from_primary_fuel']
min_plant_percent_generation_from_primary_fuel_category = model_specs["min_plant_percent_generation_from_primary_fuel_category"]
keep_mixed_plant_category = model_specs['keep_mixed_plant_category']
filter_non_egrid_emission_on_NAICS = model_specs["filter_non_egrid_emission_on_NAICS"]
efficiency_of_distribution_grid = model_specs["efficiency_of_distribution_grid"]
net_trading = model_specs["net_trading"]
fedelemflowlist_version = model_specs["fedelemflowlist_version"]
use_primaryfuel_for_coal = model_specs["use_primaryfuel_for_coal"]
fuel_name_file = model_specs["fuel_name_file"]
fuel_name = pd.read_csv(join(data_dir, fuel_name_file))
post_process_generation_emission_factors = model_specs["post_process_generation_emission_factors"]
gen_mix_from_model_generation_data=False
class ConfigurationError(Exception):
    """Exception raised for errors in the configuration file"""
    def __init__(self,message):
        self.message = message

def check_model_specs():
    #Check for consumption matching region selection
    if model_specs["regional_aggregation"] in ["FERC, BA, US"] and model_specs["EPA_eGRID_trading"]:
        raise ConfigurationError(
            "EPA trading method is not compatible with selected regional "
            f"aggregation - {model_specs['regional_aggregation']}"
        )
        raise

    if model_specs["regional_aggregation"]!="eGRID" and model_specs["EPA_eGRID_trading"]:
        raise ConfigurationError(
            "EPA trading method is not compatible with selected regional "
            f"aggregation - {model_specs['regional_aggregation']}"
        )
    if not model_specs["replace_egrid"] and model_specs["egrid_year"]!=model_specs["eia_gen_year"] and model_specs["include_upstream_processes"]:
        raise ConfigurationError(
                f"When using egrid data and adding upstream processes, "
                f"egrid_year ({model_specs['egrid_year']}) should match eia_gen_year "
                f"({model_specs['eia_gen_year']}). This is because upstream processes "
                f"use eia_gen_year to calculate fuel use. The json-ld file "
                f"will not import correctly."
        )
check_model_specs()
