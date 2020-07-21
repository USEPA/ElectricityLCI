import pandas as pd
from os.path import join
import datetime
import yaml

from electricitylci.globals import (modulepath,list_model_names_in_config,
                                    data_dir, output_dir)
#################

def assign_model_name():
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
    return model_name

# pull in model config vars
def load_model_specs(model_name):
    print('Loading model specs')
    try:
        path = join(modulepath, 'modelconfig', '{}_config.yml'.format(model_name))
        with open(path, 'r') as f:
            specs = yaml.safe_load(f)
    except FileNotFoundError:
        print("Model specs not found. Create a model specs file for the model of interest.")
    return specs

class ConfigurationError(Exception):
    """Exception raised for errors in the configuration file"""
    def __init__(self,message):
        self.message = message

def build_model_class(model_name=None):
    if not model_name:
        model_name = assign_model_name()
    specs = load_model_specs(model_name)
    check_model_specs(specs)
    model_specs = ModelSpecs(specs, model_name)
    print(f'Model Specs for {model_specs.model_name}')
    return model_specs


def check_model_specs(model_specs):
    # Check for consumption matching region selection
    print('Checking model specs')
    if model_specs["regional_aggregation"] in ["FERC, BA, US"] and model_specs["EPA_eGRID_trading"]:
        raise ConfigurationError(
            "EPA trading method is not compatible with selected regional "
            f"aggregation - {model_specs['regional_aggregation']}"
        )
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

class ModelSpecs:
    
    model_name = ''
    def __init__(self, model_specs, model_name):
        self.model_name = model_name
        self.electricity_lci_target_year = model_specs["electricity_lci_target_year"]
        self.regional_aggregation = model_specs["regional_aggregation"]
        self.egrid_year = model_specs["egrid_year"]
        self.eia_gen_year = model_specs["eia_gen_year"]
        # use 923 and cems rather than egrid, but still use the egrid_year
        # parameter to determine the data year
        self.replace_egrid = model_specs["replace_egrid"]
        
        self.include_renewable_generation = model_specs["include_renewable_generation"]
        self.include_netl_water = model_specs["include_netl_water"] 
        self.include_upstream_processes = model_specs["include_upstream_processes"]
        self.inventories_of_interest = model_specs["inventories_of_interest"]
        self.inventories = list(model_specs["inventories_of_interest"])
        self.include_only_egrid_facilities_with_positive_generation = model_specs["include_only_egrid_facilities_with_positive_generation"]
        self.filter_on_efficiency = model_specs['filter_on_efficiency']
        self.egrid_facility_efficiency_filters = model_specs["egrid_facility_efficiency_filters"]
        self.filter_on_min_plant_percent_generation_from_primary_fuel = model_specs['filter_on_min_plant_percent_generation_from_primary_fuel']
        self.min_plant_percent_generation_from_primary_fuel_category = model_specs["min_plant_percent_generation_from_primary_fuel_category"]
        self.keep_mixed_plant_category = model_specs['keep_mixed_plant_category']
        self.filter_non_egrid_emission_on_NAICS = model_specs["filter_non_egrid_emission_on_NAICS"]
        self.efficiency_of_distribution_grid = model_specs["efficiency_of_distribution_grid"]
        self.net_trading = model_specs["net_trading"]
        self.fedelemflowlist_version = model_specs["fedelemflowlist_version"]
        self.use_primaryfuel_for_coal = model_specs["use_primaryfuel_for_coal"]
        self.fuel_name_file = model_specs["fuel_name_file"]
        self.fuel_name = pd.read_csv(join(data_dir, self.fuel_name_file))
        self.post_process_generation_emission_factors = model_specs["post_process_generation_emission_factors"]
        self.gen_mix_from_model_generation_data=False
        self.namestr = (
            f"{output_dir}/{model_name}_jsonld_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            )
        
#model_specs = build_model_class('ELCI_1')