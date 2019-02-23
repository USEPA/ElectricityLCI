import json
import pandas as pd
from os.path import join

from electricitylci.globals import modulepath, data_dir

#################
#Set model_name here
model_name = 'ELCI_1'
#################

#pull in model config vars
try:
    with open(modulepath +'modelconfig/' + model_name + "_config.json") as cfg:
        model_specs = json.load(cfg)
except FileNotFoundError:
    print("Model specs not found. Create a model specs file for the model of interest.")

electricity_lci_target_year = model_specs["electricity_lci_target_year"]
egrid_year = model_specs["egrid_year"]

# use 923 and cems rather than egrid, but still use the egrid_year
# parameter to determine the data year
replace_egrid = model_specs["replace_egrid"]
region_column_name = model_specs["region_column_name"]
eia_gen_year = model_specs["eia_gen_year"]
inventories_of_interest = model_specs["inventories_of_interest"]
inventories = inventories_of_interest.keys()
include_only_egrid_facilities_with_positive_generation = model_specs["include_only_egrid_facilities_with_positive_generation"]
filter_on_efficiency = model_specs['filter_on_efficiency']
egrid_facility_efficiency_filters = model_specs["egrid_facility_efficiency_filters"]
filter_on_min_plant_percent_generation_from_primary_fuel = model_specs['filter_on_min_plant_percent_generation_from_primary_fuel']
min_plant_percent_generation_from_primary_fuel_category = model_specs["min_plant_percent_generation_from_primary_fuel_category"]
filter_non_egrid_emission_on_NAICS = model_specs["filter_non_egrid_emission_on_NAICS"]
efficiency_of_distribution_grid = model_specs["efficiency_of_distribution_grid"]
net_trading = model_specs["net_trading"]
fedelemflowlist_version = model_specs["fedelemflowlist_version"]
use_primaryfuel_for_coal = model_specs["use_primaryfuel_for_coal"]
fuel_name_file = model_specs["fuel_name_file"]
fuel_name = pd.read_csv(join(data_dir, fuel_name_file))
post_process_generation_emission_factors = model_specs["post_process_generation_emission_factors"]
gen_mix_from_model_generation_data=False