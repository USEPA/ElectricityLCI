#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# model_config.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import datetime
import logging
import os

import yaml
import pandas as pd

from electricitylci.globals import modulepath
from electricitylci.globals import list_model_names_in_config
from electricitylci.globals import data_dir
from electricitylci.globals import output_dir


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module defines a class for specifying the baseline model
to be used, which can read the configuration settings from the associated
Model Config file (found in the modelconfig directory). Methods are available
for checking the validity of a Model Config YAML file and building the
ModelSpecs class.

This class is attached to the module, model_config, which, when imported by
all other modules, is accessible (but not mutable). Therefore, model
configuration settings are set once and shared with the rest of the Python
package. To change configuration settings, restart Python.

Last edited:
    2024-01-24
"""
__all__ = [
    "ConfigurationError",
    "ModelSpecs",
    "assign_model_name",
    "build_model_class",
    "check_model_specs",
]


##############################################################################
# CLASSES
##############################################################################
class ConfigurationError(Exception):
    """Exception raised for errors in the configuration file"""
    def __init__(self,message):
        self.message = message


class ModelSpecs:
    """ElectricityLCI model specifications class.

    Attributes
    ----------
    model_name : str
        The chosen model name (e.g., 'ELCI_1').
    electricity_lci_target_year : int
        The target year (e.g., 2018).
    regional_aggregation : str
        The aggregation level (e.g., 'BA' for Balancing Authority area).
    egrid_year : int
        The eGRID year (e.g., 2016).
    eia_gen_year : int
        The generation year for EIA data (e.g, 2016).
    replace_egrid : bool
        Whether eGRID should be replaced by EIA data.
    include_renewable_generation : bool
        Whether renewable fuel types should be included.
    include_netl_water : bool
        Whether NETL plant-level water use unit processes should be included.
    include_upstream_processes : bool
        Whether upstream unit processes should be included.
    inventories_of_interest : dict
        Data sources (keys) and years (values) for analysis.
        Keys include: 'eGRID', 'TRI', 'NEI', and 'RCRAInfo'.
    inventories : list
        List of inventory names (e.g., 'eGRID', 'TRI', 'NEI', and 'RCRAInfo').
    stewicombo_file : str, optional
        Unknown, defaults to NoneType.
    include_only_egrid_facilities_with_positive_generation : bool
        When true, filters out facilities with negative generation amounts.
    filter_on_efficiency : bool
        Turns on/off power plant efficiency filter.
    egrid_facility_efficiency_filters : dict
        Threshold for 'lower_efficiency' and 'upper_efficiency' cut-off values.
        Only used when filter_on_efficiency attribute is set to true.
    filter_on_min_plant_percent_generation_from_primary_fuel : bool
        Sets a minimum threshold for primary fuel categorization at the plant
        level. Plants without a fuel that meets the minimum threshold are
        filtered out (unless keep_mixed_plant_category is set to true).
    min_plant_percent_generation_from_primary_fuel_category : int
        Percent threshold for primary fuel categorization (e.g., 90).
    keep_mixed_plant_category : bool
        Whether to keep facilities without a primary fuel category
        (see primary fuel filter on minimum generation percent). If true,
        plants without a primary fuel (based on the minimum plant percent
        generation from primary fuel category) are categorized as "MIXED"
        fuel.
    filter_non_egrid_emission_on_NAICS : bool
        Unknown
    efficiency_of_distribution_grid : float
        Used to determine transmission and distribution losses.
    EPA_eGRID_trading : bool
        Whether to use the EPA trading method (based on eGRID regions).
    net_trading : bool
        Whether to use NETL's input-output method (based on BA areas).
    NETL_IO_trading_year : int
        Year associated with NETL's input-output net trading method
        (e.g., 2016).
    fedelemflowlist_version : str
        Version string (e.g., '1.0.2').
    fuel_name_file : str
        Name of a CSV file (e.g., 'fuelname_1.csv').
    fuel_name : pandas.DataFrame
        A dataframe representing the fuel name file CSV data.
        Columns include 'FuelList', 'Fuelname', 'Heatcontent',
        'ElementaryFlowInput', 'Category', 'Subcategory', 'CategoryUUID', and
        'SubcategoryUUID'.
    post_process_generation_emission_factors : bool
        Whether to do post processing.
    gen_mix_from_model_generation_data : bool, optional
        Defaults to false.
    namestr : str
        Absolute path to JSON-LD zip output file.
        File name includes the model name and current time stamp and is
        located by default in the output directory (see globals.py).
    """
    def __init__(self, model_specs, model_name):
        """Class initialization.

        Parameters
        ----------
        model_specs : dict
            A dictionary of configuration keywords and values read from a
            model config YAML.
        model_name : str
            _description_
        """
        self.model_name = model_name
        self.electricity_lci_target_year = model_specs[
            "electricity_lci_target_year"]
        self.regional_aggregation = model_specs["regional_aggregation"]
        self.egrid_year = model_specs["egrid_year"]
        self.eia_gen_year = model_specs["eia_gen_year"]
        # use 923 and cems rather than egrid, but still use the egrid_year
        # parameter to determine the data year
        self.replace_egrid = model_specs["replace_egrid"]

        self.include_renewable_generation = model_specs[
            "include_renewable_generation"]
        self.include_netl_water = model_specs["include_netl_water"]
        self.include_upstream_processes = model_specs[
            "include_upstream_processes"]
        self.inventories_of_interest = model_specs["inventories_of_interest"]
        self.inventories = list(model_specs["inventories_of_interest"])
        if "stewicombo_file" in model_specs:
            self.stewicombo_file = model_specs["stewicombo_file"]
        else:
            self.stewicombo_file = None
        self.include_only_egrid_facilities_with_positive_generation = model_specs["include_only_egrid_facilities_with_positive_generation"]
        self.filter_on_efficiency = model_specs['filter_on_efficiency']
        self.egrid_facility_efficiency_filters = model_specs[
            "egrid_facility_efficiency_filters"]
        self.filter_on_min_plant_percent_generation_from_primary_fuel = model_specs['filter_on_min_plant_percent_generation_from_primary_fuel']
        self.min_plant_percent_generation_from_primary_fuel_category = model_specs["min_plant_percent_generation_from_primary_fuel_category"]
        self.keep_mixed_plant_category = model_specs[
            'keep_mixed_plant_category']
        self.filter_non_egrid_emission_on_NAICS = model_specs[
            "filter_non_egrid_emission_on_NAICS"]
        self.efficiency_of_distribution_grid = model_specs[
            "efficiency_of_distribution_grid"]
        self.EPA_eGRID_trading = model_specs["EPA_eGRID_trading"]
        self.net_trading = model_specs["net_trading"]
        self.NETL_IO_trading_year = model_specs["NETL_IO_trading_year"]
        self.fedelemflowlist_version = model_specs["fedelemflowlist_version"]
        self.fuel_name_file = model_specs["fuel_name_file"]
        self.fuel_name = pd.read_csv(
            os.path.join(data_dir, self.fuel_name_file))
        self.post_process_generation_emission_factors = model_specs[
            "post_process_generation_emission_factors"]
        self.gen_mix_from_model_generation_data=False
        self.namestr = (
            f"{output_dir}/{model_name}_jsonld_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        )


##############################################################################
# FUNCTIONS
##############################################################################
def _load_model_specs(model_name):
    """Read a model specification YAML file.

    Parameters
    ----------
    model_name : str
        Model name (e.g., 'ELCI_1').

    Returns
    -------
    dict
        A dictionary based on configuration settings found in the model YAML.

    Raises
    ------
    ConfigurationError
        Raised if model configuration YAML file is not found.
    """
    logging.info('Loading model specs')
    try:
        path = os.path.join(
            modulepath, 'modelconfig', '{}_config.yml'.format(model_name))
        with open(path, 'r') as f:
            specs = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigurationError(
            "Model specs not found. "
            "Create a model specs file for the model of interest.")
    return specs


def assign_model_name():
    """Request user selection for ELCI model option.

    Returns
    -------
    str
        Model option (e.g., 'ELCI_1').
    """
    model_menu = list_model_names_in_config()
    print("Select a model number to use:")
    for k in model_menu.keys():
        print("\t" + str(k) + ": " + model_menu[k])
    model_num = input()
    try:
        model_name = model_menu[int(model_num)]
        print("Model " + model_name + " selected.")
    except:
        print('You must select the menu number for an existing model')
    return model_name


def build_model_class(model_name=None):
    """Creates a ModelSpecs class instance.

    Parameters
    ----------
    model_name : str, optional
        Model name (e.g., 'ELCI_1'), by default None

    Returns
    -------
    ModelSpecs
        An initialized ModelSpecs class with attributes assignment based on
        the YAML configuration file matching the given model name.
    """
    if not model_name:
        model_name = assign_model_name()
    # pull in model config vars
    specs = _load_model_specs(model_name)
    check_model_specs(specs)
    model_class = ModelSpecs(specs, model_name)
    logging.info(f'Model Specs for {model_class.model_name}')

    return model_class


def check_model_specs(model_specs):
    """Run a series of checks against user-defined configuration settings.

    Parameters
    ----------
    model_specs : dict
        A dictionary of model configuration values (e.g., as read from
        a YAML configuration file).

    Raises
    ------
    ConfigurationError
        Raised when regional aggregation method does not match EPA trading
        requirements or if eGRID year does not match EIA generation year when
        trying to replace eGRID with EIA.
    """
    # Check for consumption matching region selection
    logging.info('Checking model specs')
    if (model_specs["regional_aggregation"] in ["FERC, BA, US"]) and (
            model_specs["EPA_eGRID_trading"]):
        raise ConfigurationError(
            "EPA trading method is not compatible with selected regional "
            f"aggregation - {model_specs['regional_aggregation']}"
        )
    if (model_specs["regional_aggregation"] != "eGRID") and (
            model_specs["EPA_eGRID_trading"]):
        raise ConfigurationError(
            "EPA trading method is not compatible with selected regional "
            f"aggregation - {model_specs['regional_aggregation']}"
        )
    if not model_specs["replace_egrid"] and (
            model_specs["egrid_year"] != model_specs["eia_gen_year"]) and (
                model_specs["include_upstream_processes"]):
        raise ConfigurationError(
            "When using egrid data and adding upstream processes, "
            f"egrid_year ({model_specs['egrid_year']}) "
            "should match eia_gen_year "
            f"({model_specs['eia_gen_year']}). "
            "This is because upstream processes "
            "use eia_gen_year to calculate fuel use. The json-ld file "
            "will not import correctly."
        )
    logging.info("Checks passed!")
