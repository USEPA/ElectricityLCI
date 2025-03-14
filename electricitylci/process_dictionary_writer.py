#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# process_dictionary_writer.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os
import time

import yaml
import pandas as pd

from electricitylci.globals import (
    data_dir,
    electricity_flow_name_generation_and_distribution,
    electricity_flow_name_consumption,
    elci_version
)
from electricitylci.utils import make_valid_version_num
from electricitylci.model_config import model_specs
if not model_specs.replace_egrid:
    from electricitylci.egrid_facilities import egrid_subregions
else:
    egrid_subregions = []


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """
This module is used by generation, generation mix, surplus, consumption and
distribution inventory generators to actually write the dictionaries in a
JSON-LD format as prescribed by OpenLCA software.

Portions of this code were cleaned using ChatGPTv3.5.

Last updated:
    2025-03-14
"""
__all__ = [
    'con_process_ref',
    'electricity_at_grid_flow',
    'electricity_at_user_flow',
    'exchange',
    'exchangeDqsystem',
    'exchange_table_creation_input',
    'exchange_table_creation_input_con_mix',
    'exchange_table_creation_input_genmix',
    'exchange_table_creation_input_international_mix',
    'exchange_table_creation_input_usaverage',
    'exchange_table_creation_output',
    'exchange_table_creation_ref',
    'exchange_table_creation_ref_cons',
    'flow_table_creation',
    'gen_process_ref',
    'location',
    'lookup_location_uuid',
    'processDqsystem',
    'process_description_creation',
    'process_doc_creation',
    'process_table_creation_con_mix',
    'process_table_creation_distribution',
    'process_table_creation_gen',
    'process_table_creation_genmix',
    'process_table_creation_surplus',
    'process_table_creation_usaverage',
    'ref_exchange_creator',
    'uncertainty_table_creation',
    'unit'
]


##############################################################################
# GLOBALS
##############################################################################
international = pd.read_csv(
    os.path.join(data_dir, "International_Electricity_Mix.csv")
)
international_reg = list(pd.unique(international['Subregion']))

# Read in general metadata to be used by all processes
with open(os.path.join(data_dir, "process_metadata.yml")) as f:
    metadata = yaml.safe_load(f)

# Read in process location uuids
location_UUID = pd.read_csv(os.path.join(data_dir, "location_UUIDs.csv"))

# Read in process name info
process_name = pd.read_csv(os.path.join(data_dir, "processname_1.csv"))
generation_name_parts = process_name[
    process_name["Stage"] == "generation"
].iloc[0]
generation_mix_name_parts = process_name[
    process_name["Stage"] == "generation mix"
].iloc[0]

generation_mix_name = (
    generation_mix_name_parts["Base name"]
    + "; "
    + generation_mix_name_parts["Location type"]
    + "; "
    + generation_mix_name_parts["Mix type"]
)

fuel_mix_name = 'Electricity; at grid; USaverage'
surplus_pool_name = "Electricity; at grid; surplus pool"
consumption_mix_name = "Electricity; at grid; consumption mix"
distribution_to_end_user_name = "Electricity; at user; consumption mix"

electricity_at_grid_flow = {
    "flowType": "PRODUCT_FLOW",
    "flowProperties": "",
    "name": electricity_flow_name_generation_and_distribution,
    "id": "",
    "category": (
        "Technosphere Flows/"
        "22: Utilities/"
        "2211: Electric Power Generation, Transmission and Distribution"),
}

electricity_at_user_flow = {
    "flowType": "PRODUCT_FLOW",
    "flowProperties": "",
    "name": electricity_flow_name_consumption,
    "id": "",
    "category": (
        "Technosphere Flows/"
        "22: Utilities/"
        "2211: Electric Power Generation, Transmission and Distribution"),
}

OLCA_TO_METADATA = {
    "timeDescription": None,
    "validUntil": "End_date",
    "validFrom": "Start_date",
    "technologyDescription": "TechnologyDescription",
    "dataCollectionDescription": "DataCollectionPeriod",
    "completenessDescription": "DataCompleteness",
    "dataSelectionDescription": "DataSelection",
    "reviewDetails": "DatasetOtherEvaluation",
    "dataTreatmentDescription": "DataTreatment",
    "inventoryMethodDescription": "LCIMethod",
    "modelingConstantsDescription": "ModelingConstants",
    "reviewer": "Reviewer",
    "samplingDescription": "SamplingProcedure",
    "sources": "Sources",
    "restrictionsDescription": "AccessUseRestrictions",
    "copyright": None,
    "creationDate": None,
    "dataDocumentor": "DataDocumentor",
    "dataGenerator": "DataGenerator",
    "dataSetOwner": "DatasetOwner",
    "intendedApplication": "IntendedApplication",
    "projectDescription": "ProjectDescription",
    "publication": None,
    "geographyDescription": None,
    "exchangeDqSystem": None,
    "dqSystem": None,
    "dqEntry": None
}

VALID_FUEL_CATS=[
    "default",
    "nuclear_upstream",
    "geothermal",
    "solar",
    "solarthermal",
    "wind",
    "consumption_mix",
    "generation_mix",
    "coal_upstream",
    "gas_upstream",
    "oil_upstream",
    "coal_transport_upstream",
    "construction_upstream",
    # Issue #150
    "solarpv_construction_upstream",
    "solartherm_construction_upstream",
    "wind_construction_upstream",
]


##############################################################################
# FUNCTIONS
##############################################################################
def process_metadata(entry):
    """
    Process and format metadata entries for reuse in other subsections.

    This function processes metadata entries to format them for reuse in other
    subsections. It can handle strings, integers, lists, and dictionaries.

    Parameters
    ----------
    entry : str, int, list, or dict
        The metadata entry to be processed.

    Returns
    -------
    str or dict
        The processed and formatted metadata entry.

    Notes
    -----
    - If the entry is a string or an integer, it is returned as is.
    - If the entry is a list, it joins its elements into a string with newline
      characters.
    - If the entry is a list of lists, it extracts the first element of each
      sub-list and joins them with newline characters.
    - If the entry is a dictionary, it recursively processes its values.

    Examples
    --------
    >>> entry = "This is a single string."
    >>> process_metadata(entry)
    'This is a single string.'
    >>> entry = ["Item 1", "Item 2", "Item 3"]
    >>> process_metadata(entry)
    'Item 1\nItem 2\nItem 3\n'
    >>> entry = [["A", "B"], ["C"]]
    >>> process_metadata(entry)
    'A\nBC'
    >>> entry = {"key1": "Value 1", "key2": ["Subvalue 1", "Subvalue 2"]}
    >>> process_metadata(entry)
    {'key1': 'Value 1', 'key2': 'Subvalue 1\nSubvalue 2\n'}
    """
    if isinstance(entry, (str, int)):
        return entry
    elif isinstance(entry, list):
        try:
            total_string = ""
            for x in entry:
                if isinstance(x, str):
                    total_string += x + "\n"
                elif isinstance(x, list):
                    if len(x) == 1:
                        total_string += x[0]
                    else:
                        total_string += "\n".join([y[0] for y in x])
            return total_string
        except ValueError:
            pass
    elif isinstance(entry, dict):
        for key in entry.keys():
            entry[key] = process_metadata(entry[key])
        return entry


def lookup_location_uuid(location):
    """
    Lookup the UUID (Unique Identifier) for a given location.

    This function takes a location name and attempts to find its corresponding
    UUID in a DataFrame called `location_UUID`. If a match is found, it returns
    the UUID. If no match is found, it returns an empty string.

    Parameters
    ----------
    location : str
        The name of the location for which you want to find the UUID.
        Example locations include, "United States", "Mexico", and "TRE".

    Returns
    -------
    str
        The UUID of the specified location, or an empty string if not found.

    Examples
    --------
    >>> location = "United States"
    >>> uuid = lookup_location_uuid(location)
    """
    try:
        uuid = location_UUID.loc[location_UUID["NAME"] == location]["REF_ID"].iloc[0]
    except IndexError:
        uuid = ""
    return uuid


def exchange(flw, exchanges_list):
    """
    Add a flow to a list of exchanges.

    This function takes a flow (`flw`) and appends it to a list of exchanges
    (`exchanges_list`), which is commonly used to represent exchanges in a
    process or system.

    Parameters
    ----------
    flw : str or dict
        The flow to be added to the list of exchanges.

    exchanges_list : list
        The list of exchanges to which the flow will be added.

    Returns
    -------
    list
        The updated list of exchanges after adding the specified flow.

    Notes
    -----
    Referenced in eia_io_trading.py and generation_mix.py.

    Examples
    --------
    >>> exchanges = []
    >>> flow1 = {"name": "Flow 1", "amount": 100, "unit": "kg"}
    >>> updated_exchanges = exchange(flow1, exchanges)
    """
    exchanges_list.append(flw)
    return exchanges_list


def exchange_table_creation_ref(data):
    """
    Create a reference exchange table entry for electricity at grid.

    This function generates a dictionary representing a reference exchange
    table entry based on the provided input data. The reference entry typically
    describes a specific flow, such as electricity at the grid, within a given
    region.

    Parameters
    ----------
    data : pd.DataFrame
        Unused.

    Returns
    -------
    dict
        A dictionary representing the reference exchange table entry.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({"Subregion": ["Region1"]})
    >>> reference_entry = exchange_table_creation_ref(data)
    """
    ar = {
        "internalId": "",
        "@type": "Exchange",
        "avoidedProduct": False,
        "flow": electricity_at_grid_flow,
        "flowProperty": "",
        "input": False,
        "quantitativeReference": True,
        "baseUncertainty": "",
        "provider": "",
        "amount": 1.0,
        "amountFormula": "",
        "unit": unit("MWh")
    }
    return ar


def exchange_table_creation_ref_cons(data):
    """
    Create a reference exchange table entry for electricity at user.

    This function generates a dictionary representing a reference exchange
    table entry based on the provided input data. The reference entry typically
    describes a specific flow, such as electricity at the grid, within a given
    region.

    Referenced in eia_io_trading.py

    Parameters
    ----------
    data : pd.DataFrame
        Unused.

    Returns
    -------
    dict
        A dictionary representing the reference exchange table entry.
    """
    ar = {
        "internalId": "",
        "@type": "Exchange",
        "avoidedProduct": False,
        "flow": electricity_at_user_flow,
        "flowProperty": "",
        "input": False,
        "quantitativeReference": True,
        "baseUncertainty": "",
        "provider": "",
        "amount": 1.0,
        "amountFormula": "",
        "unit": unit("MWh")
    }
    return ar


def gen_process_ref(fuel, reg):
    """
    Generate a reference process entry for electricity generation.

    This function creates a dictionary representing a reference process entry
    for electricity generation. It uses the provided fuel and region
    information to generate the process name, location, and category path.

    Parameters
    ----------
    fuel : str
        The fuel source used for electricity generation.

    reg : str
        The region where electricity generation takes place.

    Returns
    -------
    dict
        A dictionary representing the reference process entry.

    Notes
    -----
    Depends on global parameter, generation_name_parts.

    Examples
    --------
    >>> fuel_type = "Coal"
    >>> region = "Region1"
    >>> reference_process = gen_process_ref(fuel_type, region)
    """
    processref = {
        "name": (
            generation_name_parts["Base name"]
            + "; from "
            + str(fuel)
            + "; "
            + generation_name_parts["Location type"]
            + " - "
            + reg
        ),
        "location": reg,
        "processType": "UNIT_PROCESS",
        "categoryPath": [
            "22: Utilities",
            "2211: Electric Power Generation, Transmission and Distribution",
            fuel,
        ]
    }
    return processref


def con_process_ref(reg, ref_type="generation"):
    """
    Generate a reference process entry for electricity consumption or
    generation.

    This function creates a dictionary representing a reference process entry
    for electricity consumption or generation. It uses the provided region and
    reference type to generate the process name, location, and category path.

    If reference is to a consumption mix (for a distribution process), it uses
    the consumption mix name; otherwise, if the region is an eGrid region, it
    is a generation mix process; otherwise it is a surplus pool process.

    Parameters
    ----------
    reg : str
        The region associated with the reference process.

    ref_type : str, optional
        The type of reference process ("generation" or "consumption"). Default is "generation."

    Returns
    -------
    dict
        A dictionary representing the reference process entry.

    Notes
    -----
    Depends on global parameters, egrid_subregions, international_reg,
    fuel_mix_name, surplus_pool_name, consumption_mix_name.

    Examples
    --------
    >>> region = "Region1"
    >>> reference_type = "generation"
    >>> reference_process = con_process_ref(region, reference_type)
    """
    if ref_type == "consumption":
        name = consumption_mix_name + " - " + reg
    elif reg in egrid_subregions or reg in international_reg:
        name = generation_mix_name + " - " + reg
    elif ref_type == "generation_international":
        name = fuel_mix_name + " - " + reg
    else:
        name = surplus_pool_name + " - " + reg

    processref = {
        "name": name,
        "location": "US" if ref_type == "generation_international" else reg,
        "processType": "UNIT_PROCESS",
        "categoryPath": [
            "22: Utilities",
            "2211: Electric Power Generation, Transmission and Distribution",
        ]
    }
    return processref


def exchange_table_creation_input_genmix(database, fuelname):
    """
    Create an input exchange table entry for a generation mix.

    This function generates a dictionary representing an input exchange table
    entry for a generation mix based on the provided database and fuel name.
    The entry describes the input of a specific fuel source to the generation
    mix within a given region.

    Parameters
    ----------
    database : pd.DataFrame
        Input data, such as a DataFrame containing subregion and generation
        ratio information.

    fuelname : str
        The name of the fuel source.

    Returns
    -------
    dict
        A dictionary representing the input exchange table entry for the
        generation mix.

    Notes
    -----
    It appears that 'baseUncertainty' and 'pedigreeUncertainty' attributes are
    removed and a 'dqEntry' is used in their place.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...   "Subregion": ["Region1"], "Generation_Ratio": [0.8]})
    >>> fuel_type = "Coal"
    >>> input_exchange = exchange_table_creation_input_genmix(data, fuel_type)
    """
    region = database["Subregion"].iloc[0]
    ar = {
        "internalId": "",  # should be an integer
        "@type": "Exchange",
        "avoidedProduct": False,
        "flow": electricity_at_grid_flow,
        "flowProperty": "",
        "input": True,
        "quantitativeReference": True,
        "baseUncertainty": "",
        "provider": gen_process_ref(fuelname, region),
        "amount": database["Generation_Ratio"].iloc[0],
        "unit": unit("MWh"),
        "pedigreeUncertainty": "",
        "comment": "from " + fuelname + " - " + region,
        "uncertainty": ""
    }
    return ar


def exchange_table_creation_input_usaverage(database, fuelname):
    """
    Create an input exchange table entry for a US average electricity
    generation mix.

    This function generates a dictionary representing an input exchange table
    entry for a US average electricity generation mix based on the provided
    database and fuel name. The entry describes the input of a specific fuel
    source to the US average electricity generation mix.

    Parameters
    ----------
    database : pd.DataFrame
        Input data, such as a DataFrame containing subregion and generation
        ratio information.

    fuelname : str
        The name of the fuel source.

    Returns
    -------
    dict
        A dictionary representing the input exchange table entry for the US
        average electricity generation mix.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...   "Subregion": ["US Average"], "Generation_Ratio": [0.8]})
    >>> fuel_type = "Coal"
    >>> input_exchange = exchange_table_creation_input_usaverage(
    ...   data, fuel_type)
    """
    region = database["Subregion"].iloc[0]
    ar = {
        "internalId": "",
        "@type": "Exchange",
        "avoidedProduct": False,
        "flow": electricity_at_grid_flow,
        "flowProperty": "",
        "input": True,
        "quantitativeReference": True,
        "baseUncertainty": "",
        "provider": gen_process_ref(fuelname, region),
        "amount": database["Generation_Ratio"].iloc[0],
        "unit": unit("MWh"),
        "pedigreeUncertainty": "",
        "comment": "from " + fuelname + " - " + region,
        "uncertainty": ""
    }
    return ar


def exchange_table_creation_input_international_mix(
        database, ref_to_consumption=False):
    """
    Create an input exchange table entry for an international electricity
    generation mix.

    This function generates a dictionary representing an input exchange table
    entry for an international electricity generation mix based on the provided
    database. The entry describes the input of a specific fuel source to the
    international electricity generation mix.

    Parameters
    ----------
    database : pd.DataFrame
        Input data, such as a DataFrame containing fuel category and generation
        ratio.

    ref_to_consumption : bool, optional
        Whether the reference is to a consumption mix. Default is False.
        Unused.

    Returns
    -------
    dict
        A dictionary representing the input exchange table entry for the
        international electricity generation mix.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...   "FuelCategory": ["Coal"], "Generation_Ratio": [0.8]})
    >>> ref_to_consumption = False
    >>> input_exchange = exchange_table_creation_input_international_mix(
    ...   data, ref_to_consumption)
    """
    fuelname = database["FuelCategory"].iloc[0]
    ar = {
        "internalId": "",
        "@type": "Exchange",
        "avoidedProduct": False,
        "flow": electricity_at_grid_flow,
        "flowProperty": "",
        "input": True,
        "baseUncertainty": "",
        "provider": con_process_ref(fuelname, "generation_international"),
        "amount": database["Generation_Ratio"].iloc[0],
        "unit": unit("MWh"),
        "pedigreeUncertainty": "",
        "uncertainty": "",
        "comment": (
            "eGRID " + str(model_specs.egrid_year)
            + ". From US Average - " + fuelname
        )
    }
    return ar


def exchange_table_creation_input_con_mix(
        generation, loc, ref_to_consumption=False):
    """
    Create an input exchange table entry for a consumption mix.

    This function generates a dictionary representing an input exchange table
    entry for a consumption mix based on the provided generation amount and
    location.

    Parameters
    ----------
    generation : float
        The generation amount for the consumption mix.

    loc : str
        The location associated with the consumption mix.

    ref_to_consumption : bool, optional
        Whether the reference is to a consumption mix. Default is False.

    Returns
    -------
    dict
        A dictionary representing the input exchange table entry for the
        consumption mix.

    Examples
    --------
    >>> generation_amount = 100.0
    >>> location = "Region1"
    >>> ref_to_consumption = False
    >>> input_exchange = exchange_table_creation_input_con_mix(
    ...   generation_amount, location, ref_to_consumption)
    """
    ar = {
        "internalId": "",
        "@type": "Exchange",
        "avoidedProduct": False,
        "flow": electricity_at_grid_flow,
        "flowProperty": "",
        "input": True,
        "baseUncertainty": "",
        "provider": con_process_ref(loc, "consumption") if ref_to_consumption else con_process_ref(loc),
        "amount": generation,
        "unit": unit("MWh"),
        "pedigreeUncertainty": "",
        "uncertainty": "",
        "comment": "eGRID " + str(model_specs.egrid_year) + ". From " + loc
    }
    return ar


def process_table_creation_gen(fuelname, exchanges_list, region):
    """
    Create a process table entry for electricity generation.

    This function generates a dictionary representing a process table entry for
    electricity generation based on the provided fuel name, list of exchanges,
    and region.

    Parameters
    ----------
    fuelname : str
        The name of the fuel source used for electricity generation.

    exchanges_list : list
        The list of exchanges associated with the electricity generation
        process.

    region : str
        The region where electricity generation takes place.

    Returns
    -------
    dict
        A dictionary representing the process table entry for electricity
        generation.

    Examples
    --------
    >>> fuel_type = "Coal"
    >>> exchanges = [{"flow": "Electricity", "amount": 100, "unit": "MWh"}]
    >>> region = "Region1"
    >>> generation_process = process_table_creation_gen(
    ...    fuel_type, exchanges, region)
    """
    ar = {
        "@type": "Process",
        "allocationFactors": "",
        "defaultAllocationMethod": "",
        "exchanges": exchanges_list,
        "location": location(region),
        "parameters": "",
        "processDocumentation": process_doc_creation(),
        "processType": "UNIT_PROCESS",
        "name": (
            generation_name_parts["Base name"]
            + "; from "
            + str(fuelname)
            + "; "
            + generation_name_parts["Location type"]
        ),
        "category": (
            "22: Utilities/2211: Electric Power Generation, Transmission and Distribution/"
            + fuelname
        ),
        "description": (
            "Electricity from "
            + str(fuelname)
            + " produced at generating facilities in the "
            + str(region)
            + " region"
        )
    }
    try:
        # Use the software version number as the process version
        ar["version"] = make_valid_version_num(elci_version)
    except:
        # Set to 1 by default
        ar["version"] = 1
    return ar


def location(region):
    """
    Create a location entry based on a region.

    This function generates a dictionary representing a location entry based on
    the provided region. It includes the region's ID, type, and name.

    Parameters
    ----------
    region : str
        The region for which the location entry is created.

    Returns
    -------
    dict
        A dictionary representing the location entry for the specified region.

    Examples
    --------
    >>> region_name = "United States"
    >>> location_entry = location(region_name)
    """
    ar = {
        "id": lookup_location_uuid(region),
        "type": "Location",
        "name": region
    }
    return ar


def process_doc_creation(process_type="default"):
    """
    Create a process metadata dictionary specific to a given process type.

    This function generates a dictionary with process metadata based on the
    provided process type. It maps certain keys from OLCA to Metadata and
    retrieves values from the metadata based on the process type and keys.

    Parameters
    ----------
    process_type : str, optional
        One of the process types described in VALID_FUEL_CATS. Default is
        "default".

    Returns
    -------
    dict
        A dictionary with process metadata.

    Examples
    --------
    >>> process_type = "Coal"
    >>> metadata_dict = process_doc_creation(process_type)
    """
    from electricitylci.generation import get_generation_years
    try:
        assert process_type in VALID_FUEL_CATS, f"Invalid process_type ({process_type}), using default"
    except AssertionError:
        process_type = "default"

    if model_specs.replace_egrid is True:
        subkey = "replace_egrid"
    else:
        subkey = "use_egrid"

    global year
    ar = dict()

    for key in OLCA_TO_METADATA.keys():
        if OLCA_TO_METADATA[key] is not None:
            try:
                ar[key] = metadata[process_type][OLCA_TO_METADATA[key]]
            except KeyError:
                logging.debug(
                    f"Failed first key ({key}), trying subkey: {subkey}")
                try:
                    ar[key] = metadata[process_type][subkey][OLCA_TO_METADATA[key]]
                    logging.debug(
                        "Failed subkey, likely no entry in metadata for "
                        f"{process_type}:{key}")
                except KeyError:
                    ar[key] = metadata["default"][OLCA_TO_METADATA[key]]
            except TypeError:
                logging.debug(
                    "Failed first key, likely no metadata defined for "
                    f"{process_type}")
                process_type = "default"
                ar[key] = metadata[process_type][OLCA_TO_METADATA[key]]

    ar["timeDescription"] = ""
    # default valid year is the range of generation years
    if not ar["validUntil"]:
        #Hot fix for https://github.com/USEPA/ElectricityLCI/issues/244
        year_range = get_generation_years()
        ar["validUntil"] = "12/31/" + str(max(year_range))
        ar["validFrom"] = "1/1/" + str(min(year_range))
    ar["sources"] = [x for x in ar["sources"].values()]
    ar["copyright"] = False
    ar["creationDate"] = time.time()
    ar["publication"] = ""
    ar["geographyDescription"] = ""
    ar["exchangeDqSystem"] = exchangeDqsystem()
    ar["dqSystem"] = processDqsystem()
    # Temp place holder for process DQ scores
    ar["dqEntry"] = "(5;5)"
    ar["description"] = process_description_creation(process_type)

    return ar


def process_description_creation(process_type="fossil"):
    """
    Create a process description based on a given process type.

    This function generates a process description based on the provided process
    type. It retrieves the description string from metadata based on the
    process type and appends additional information about the process creation.

    Parameters
    ----------
    process_type : str, optional
        One of the process types described in VALID_FUEL_CATS.
        Default is "fossil".

    Returns
    -------
    str
        A string representing the process description.

    Examples
    --------
    >>> process_type = "Coal"
    >>> description = process_description_creation(process_type)
    """
    try:
        assert process_type in VALID_FUEL_CATS, f"Invalid process_type ({process_type}), using default"
    except AssertionError:
        process_type = "default"

    if model_specs.replace_egrid is True:
        subkey = "replace_egrid"
    else:
        subkey = "use_egrid"

    global year
    key = "Description"

    try:
        desc_string = metadata[process_type][key]
    except KeyError:
        logging.debug(
            f"Failed first key ({key}), trying subkey: {subkey}")
        try:
            desc_string = metadata[process_type][subkey][key]
            logging.debug(
                "Failed subkey, likely no entry in metadata for "
                f"{process_type}:{key}")
        except KeyError:
            desc_string = metadata["default"][key]
    except TypeError:
        logging.debug(
            f"Failed first key, likely no metadata defined for {process_type}")
        process_type = "default"
        desc_string = metadata[process_type][key]

    desc_string = (
        desc_string
        + " This process was created with ElectricityLCI "
        + "(https://github.com/USEPA/ElectricityLCI) version "
        + elci_version
        + " using the " + model_specs.model_name + " configuration.")

    return desc_string


def exchangeDqsystem():
    """
    Create an exchange data quality system entry.

    This function generates a dictionary representing an exchange data quality
    system entry for US EPA - Flow Pedigree Matrix. It includes the type, ID,
    and name of the data quality system.

    Notes
    -----
    The universally unique identifier included with this dictionary is
    formatted as version 4.0.

    Returns
    -------
    dict
        A dictionary representing the exchange data quality system entry.

    Examples
    --------
    >>> dq_system = exchangeDqsystem()
    """
    ar = dict()
    ar["@type"] = "DQSystem"
    ar["@id"] = "d13b2bc4-5e84-4cc8-a6be-9101ebb252ff"
    ar["name"] = "US EPA - Flow Pedigree Matrix"
    return ar


def processDqsystem():
    """
    Create a process data quality system entry.

    This function generates a dictionary representing a process data quality
    system entry for the US EPA - Process Pedigree Matrix. It includes the
    type, ID, and name of the data quality system for processes.

    Notes
    -----
    The universally unique identifier included with this dictionary is
    formatted as version 4.0.

    Returns
    -------
    dict
        A dictionary representing the process data quality system entry.

    Examples
    --------
    >>> dq_system = processDqsystem()
    """
    ar = dict()
    ar["@type"] = "DQSystem"
    ar["@id"] = "70bf370f-9912-4ec1-baa3-fbd4eaf85a10"
    ar["name"] = "US EPA - Process Pedigree Matrix"
    return ar


def exchange_table_creation_input(data):
    """
    Create an input exchange table entry based on provided data.

    This function generates a dictionary representing an input exchange table
    entry based on the provided data, such as emission factor and unit.

    Parameters
    ----------
    data : pd.DataFrame
        Input data, such as a DataFrame containing emission factor, unit, and
        uncertainty.

    Returns
    -------
    dict
        A dictionary representing the input exchange table entry.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({"Emission_factor": [0.5], "Unit": ["kg/MWh"]})
    >>> input_exchange = exchange_table_creation_input(data)
    """
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = flow_table_creation(data)
    ar["flowProperty"] = ""
    ar["input"] = True
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amount"] = data["Emission_factor"].iloc[0]
    ar["amountFormula"] = "  "
    ar["unit"] = unit(data["Unit"].iloc[0])
    ar["dqEntry"] = ""
    ar["pedigreeUncertainty"] = ""
    ar["uncertainty"] = uncertainty_table_creation(data)

    return ar


def unit(unt):
    """
    Create a unit entry based on the provided unit name.

    This function generates a dictionary representing a unit entry based on the
    provided unit name.

    Parameters
    ----------
    unt : str
        The unit name for which the unit entry is created.

    Returns
    -------
    dict
        A dictionary representing the unit entry for the specified unit name.

    Notes
    -----
    For more information on the attributes for Unit object, see
    https://greendelta.github.io/olca-schema/classes/Unit.html

    Examples
    --------
    >>> unit_name = "kg/MWh"
    >>> unit_entry = unit(unit_name)
    """
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Unit"
    ar["name"] = unt
    return ar


def exchange_table_creation_output(data):
    """
    Create an output exchange table entry based on provided data.

    This function generates a dictionary representing an output exchange table
    entry based on the provided data, such as emission factor, unit, and uncertainty.

    Parameters
    ----------
    data : pd.DataFrame
        Output data, such as a DataFrame containing emission factor, unit, and
        uncertainty.

    Returns
    -------
    dict
        A dictionary representing the output exchange table entry.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...     "Year": [2023],
    ...     "Source": ["Example Source"],
    ...     "Emission_factor": [0.5],
    ...     "Unit": ["kg/MWh"],
    ...     "DataReliability": [0.9],
    ...     "TemporalCorrelation": [0.8],
    ...     "GeographicalCorrelation": [0.7],
    ...     "TechnologicalCorrelation": [0.6],
    ...     "DataCollection": [0.95]
    ... })
    >>> output_exchange = exchange_table_creation_output(data)
    """
    year = data["Year"].iloc[0]
    source = data["Source"].iloc[0]
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = flow_table_creation(data)
    ar["flowProperty"] = ""
    ar["input"] = False
    ar["quantitativeReference"] = False
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amount"] = data["Emission_factor"].iloc[0]
    ar["amountFormula"] = ""
    ar["unit"] = unit(data["Unit"].iloc[0])
    ar["pedigreeUncertainty"] = ""
    ar["dqEntry"] = (
        "("
        + str(round(data["DataReliability"].iloc[0], 1))
        + ";"
        + str(round(data["TemporalCorrelation"].iloc[0], 1))
        + ";"
        + str(round(data["GeographicalCorrelation"].iloc[0], 1))
        + ";"
        + str(round(data["TechnologicalCorrelation"].iloc[0], 1))
        + ";"
        + str(round(data["DataCollection"].iloc[0], 1))
        + ")"
    )
    ar["uncertainty"] = uncertainty_table_creation(data)
    ar["comment"] = str(source) + " " + str(year)

    return ar


def uncertainty_table_creation(data):
    """
    Create an uncertainty table entry based on provided data.

    This function generates a dictionary representing an uncertainty table
    entry based on the provided data, such as geometric mean, geometric
    standard deviation, maximum, and minimum values.

    Assumes a log-normal distribution.

    Parameters
    ----------
    data : pd.DataFrame
        Data containing uncertainty information, including geometric mean,
        geometric standard deviation, maximum, and minimum values.

    Returns
    -------
    dict
        A dictionary representing the uncertainty table entry.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...     "GeomMean": [1.0],
    ...     "GeomSD": [0.2],
    ...     "Maximum": [1.5],
    ...     "Minimum": [0.8]
    ... })
    >>> uncertainty_entry = uncertainty_table_creation(data)
    """
    ar = dict()

    gm = data["GeomMean"].iloc[0]
    gs = data["GeomSD"].iloc[0]

    # NaN checking at its best.
    if gm == gm and gs == gs:
        ar["geomMean"] = gm
        ar["geomSd"] = gs
        ar["distributionType"] = "Logarithmic Normal Distribution"

    # NOTE: here is good place to check other values to implement
    #       alternative uncertainty (e.g., uniform, triangle)

    return ar


def flow_table_creation(data):
    """
    Create a flow table entry based on provided data.

    This function generates a dictionary representing a flow table entry
    based on the provided data, including flow type, flow name, UUID, and
    category.

    Parameters
    ----------
    data : pd.DataFrame
        Data containing flow information, including flow type, name, UUID, and
        category.

    Returns
    -------
    dict
        A dictionary representing the flow table entry.

    Examples
    --------
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...     "FlowType": ["ELEMENTARY_FLOW"],
    ...     "FlowName": ["Example Flow"],
    ...     "FlowUUID": ["123456"],
    ...     "Compartment": ["Air/Emission/CO2"]
    ... })
    >>> flow_entry = flow_table_creation(data)
    """
    ar = dict()
    flowtype = data["FlowType"].iloc[0]
    ar["flowType"] = flowtype
    ar["flowProperties"] = ""
    ar["name"] = data["FlowName"].iloc[0][:255]  # Corrected string slicing
    ar["id"] = data["FlowUUID"].iloc[0]
    comp = str(data["Compartment"].iloc[0])

    if (flowtype == "ELEMENTARY_FLOW") and (comp != ""):
        if "emission" in comp or "resource" in comp:
            ar["category"] = "Elementary Flows/" + comp
        elif "input" in comp:
            ar["category"] = "Elementary Flows/resource"
        else:
            ar["category"] = "Elementary Flows/emission/" + comp.lstrip("/")
    elif (flowtype == "PRODUCT_FLOW") and (comp != ""):
        ar["category"] = comp
    elif flowtype == "WASTE_FLOW":
        ar["category"] = comp
    else:
        # Assume this is electricity or a byproduct
        ar["category"] = (
            "Technosphere Flows/22: Utilities/"
            "2211: Electric Power Generation, Transmission and Distribution")

    return ar


def ref_exchange_creator(electricity_flow=electricity_at_grid_flow):
    """
    Create a reference exchange based on the provided electricity flow.

    This function generates a dictionary representing a reference exchange with
    the provided electricity flow and default values for other properties.

    Parameters
    ----------
    electricity_flow : str, optional
        The electricity flow to use in the reference exchange. Default is
        'electricity_at_grid_flow'.

    Returns
    -------
    dict
        A dictionary representing the reference exchange.

    Examples
    --------
    >>> ref_exchange = ref_exchange_creator()
    """
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = electricity_flow
    ar["flowProperty"] = ""
    ar["input"] = False
    ar["quantitativeReference"] = True
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amount"] = 1.0
    ar["amountFormula"] = ""
    ar["unit"] = unit("MWh")
    ar["location"] = ""

    return ar


def process_table_creation_con_mix(region, exchanges_list):
    """
    Create a process table entry for a consumption mix process.

    This function generates a dictionary representing a process table entry for
    a consumption mix process with the provided region and list of exchanges.

    Parameters
    ----------
    region : str
        The region for which the consumption mix process is created.
    exchanges_list : list
        A list of exchanges associated with the consumption mix process.

    Returns
    -------
    dict
        A dictionary representing the consumption mix process.

    Examples
    --------
    >>> region = "Example Region"
    >>> exchanges = [exchange_1, exchange_2]
    >>> con_mix_process = process_table_creation_con_mix(region, exchanges)
    """
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location(region)
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation(
        process_type="consumption_mix")
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = consumption_mix_name + " - " + region
    ar["category"] = (
        "22: Utilities/"
        "2211: Electric Power Generation, Transmission and Distribution")
    ar["description"] = (
        "Electricity consumption mix using power plants in the "
        + str(region) + " region.")
    ar["description"] = (ar["description"]
        + " This process was created with ElectricityLCI "
        + "(https://github.com/USEPA/ElectricityLCI) version " + elci_version
        + " using the " + model_specs.model_name + " configuration."
    )
    ar["version"] = make_valid_version_num(elci_version)

    return ar


def process_table_creation_genmix(region, exchanges_list):
    """
    Create a dictionary representing a process table for a generation mix.

    Parameters
    ----------
    region : str
        The region for which the generation mix is created.

    exchanges_list : list
        A list of exchanges for the process.

    Returns
    -------
    dict
        A dictionary representing the process table.

    Notes
    -----
    This function creates a dictionary to represent a process table for a
    generation mix. It populates the dictionary with various key-value pairs,
    including region-specific information and exchanges.

    Examples
    --------
    >>> region = "ExampleRegion"
    >>> exchanges = [exchange1, exchange2, exchange3]
    >>> process_table = process_table_creation_genmix(region, exchanges)
    """
    process_dict = {
        "@type": "Process",
        "allocationFactors": "",
        "defaultAllocationMethod": "",
        "exchanges": exchanges_list,
        "location": location(region),
        "parameters": "",
        "processDocumentation": process_doc_creation(
            process_type="generation_mix"),
        "processType": "UNIT_PROCESS",
        "name": f"{generation_mix_name} - {region}",
        "category": (
            "22: Utilities/2211: Electric Power Generation, "
            "Transmission and Distribution"),
        "description": (
            f"Electricity generation mix in the {region} region. "
            "This process was created with ElectricityLCI "
            "(https://github.com/USEPA/ElectricityLCI) version "
            f"{elci_version} using the {model_specs.model_name} "
            "configuration."),
        "version": make_valid_version_num(elci_version)
    }
    return process_dict


def process_table_creation_usaverage(fuel, exchanges_list):
    """
    Create a process table entry for a US Average fuel mix process.

    This function generates a dictionary representing a process table entry for
    a US Average fuel mix process with the provided fuel and list of exchanges.

    Parameters
    ----------
    fuel : str
        The fuel type for which the US Average fuel mix process is created.
    exchanges_list : list
        A list of exchanges associated with the US Average fuel mix process.

    Returns
    -------
    dict
        A dictionary representing the US Average fuel mix process.

    Examples
    --------
    >>> fuel_type = "Natural Gas"
    >>> exchanges = [exchange_1, exchange_2]
    >>> usaverage_process = process_table_creation_usaverage(fuel_type, exchanges)
    """
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location('US')
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation(process_type="fuel_mix")
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = fuel_mix_name + " - " + str(fuel)
    ar["category"] = (
        "22: Utilities/"
        "2211: Electric Power Generation, Transmission and Distribution")
    ar["description"] = (
        "Electricity fuel US Average mix for the " + str(fuel) + " fuel.")
    ar["description"] = (ar["description"]
        + " This process was created with ElectricityLCI "
        + "(https://github.com/USEPA/ElectricityLCI) version " + elci_version
        + " using the " + model_specs.model_name + " configuration."
    )
    ar["version"] = make_valid_version_num(elci_version)

    return ar


def process_table_creation_surplus(region, exchanges_list):
    """
    Create a process table entry for a surplus pool process.

    This function generates a dictionary representing a process table entry for
    a surplus pool process with the provided region and list of exchanges.

    Parameters
    ----------
    region : str
        The region for which the surplus pool process is created.
    exchanges_list : list
        A list of exchanges associated with the surplus pool process.

    Returns
    -------
    dict
        A dictionary representing the surplus pool process.

    Examples
    --------
    >>> region = "Example Region"
    >>> exchanges = [exchange_1, exchange_2]
    >>> surplus_pool_process = process_table_creation_surplus(region, exchanges)
    """
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location(region)
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation()
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = surplus_pool_name + " - " + region
    ar["category"] = (
        "22: Utilities/"
        "2211: Electric Power Generation, Transmission and Distribution")
    ar["description"] = "Electricity surplus in the " + str(region) + " region."
    ar["description"]=(ar["description"]
        + " This process was created with ElectricityLCI "
        + "(https://github.com/USEPA/ElectricityLCI) version " + elci_version
        + " using the " + model_specs.model_name + " configuration."
    )
    ar["version"] = make_valid_version_num(elci_version)

    return ar


def process_table_creation_distribution(region, exchanges_list):
    """
    Create a process table entry for an electricity distribution process.

    Parameters
    ----------
    region : str
        The region for which the generation mix is created.
    exchanges_list : list
        A list of exchanges associated with the surplus fuel mix process.

    Returns
    -------
    dict
        A dictionary representing the distribution fuel mix process.
    """
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location(region)
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation()
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = distribution_to_end_user_name + " - " + region
    ar["category"] = (
        "22: Utilities/"
        "2211: Electric Power Generation, Transmission and Distribution")
    ar["description"] = (
        "Electricity distribution to end user in the "
        + str(region)
        + " region."
    )
    ar["description"]=(ar["description"]
        + " This process was created with ElectricityLCI "
        + "(https://github.com/USEPA/ElectricityLCI) version " + elci_version
        + " using the " + model_specs.model_name + " configuration."
    )
    ar["version"] = make_valid_version_num(elci_version)

    return ar


##############################################################################
# POST-PROCESSING GLOBALS
##############################################################################
for key in metadata.keys():
    metadata[key] = process_metadata(metadata[key])


##############################################################################
# MAIN
##############################################################################
if __name__=="__main__":
    p_docs = []
    for p in VALID_FUEL_CATS:
        p_docs.append(process_doc_creation(p))
    print("View p_docs in logger for debug1")
