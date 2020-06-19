"""
Writes process data and metadata as a dictionary
The dictionary is based on the openLCA (OLCA) schema
This dictionary can be used for writing JSON-LD files or templates
"""

import time
import pandas as pd
from os.path import join
from electricitylci.globals import (
    data_dir,
    electricity_flow_name_generation_and_distribution,
    electricity_flow_name_consumption,
)
from electricitylci.model_config import (
        egrid_year,
        model_specs,
        electricity_lci_target_year,
        model_name
)
from electricitylci.egrid_facilities import egrid_subregions
import yaml
import logging
import pkg_resources  # part of setuptools
elci_version = pkg_resources.require("ElectricityLCI")[0].version

module_logger = logging.getLogger("process_dictionary_writer.py")
year = egrid_year
targetyear=electricity_lci_target_year

# Read in general metadata to be used by all processes
with open(join(data_dir, "process_metadata.yml")) as f:
    metadata=yaml.safe_load(f)


# Wanted to be able to reuse sections of the metadata in other subsections.
# in order to do this with yaml, we need to be able to process lists of lists.
# process_metadata makes this happen.
def process_metadata(entry):
    """Add docstring."""
    if isinstance(entry,str) or isinstance(entry,int):
        return entry
    elif isinstance(entry,list):
        try:
            total_string = ""
            for x in entry:
                if isinstance(x,str):
                    total_string=total_string+x+"\n"
                elif isinstance(x,list):
                    if len(x)==1:
                        total_string+=x[0]
                    else:
                        total_string=total_string+"\n".join([y[0] for y in x])
#            result = '\n'.join([x[0] for x in entry])
            return total_string
        except ValueError:
            pass

    elif isinstance(entry,dict):
        for key in entry.keys():
            entry[key] = process_metadata(entry[key])
        return entry


for key in metadata.keys():
    metadata[key]=process_metadata(metadata[key])

# Read in process location uuids
location_UUID = pd.read_csv(join(data_dir, "location_UUIDs.csv"))

def lookup_location_uuid(location):
    """Add docstring."""
    try:
        uuid = location_UUID.loc[location_UUID["NAME"] == location][
            "REF_ID"
        ].iloc[0]
    except IndexError:
        uuid = ""
    return uuid

# Read in process name info
process_name = pd.read_csv(join(data_dir, "processname_1.csv"))
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
surplus_pool_name = "Electricity; at grid; surplus pool"
consumption_mix_name = "Electricity; at grid; consumption mix"
distribution_to_end_user_name = "Electricity; at user; consumption mix"

electricity_at_grid_flow = {
    "flowType": "PRODUCT_FLOW",
    "flowProperties": "",
    "name": electricity_flow_name_generation_and_distribution,
    "id": "",
    "category": "Technosphere Flows/22: Utilities/2211: Electric Power Generation, Transmission and Distribution",
}

electricity_at_user_flow = {
    "flowType": "PRODUCT_FLOW",
    "flowProperties": "",
    "name": electricity_flow_name_consumption,
    "id": "",
    "category": "Technosphere Flows/22: Utilities/2211: Electric Power Generation, Transmission and Distribution",
}


def exchange(flw, exchanges_list):
    """Add docstring."""
    exchanges_list.append(flw)
    return exchanges_list


def exchange_table_creation_ref(data):
    """Add docstring."""
    region = data["Subregion"].iloc[0]
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = electricity_at_grid_flow
    ar["flowProperty"] = ""
    ar["input"] = False
    ar["quantitativeReference"] = True
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amount"] = 1.0
    ar["amountFormula"] = ""
    ar["unit"] = unit("MWh")
    return ar


def exchange_table_creation_ref_cons(data):
    """Add docstring."""
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = electricity_at_grid_flow
    ar["flowProperty"] = ""
    ar["input"] = False
    ar["quantitativeReference"] = True
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amount"] = 1.0
    ar["amountFormula"] = ""
    ar["unit"] = unit("MWh")
    return ar


def gen_process_ref(fuel, reg):
    """Add docstring."""
    processref = dict()
    processref["name"] = (
        generation_name_parts["Base name"]
        + "; from "
        + str(fuel)
        + "; "
        + generation_name_parts["Location type"]
        +" - "
        +reg
    )
    processref["location"] = reg
    processref["processType"] = "UNIT_PROCESS"
    processref["categoryPath"] = [
        "22: Utilities",
        "2211: Electric Power Generation, Transmission and Distribution",
        fuel,
    ]
    return processref


def con_process_ref(reg, ref_type="generation"):
    """Add docstring."""
    # If ref is to a consunmption mix (for a distribution process), use consumption mix name
    # If not, if the region is an egrid regions, its a generation mix process; otherwise its a surplus pool process
    if ref_type == "consumption":
        name = consumption_mix_name +" - "+reg
    elif reg in egrid_subregions:
        name = generation_mix_name +" - "+reg
    else:
        name = surplus_pool_name + " - "+reg
    processref = dict()
    processref["name"] = name
    processref["location"] = reg
    processref["processType"] = "UNIT_PROCESS"
    processref["categoryPath"] = [
        "22: Utilities",
        "2211: Electric Power Generation, Transmission and Distribution",
    ]
    return processref


def exchange_table_creation_input_genmix(database, fuelname):
    """Add docstring."""
    region = database["Subregion"].iloc[0]
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = electricity_at_grid_flow
    ar["flowProperty"] = ""
    ar["input"] = True
    ar["quantitativeReference"] = "True"
    ar["baseUncertainty"] = ""
    ar["provider"] = gen_process_ref(fuelname, region)
    ar["amount"] = database["Generation_Ratio"].iloc[0]
    ar["unit"] = unit("MWh")
    ar["pedigreeUncertainty"] = ""
    # ar['category']='22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+fuelname
    ar["comment"] = "from " + fuelname +" - "+ region
    ar["uncertainty"] = ""
    return ar


def exchange_table_creation_input_con_mix(
    generation, loc, ref_to_consumption=False
):
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = electricity_at_grid_flow
    ar["flowProperty"] = ""
    ar["input"] = True
    ar["baseUncertainty"] = ""
    if ref_to_consumption:
        ar["provider"] = con_process_ref(loc, "consumption")
    else:
        ar["provider"] = con_process_ref(loc)
    ar["amount"] = generation
    ar["unit"] = unit("MWh")
    ar["pedigreeUncertainty"] = ""
    ar["uncertainty"] = ""
    ar["comment"] = "eGRID " + str(year) + ". From " + loc
    # ar['location'] = location(loc)
    return ar


def process_table_creation_gen(fuelname, exchanges_list, region):
    """Add docstring."""
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location(region)
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation()
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = (
        generation_name_parts["Base name"]
        + "; from "
        + str(fuelname)
        + "; "
        + generation_name_parts["Location type"]
    )
    ar["category"] = (
        "22: Utilities/2211: Electric Power Generation, Transmission and Distribution/"
        + fuelname
    )
    ar["description"] = (
        "Electricity from "
        + str(fuelname)
        + " produced at generating facilities in the "
        + str(region)
        + " region"
    )
    return ar


def process_table_creation_genmix(region, exchanges_list):
    """Add docstring."""
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location(region)
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation(process_type="generation_mix")
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = generation_mix_name + " - " + str(region)
    ar[
        "category"
    ] = "22: Utilities/2211: Electric Power Generation, Transmission and Distribution"
    ar["description"] = (
        "Electricity generation mix in the " + str(region) + " region"
    )
    return ar


# Will be used later
# def category():
#
#     global fuelname;
#     ar = {'':''}
#     ar['@id'] = ''
#     ar['@type'] = 'Category'
#     ar['name'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+str(fuelname)
#     del ar['']
#     return ar


# Will be used later
def location(region):
    """Add docstring."""
    ar = dict()
    ar["id"] = lookup_location_uuid(region)
    ar["type"] = "Location"
    ar["name"] = region
    return ar


OLCA_TO_METADATA={
        "timeDescription":None,
        "validUntil":"End_date",
        "validFrom":"Start_date",
        "technologyDescription":"TechnologyDescription",
        "dataCollectionDescription":"DataCollectionPeriod",
        "completenessDescription":"DataCompleteness",
        "dataSelectionDescription":"DataSelection",
        "reviewDetails":"DatasetOtherEvaluation",
        "dataTreatmentDescription":"DataTreatment",
        "inventoryMethodDescription":"LCIMethod",
        "modelingConstantsDescription":"ModelingConstants",
        "reviewer":"Reviewer",
        "samplingDescription":"SamplingProcedure",
        "sources":"Sources",
        "restrictionsDescription":"AccessUseRestrictions",
        "copyright":None,
        "creationDate":None,
        "dataDocumentor":"DataDocumentor",
        "dataGenerator":"DataGenerator",
        "dataSetOwner":"DatasetOwner",
        "intendedApplication":"IntendedApplication",
        "projectDescription":"ProjectDescription",
        "publication":None,
        "geographyDescription":None,
        "exchangeDqSystem":None,
        "dqSystem":None,
        "dqEntry":None
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
        "construction_upstream"
]


def process_doc_creation(process_type="default"):
    """
    Creates a process metadata dictionary specific to a given process type
    :param process_type: One of process types described in VALID_FUEL_CATS
    :return: A dictionary with process metadata
    """

    try:
        assert process_type in VALID_FUEL_CATS, f"Invalid process_type ({process_type}), using default"
    except AssertionError:
        process_type="default"
    if model_specs["replace_egrid"] is True:
        subkey = "replace_egrid"
    else:
        subkey= "use_egrid"
    global year
    ar = dict()
    for key in OLCA_TO_METADATA.keys():
        if OLCA_TO_METADATA[key] is not None:
            try:
                ar[key]=metadata[process_type][OLCA_TO_METADATA[key]]
            except KeyError:
                module_logger.debug(f"Failed first key ({key}), trying subkey: {subkey}")
                try:
                    ar[key]=metadata[process_type][subkey][OLCA_TO_METADATA[key]]
                    module_logger.debug(f"Failed subkey, likely no entry in metadata for {process_type}:{key}")
                except KeyError:
                    ar[key]=metadata["default"][OLCA_TO_METADATA[key]]
            except TypeError:
                module_logger.debug(f"Failed first key, likely no metadata defined for {process_type}")
                process_type="default"
                ar[key]=metadata[process_type][OLCA_TO_METADATA[key]]
    ar["timeDescription"] = ""
    if not ar["validUntil"]:
        ar["validUntil"] = "12/31/"+str(targetyear)
        ar["validFrom"] = "1/1/"+str(targetyear)
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
    """Add docstring."""
    try:
        assert process_type in VALID_FUEL_CATS, f"Invalid process_type ({process_type}), using default"
    except AssertionError:
        process_type = "default"
    if model_specs["replace_egrid"] is True:
        subkey = "replace_egrid"
    else:
        subkey = "use_egrid"
    global year
    key = "Description"
    try:
        desc_string = metadata[process_type][key]
    except KeyError:
        module_logger.debug(f"Failed first key ({key}), trying subkey: {subkey}")
        try:
            desc_string = metadata[process_type][subkey][key]
            module_logger.debug(
                "Failed subkey, likely no entry in metadata for {process_type}:{key}")
        except KeyError:
            desc_string = metadata["default"][key]
    except TypeError:
        module_logger.debug(f"Failed first key, likely no metadata defined for {process_type}")
        process_type = "default"
        desc_string = metadata[process_type][key]
    desc_string = desc_string + " This process was created with ElectricityLCI " \
                  "(https://github.com/USEPA/ElectricityLCI) version " + elci_version\
                  + " using the " + model_name + " model."

    return desc_string

def exchangeDqsystem():
    """Add docstring."""
    ar = dict()
    ar["@type"] = "DQSystem"
    ar["@id"] = "d13b2bc4-5e84-4cc8-a6be-9101ebb252ff"
    ar["name"] = "US EPA - Flow Pedigree Matrix"
    return ar

def processDqsystem():
    """Add docstring."""
    ar = dict()
    ar["@type"] = "DQSystem"
    ar["@id"] = "70bf370f-9912-4ec1-baa3-fbd4eaf85a10"
    ar["name"] = "US EPA - Process Pedigree Matrix"
    return ar

def exchange_table_creation_input(data):
    """Add docstring."""
    year = data["Year"].iloc[0]
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
    ar["comment"] = "eGRID " + str(year)
    # if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
    #   ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    # elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
    #   ar['category'] = 'Waste flows/'
    # else:
    #   ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+fuelname
    return ar


def unit(unt):
    """Add docstring."""
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Unit"
    ar["name"] = unt
    return ar


def exchange_table_creation_output(data):
    """Add docstring."""
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
        + str(round(data["ReliabilityScore"].iloc[0], 1))
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
    # if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
    #  ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    # elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
    #  ar['category'] = 'Waste flows/'
    # else:
    #  ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+data['FlowName'].iloc[0]

    return ar


def uncertainty_table_creation(data):
    """Add docstring."""
    ar = dict()
    #    print(data["GeomMean"].iloc[0] + ' - ' +data["GeomSD"].iloc[0])
    if data["GeomMean"].iloc[0] is not None:
        ar["geomMean"] = str(float(data["GeomMean"].iloc[0]))
    if data["GeomSD"].iloc[0] is not None:
        ar["geomSd"] = str(float(data["GeomSD"].iloc[0]))
    ar["distributionType"] = "Logarithmic Normal Distribution"
    ar["mean"] = ""
    ar["meanFormula"] = ""
    ar["geomMeanFormula"] = ""
    ar["maximum"] = data["Maximum"].iloc[0]
    ar["minimum"] = data["Minimum"].iloc[0]
    ar["minimumFormula"] = ""
    ar["sd"] = ""
    ar["sdFormula"] = ""
    ar["geomSdFormula"] = ""
    ar["mode"] = ""
    ar["modeFormula"] = ""
    ar["maximumFormula"] = ""
    return ar


def flow_table_creation(data):
    """Add docstring."""
    ar = dict()
    flowtype = data["FlowType"].iloc[0]
    ar["flowType"] = flowtype
    ar["flowProperties"] = ""
    ar["name"] = data["FlowName"].iloc[0][
        0:255
    ]  # cutoff name at length 255 if greater than that
    ar["id"] = data["FlowUUID"].iloc[0]
    comp = str(data["Compartment"].iloc[0])
    if (flowtype == "ELEMENTARY_FLOW") & (comp != ""):
        if "emission" in comp or "resource" in comp:
            ar["category"] = (
                "Elementary Flows/"
                + comp
            )
        elif "input" in comp:
            ar["category"] = (
                "Elementary Flows/resource"
        )
        else:
            ar["category"] = (
                "Elementary Flows/"
                "emission/"
                + comp.lstrip("/")
            )
    elif (flowtype == "PRODUCT_FLOW") & (comp != ""):
        ar["category"] = comp
    elif flowtype == "WASTE_FLOW":
        ar["category"] = comp
    else:
        # Assume this is electricity or a byproduct
        ar[
            "category"
        ] = "Technosphere Flows/22: Utilities/2211: Electric Power Generation, Transmission and Distribution"
    return ar


def ref_exchange_creator(electricity_flow=electricity_at_grid_flow):
    """Add docstring."""
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
    """Add docstring."""
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = location(region)
    ar["parameters"] = ""
    ar["processDocumentation"] = process_doc_creation(process_type="consumption_mix")
    ar["processType"] = "UNIT_PROCESS"
    ar["name"] = consumption_mix_name + " - " + region
    ar[
        "category"
    ] = "22: Utilities/2211: Electric Power Generation, Transmission and Distribution"
    ar["description"] = (
        "Electricity consumption mix using power plants in the "
        + str(region)
        + " region"
    )
    return ar


def process_table_creation_surplus(region, exchanges_list):
    """Add docstring."""
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
    ar[
        "category"
    ] = "22: Utilities/2211: Electric Power Generation, Transmission and Distribution"
    ar["description"] = "Electricity surplus in the " + str(region) + " region"
    return ar


def process_table_creation_distribution(region, exchanges_list):
    """Add docstring."""
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
    ar[
        "category"
    ] = "22: Utilities/2211: Electric Power Generation, Transmission and Distribution"
    ar["description"] = (
        "Electricity distribution to end user in the "
        + str(region)
        + " region"
    )
    return ar

if __name__=="__main__":
    """
    Run for debugging purposes, to evaluate result of metadata from various models
    """
    p_docs = []
    for p in VALID_FUEL_CATS:
        p_docs.append(process_doc_creation(p))
    print("View p_docs in logger for debug1")
