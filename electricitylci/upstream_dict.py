#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# upstream_dict.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

from electricitylci.coal_upstream import (
    coal_type_codes,
    mine_type_codes,
    basin_codes,
)
from electricitylci import write_process_dicts_to_jsonld
from electricitylci.process_dictionary_writer import (
        process_doc_creation,
        process_description_creation
)
from electricitylci.utils import make_valid_version_num
from electricitylci.globals import elci_version
# Issue #150, need Balancing Authority names for regional construction
from electricitylci.eia860_facilities import eia860_balancing_authority
import electricitylci.model_config as config


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """
This module contains the relevant methods for generating openLCA-compliant
dictionaries for upstream process inventories, such as coal/natural gas/
petroleum extraction and processing, coal transport, nuclear fuel extraction,
processing, and transport, and power plant construction.

Last updated:
    2025-02-06
"""
__all__ = [
    "olcaschema_genupstream_processes",
]


##############################################################################
# FUNCTIONS
##############################################################################
def _unit(unt):
    """Create a unit dictionary in olca-schema format.

    See `online <https://greendelta.github.io/olca-schema/classes/Unit.html>`_

    Parameters
    ----------
    unt : str
        Unit name

    Returns
    -------
    dict
        openLCA-schema formatted dictionary for Unit.
    """
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Unit"
    ar["name"] = unt
    return ar


def _process_table_creation_gen(process_name, exchanges_list, fuel_type):
    """Generate an openlca-schema formatted dictionary for a Process.

    See
    `online <https://greendelta.github.io/olca-schema/classes/Process.html>`_.

    Parameters
    ----------
    process_name : str
        Process name.
    exchanges_list : list
        List of exchange dictionaries.
    fuel_type : str
        Fuel type

    Returns
    -------
    dict
        A dictionary for an openLCA schema Process.
    """
    # Standard categories for openLCA processes by technology (by NAICS code).
    fuel_category_dict = {
        "COAL": (
            "21: Mining, Quarrying, and Oil and Gas Extraction/"
            "2121: Coal Mining"),
        "GAS": (
            "22: Utilities/"
            "2212: Natural Gas Distribution"),
        "OIL": (
            "31-33: Manufacturing/"
            "3241: Petroleum and Coal Products Manufacturing"),
        "NUCLEAR": (
            "31-33: Manufacturing/"
            "3251: Basic Chemical Manufacturing"),
        "CONSTRUCTION": (
            "23: Construction/"
            "2371: Utility System Construction"),
            #New Issue #150
        "WIND_CONSTRUCTION": (
            "23: Construction/"
            "2371: Utility System Construction"),
        "SOLARPV_CONSTRUCTION": (
            "23: Construction/"
            "2371: Utility System Construction"),
        "SOLARTHERM_CONSTRUCTION": (
            "23: Construction/"
            "2371: Utility System Construction"),
    }

    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = ""  # location(region)
    ar["parameters"] = ""

    logging.debug(
        f"passing {fuel_type.lower()}_upstream to process_doc_creation")
    ar['processDocumentation'] = process_doc_creation(
        process_type=f"{fuel_type.lower()}_upstream")
    ar["processType"] = "LCI_RESULT"
    ar["name"] = process_name
    if fuel_type == "coal_transport":
        ar["category"] = fuel_category_dict["COAL"]
    else:
        ar["category"] = fuel_category_dict[fuel_type]

    # TODO: here is where renewable construction process documentation
    # is handled; however, the fixes for it are found in generation.py;
    # here construction_upstream is used to make the default fossil/ngcc
    # construction documentation as found in process_dictionary_writer.py;
    # potentially add alternative route in process_description_creation
    # (e.g., pull construction type from process name).
    ar["description"] = process_description_creation(
        f"{fuel_type.lower()}_upstream")
    ar["version"] = make_valid_version_num(elci_version)

    return ar


def _exchange_table_creation_ref(fuel_type):
    natural_gas_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "natural gas, through transmission",
        "id": "",
        "category": (
            "Technosphere Flows/"
            "22: Utilities/"
            "2212: Natural Gas Distribution"),
    }
    coal_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "coal, processed, at mine",
        "id": "",
        "category": (
            "Technosphere Flows/"
            "21: Mining, Quarrying, and Oil and Gas Extraction/"
            "2121: Coal Mining"),
    }
    petroleum_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "petroleum fuel, through transportation",
        "id": "",
        "category": (
            "Technosphere Flows/"
            "31-33: Manufacturing/"
            "3241: Petroleum and Coal Products Manufacturing"),
    }
    transport_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "coal, transported",
        "id": "",
        "category": (
            "Technosphere Flows/"
            "21: Mining, Quarrying, and Oil and Gas Extraction/"
            "2121: Coal Mining"),
    }
    nuclear_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "nuclear fuel, through transportation",
        "id": "",
        "category": (
            "Technosphere Flows/"
            "31-33: Manufacturing/"
            "3251: Basic Chemical Manufacturing"),
    }
    construction_flow = {
        "flowType":"PRODUCT_FLOW",
        "flowProperties":"",
        "name":"power plant construction",
        "id":"",
        "category": (
            "Technosphere Flows/"
            "23: Construction/"
            "2371: Utility System Construction")
    }

    # The following link provides the undefined variables:
    # https://github.com/KeyLogicLCA/ElectricityLCI/commit/f61d28a3d0cf5b0ef61ca147f870e15a863f8ec3
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    if fuel_type == "COAL":
        ar["flow"] = coal_flow
        ar["unit"] = _unit("sh tn")
        ar["amount"] = 1.0
    elif fuel_type == "GAS":
        ar["flow"] = natural_gas_flow
        ar["unit"] = _unit("MJ")
        ar["amount"] = 1
    elif fuel_type == "OIL":
        ar["flow"] = petroleum_flow
        ar["unit"] = _unit("MJ")
        ar["amount"] = 1
    elif fuel_type == "Coal transport":
        ar["flow"] = transport_flow
        ar["unit"] = _unit("kg*km")
        ar["amount"] = 1
    elif fuel_type == "NUCLEAR":
        ar["flow"] = nuclear_flow
        ar["unit"] = _unit("MWh")
        ar["amount"] = 1
    elif fuel_type == "GEOTHERMAL":
        logging.warning("Undefined geothermal flow")
        ar["flow"] = geothermal_flow
        ar["unit"] = _unit("MWh")
        ar["amount"] = 1
    elif fuel_type == "SOLAR":
        logging.warning("Undefined solar flow")
        ar["flow"] = solar_flow
        ar["unit"] = _unit("Item(s)")
        ar["amount"] = 1
    elif fuel_type == "WIND":
        logging.warning("Undefined wind flow")
        ar["flow"] = wind_flow
        ar["unit"] = _unit("Item(s)")
        ar["amount"] = 1
    # issue #150, catching multiple construction types
    elif "CONSTRUCTION" in fuel_type:
        ar["flow"] = construction_flow
        ar["unit"] = _unit("Item(s)")
        ar["amount"] = 1
    ar["flowProperty"] = ""
    ar["input"] = False
    ar["quantitativeReference"] = True
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amountFormula"] = ""

    return ar


def _flow_table_creation(data):
    # HOTFIX iss267. v2: Add elementary flow prime context check.
    # HOTFIX iss267. v1: Construction flows do not have a FlowType assigned;
    # however, other flows do. Let's check them first.
    ar = dict()
    try:
        ar['flowType'] = data['FlowType']
    except KeyError:
        # No flow type index
        if "emission" in data["Compartment"] or "resource" in data["Compartment"]:
            ar["flowType"] = "ELEMENTARY_FLOW"
        elif "technosphere" in data["Compartment"].lower() or (
                "valuable" in data["Compartment"].lower()):
            ar["flowType"] = "PRODUCT_FLOW"
        elif data['ElementaryFlowPrimeContext'] == 'technosphere':
            # Iss267; cross-check prime context for product flows [241125; TWD]
            ar["flowType"] = "PRODUCT_FLOW"
        elif "waste" in data["Compartment"].lower():
            ar["flowType"] = "WASTE_FLOW"
        else:
            ar["flowType"] = "ELEMENTARY_FLOW"

    ar["flowProperties"] = ""
    ar["name"] = data["FlowName"][0:255] # Cutoff flow name at length 255
    ar["id"] = data["FlowUUID"]

    comp = str(data["Compartment"])
    if (ar["flowType"] == "ELEMENTARY_FLOW") & (comp != ""):
        if "emission" in comp or "resource" in comp:
            ar["category"]="Elementary flows/"+comp
        else:
            ar["category"] = "Elementary flows/" + "emission" + "/" + comp
    elif (ar["flowType"] == "PRODUCT_FLOW") & (comp != ""):
        # HOTFIX: put technosphere flows in their own sub-category [250206;TWD]
        if not comp.startswith("Technosphere Flows/"):
            comp = "Technosphere Flows/" + comp
        ar["category"] = comp
    elif ar["flowType"] == "WASTE_FLOW":
        ar["category"] = "Waste flows/"
    else:
        ar["category"] = (
            "22: Utilities/"
            "2211: Electric Power Generation, Transmission and Distribution")

    return ar


def _exchange_table_creation_output(data):
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Exchange"
    ar["avoidedProduct"] = False
    ar["flow"] = _flow_table_creation(data)
    ar["flowProperty"] = ""
    ar["input"] = data["input"]
    ar["quantitativeReference"] = False
    ar["baseUncertainty"] = ""
    ar["provider"] = ""
    ar["amount"] = data["emission_factor"]
    ar["amountFormula"] = ""
    ar["unit"] = _unit(data["Unit"])
    ar["pedigreeUncertainty"] = ""
    if type(ar) == "DataFrame":
        print(data)
    return ar


def olcaschema_genupstream_processes(merged):
    """Generate olca-schema dictionaries.

    For upstream processes for the inventory provided in the given data frame.

    Parameters
    ----------
    merged: pandas.DataFrame
        Data frame containing the inventory for upstream processes used by
        electricity generation.

    Returns
    ----------
    dict
        Dictionary containing all of the unit processes to be written to
        JSON-LD for import to openLCA.
    """
    coal_type_codes_inv = dict(map(reversed, coal_type_codes.items()))
    mine_type_codes_inv = dict(map(reversed, mine_type_codes.items()))
    basin_codes_inv = dict(map(reversed, basin_codes.items()))
    # Hotfix: add 'Belt' to coal transport list [12/16/2024;MBJ]
    # NOTE: I don't think the belt inventory is actually used anywhere
    coal_transport = [
        "Barge",
        "Lake Vessel",
        "Ocean Vessel",
        "Railroad",
        "Truck",
        "Belt",
    ]
    # First going to keep plant IDs to account for possible emission repeats
    # for the same compartment, leading to erroneously low emission factors
    merged_summary = merged.groupby(
        by=[
            "FuelCategory",
            "stage_code",
            "FlowName",
            "FlowUUID",
            "Compartment",
            "ElementaryFlowPrimeContext",
            "plant_id",
            "Unit",
            "input"
        ],
        as_index=False,
    ).agg({"FlowAmount": "sum", "quantity": "mean"})
    # NEW Issue #150, adding regional ability for construction of all types
    plant_region = eia860_balancing_authority(config.model_specs.eia_gen_year)
    plant_region["Plant Id"] = plant_region["Plant Id"].astype(int)
    merged_summary_regional = (
        merged_summary.loc[
            merged_summary["FuelCategory"].str.contains("CONSTRUCTION"), :
        ]
        .merge(
            plant_region, how="left", left_on="plant_id", right_on="Plant Id"
        )
        .copy().reset_index()
    )
    merged_summary = merged_summary.groupby(
        by=[
            "FuelCategory",
            "stage_code",
            "FlowName",
            "FlowUUID",
            "Compartment",
            "ElementaryFlowPrimeContext",
            "Unit",
            "input",
        ],
        as_index=False,
    )[["quantity", "FlowAmount"]].sum()

    # Issue #150, adding regional ability for construction of all types
    # NOTE: Added "Balancing Authority Name" below to enable regionalized
    # construction processes. Only for balancing authorities now but should
    # be expanded to eGRID, etc.
    merged_summary_regional = merged_summary_regional.groupby(
        by=[
            "FuelCategory",
            "stage_code",
            "FlowName",
            "FlowUUID",
            "Compartment",
            "ElementaryFlowPrimeContext",
            "Unit",
            "input",
            "Balancing Authority Name",
        ],
        as_index=False,
    )[["quantity", "FlowAmount"]].sum()
    # For natural gas extraction there are extraction and transportation stages
    # that will get lumped together in the groupby which will double
    # the quantity and erroneously lower emission rates.

    # Calculate emission factor (e.g., total emission per total MWh)
    merged_summary["emission_factor"] = (
        merged_summary["FlowAmount"] / merged_summary["quantity"]
    )
    merged_summary.dropna(subset=["emission_factor"], inplace=True)
    # NEW Issue #150, adding regional ability for construction of all types
    merged_summary_regional["emission_factor"] = (
        merged_summary_regional["FlowAmount"] / merged_summary_regional["quantity"]
    )
    merged_summary_regional.dropna(subset=["emission_factor"], inplace=True)
    # Make upstream processes for each stage code and save to a dictionary.
    upstream_process_dict = dict()
    upstream_list = [x for x in merged_summary["stage_code"].unique()]
    for upstream in upstream_list:
        logging.info(f"Building dictionary for {upstream}")
        exchanges_list = list()

        upstream_filter = merged_summary["stage_code"] == upstream
        merged_summary_filter = merged_summary.loc[upstream_filter, :].copy()
        merged_summary_filter.drop_duplicates(
            subset=["FlowName", "Compartment", "FlowAmount"],
            inplace=True
        )
        merged_summary_filter.dropna(subset=["FlowName"], inplace=True)

        # TODO: where does "[no match]" get set? FEDEFL mapper?
        garbage = merged_summary_filter.loc[
            merged_summary_filter["FlowName"] == "[no match]", :].index
        merged_summary_filter.drop(garbage, inplace=True)

        ra = merged_summary_filter.apply(
            _exchange_table_creation_output, axis=1).tolist()
        exchanges_list.extend(ra)

        # TODO
        # Iss253. The fuel type is 'Construction' and stage code is one of
        # 'solar_pv_const', 'solar_thermal_const', or 'wind_const'
        # Paths forward could be to make unique calls for
        first_row = min(merged_summary_filter.index)
        fuel_type = merged_summary_filter.loc[first_row, "FuelCategory"]
        stage_code = merged_summary_filter.loc[first_row, "stage_code"]

        if (fuel_type == "COAL") & (stage_code not in coal_transport):
            split_name = merged_summary_filter.loc[
                first_row, "stage_code"
            ].split("-")
            combined_name = (
                "coal extraction and processing - "
                + basin_codes_inv[split_name[0]]
                + ", "
                + coal_type_codes_inv[split_name[1]]
                + ", "
                + mine_type_codes_inv[split_name[2]]
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif (fuel_type == "COAL") & (stage_code in coal_transport):
            combined_name = "coal transport - " + stage_code
            exchanges_list.append(
                _exchange_table_creation_ref("Coal transport")
            )
        elif fuel_type == "GAS":
            combined_name = (
                "natural gas extraction and processing - "
                + merged_summary_filter.loc[first_row, "stage_code"]
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type == "OIL":
            split_name = merged_summary_filter.loc[
                first_row, "stage_code"
            ].split("_")
            combined_name = (
                "petroleum extraction and processing - " + split_name[0] + " "
                f"PADD {split_name[1]}"
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type == "NUCLEAR":
            combined_name = (
                "nuclear fuel extraction, processing, and transport"
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type == "GEOTHERMAL":
            combined_name = f"geothermal upstream and operation - {stage_code}"
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type == "SOLAR":
            combined_name = f"solar photovoltaic upstream and operation - {stage_code}"
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type == "WIND":
            combined_name = f"wind upstream and operation - {stage_code}"
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        #Issue #150, catching multiple types of CONSTRUCTION. Worth noting that
        #US average is appended to these in case they're needed (unlikely)
        elif "CONSTRUCTION" in fuel_type:
            combined_name= f"power plant construction - {stage_code} - US Average"
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))

        process_name = f"{combined_name}"
        if (fuel_type == "COAL") & (stage_code in coal_transport):
            final = _process_table_creation_gen(
                process_name, exchanges_list, "coal_transport"
            )
        else:
            final = _process_table_creation_gen(
                process_name, exchanges_list, fuel_type
            )
        upstream_process_dict[
            merged_summary_filter.loc[first_row, "stage_code"]
        ] = final
    # New, Issue #150 - adding regional construction profiles.
    merged_summary_regional["scen_name"] = (
        merged_summary_regional["stage_code"]
        + " - "
        + merged_summary_regional["Balancing Authority Name"]
    )
    upstream_regional_list = [x for x in merged_summary_regional["scen_name"].unique()]
    for upstream in upstream_regional_list:
        logging.info(f"Building dictionary for {upstream}")
        exchanges_list = list()

        upstream_filter = merged_summary_regional["scen_name"] == upstream
        merged_summary_filter = merged_summary_regional.loc[upstream_filter, :].copy()
        merged_summary_filter.drop_duplicates(
            subset=["FlowName", "Compartment", "FlowAmount"],
            inplace=True
        )
        merged_summary_filter.dropna(subset=["FlowName"], inplace=True)

        # TODO: where does "[no match]" get set? FEDEFL mapper?
        garbage = merged_summary_filter.loc[
            merged_summary_filter["FlowName"] == "[no match]", :].index
        merged_summary_filter.drop(garbage, inplace=True)

        ra = merged_summary_filter.apply(
            _exchange_table_creation_output, axis=1).tolist()
        exchanges_list.extend(ra)

        first_row = min(merged_summary_filter.index)
        fuel_type = merged_summary_filter.loc[first_row, "FuelCategory"]
        stage_code = merged_summary_filter.loc[first_row, "stage_code"]
        if "CONSTRUCTION" in fuel_type:
            combined_name= f"power plant construction - {upstream}"
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))

        process_name = f"{combined_name}"
        final = _process_table_creation_gen(
                process_name, exchanges_list, fuel_type
            )
        upstream_process_dict[
            merged_summary_filter.loc[first_row, "scen_name"]
        ] = final
    return upstream_process_dict


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    import electricitylci.coal_upstream as coal
    import electricitylci.natural_gas_upstream as ng
    import electricitylci.petroleum_upstream as petro
    import electricitylci.nuclear_upstream as nuke
    from combinator import concat_map_upstream_databases
    from electricitylci.globals import output_dir

    year = 2016
    coal_df = coal.generate_upstream_coal(year)
    ng_df = ng.generate_upstream_ng(year)
    petro_df = petro.generate_petroleum_upstream(year)
    nuke_df = nuke.generate_upstream_nuc(year)
    merged = concat_map_upstream_databases(
        coal_df, ng_df, petro_df, nuke_df
    )
    merged.to_csv(f"{output_dir}/total_upstream_{year}.csv")
    upstream_process_dict = olcaschema_genupstream_processes(merged)
    upstream_olca_processes = write_process_dicts_to_jsonld(upstream_process_dict)
