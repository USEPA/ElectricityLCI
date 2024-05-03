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


##############################################################################
# FUNCTIONS
##############################################################################
def _unit(unt):
    ar = dict()
    ar["internalId"] = ""
    ar["@type"] = "Unit"
    ar["name"] = unt
    return ar


def _process_table_creation_gen(process_name, exchanges_list, fuel_type):
    fuel_category_dict = {
        "COAL": "21: Mining, Quarrying, and Oil and Gas Extraction/2121: Coal Mining",
        "GAS": "22: Utilities/2212: Natural Gas Distribution",
        "OIL": "31-33: Manufacturing/3241: Petroleum and Coal Products Manufacturing",
        "NUCLEAR": "31-33: Manufacturing/3251: Basic Chemical Manufacturing",
        "CONSTRUCTION":"23: Construction/2371: Utility System Construction",
    }
    ar = dict()
    ar["@type"] = "Process"
    ar["allocationFactors"] = ""
    ar["defaultAllocationMethod"] = ""
    ar["exchanges"] = exchanges_list
    ar["location"] = ""  # location(region)
    ar["parameters"] = ""
    logging.info(f"passing {fuel_type.lower()}_upstream to process_doc_creation")
    ar['processDocumentation']=process_doc_creation(process_type=f"{fuel_type.lower()}_upstream")
    ar["processType"] = "LCI_RESULT"
    ar["name"] = process_name
    if fuel_type == "coal_transport":
        ar["category"] = fuel_category_dict["COAL"]
    else:
        ar["category"] = fuel_category_dict[fuel_type]
    ar["description"] = process_description_creation(f"{fuel_type.lower()}_upstream")
    ar["version"]=make_valid_version_num(elci_version)
    return ar


def _exchange_table_creation_ref(fuel_type):
    natural_gas_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "natural gas, through transmission",
        "id": "",
        "category": "Technosphere Flows/22: Utilities/2212: Natural Gas Distribution",
    }

    coal_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "coal, processed, at mine",
        "id": "",
        "category": "Technosphere Flows/21: Mining, Quarrying, and Oil and Gas Extraction/2121: Coal Mining",
    }

    petroleum_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "petroleum fuel, through transportation",
        "id": "",
        "category": "Technosphere Flows/31-33: Manufacturing/3241: Petroleum and Coal Products Manufacturing",
    }

    transport_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "coal, transported",
        "id": "",
        "category": "Technosphere Flows/21: Mining, Quarrying, and Oil and Gas Extraction/2121: Coal Mining",
    }
    nuclear_flow = {
        "flowType": "PRODUCT_FLOW",
        "flowProperties": "",
        "name": "nuclear fuel, through transportation",
        "id": "",
        "category": "Technosphere Flows/31-33: Manufacturing/3251: Basic Chemical Manufacturing",
    }
    construction_flow ={
            "flowType":"PRODUCT_FLOW",
            "flowProperties":"",
            "name":"power plant construction",
            "id":"",
            "category":"Technosphere Flows/23: Construction/2371: Utility System Construction"
            }
    # the following link provides the undefined variable: https://github.com/KeyLogicLCA/ElectricityLCI/commit/f61d28a3d0cf5b0ef61ca147f870e15a863f8ec3
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
        ar["flow"] = geothermal_flow
        ar["unit"] = _unit("MWh")
        ar["amount"] = 1
    elif fuel_type == "SOLAR":
        ar["flow"] = solar_flow
        ar["unit"] = _unit("Item(s)")
        ar["amount"] = 1
    elif fuel_type == "WIND":
        ar["flow"] = wind_flow
        ar["unit"] = _unit("Item(s)")
        ar["amount"] = 1
    elif fuel_type == "CONSTRUCTION":
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
    ar = dict()
    if "emission" in data["Compartment"] or "resource" in data["Compartment"]:
        ar["flowType"] = "ELEMENTARY_FLOW"
    elif "technosphere" in data["Compartment"].lower() or "valuable" in data["Compartment"].lower():
        ar["flowType"] = "PRODUCT_FLOW"
    elif "waste" in data["Compartment"].lower():
        ar["flowType"] = "WASTE_FLOW"
    else:
        ar["flowType"] = "ELEMENTARY_FLOW"
    ar["flowProperties"] = ""
    ar["name"] = data["FlowName"][
        0:255
    ]  # cutoff name at length 255 if greater than that
    ar["id"] = data["FlowUUID"]
    comp = str(data["Compartment"])
    if (ar["flowType"] == "ELEMENTARY_FLOW") & (comp != ""):
        if "emission" in comp or "resource" in comp:
            ar["category"]="Elementary flows/"+comp
        else:
            ar["category"] = "Elementary flows/" + "emission" + "/" + comp
    elif (ar["flowType"] == "PRODUCT_FLOW") & (comp != ""):
        ar["category"] = comp
    elif ar["flowType"] == "WASTE_FLOW":
        ar["category"] = "Waste flows/"
    else:
        ar[
            "category"
        ] = "22: Utilities/2211: Electric Power Generation, Transmission and Distribution"
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
    """
    Generate olca-schema dictionaries.

    For upstream processes for the inventory provided in the given dataframe.

    Parameters
    ----------
    merged: dataframe
        Dataframe containing the inventory for upstream processes used by
        eletricity generation.

    Returns
    ----------
    dictionary
        Dictionary containing all of the unit processes to be written to
        JSON-LD for import to openLCA
    """

    coal_type_codes_inv = dict(map(reversed, coal_type_codes.items()))
    mine_type_codes_inv = dict(map(reversed, mine_type_codes.items()))
    basin_codes_inv = dict(map(reversed, basin_codes.items()))
    coal_transport = [
        "Barge",
        "Lake Vessel",
        "Ocean Vessel",
        "Railroad",
        "Truck",
    ]
    # First going to keep plant IDs to account for possible emission repeats
    # for the same compartment, leading to erroneously low emission factors
    merged_summary = merged.groupby(
        [
            "FuelCategory",
            "stage_code",
            "FlowName",
            "FlowUUID",
            "Compartment",
            "plant_id",
            "Unit",
            "input"
        ],
        as_index=False,
    ).agg({"FlowAmount": "sum", "quantity": "mean"})
    merged_summary = merged_summary.groupby(
        ["FuelCategory", "stage_code", "FlowName", "FlowUUID", "Compartment","Unit","input"],
        as_index=False,
    )[["quantity", "FlowAmount"]].sum()

    # For natural gas extraction there are extraction and transportation stages
    # that will get lumped together in the groupby which will double
    # the quantity and erroneously lower emission rates.
    merged_summary["emission_factor"] = (
        merged_summary["FlowAmount"] / merged_summary["quantity"]
    )
    merged_summary.dropna(subset=["emission_factor"],inplace=True)
    upstream_list = list(
        x
        for x in merged_summary["stage_code"].unique()
    )

    upstream_process_dict = dict()
    for upstream in upstream_list:
        logging.info(f"Building dictionary for {upstream}")
        exchanges_list = list()
        upstream_filter = merged_summary["stage_code"] == upstream
        merged_summary_filter = merged_summary.loc[upstream_filter, :].copy()
        merged_summary_filter.drop_duplicates(
            subset=["FlowName", "Compartment", "FlowAmount"], inplace=True
        )
        merged_summary_filter.dropna(subset=["FlowName"], inplace=True)
        garbage = merged_summary_filter.loc[
            merged_summary_filter["FlowName"] == "[no match]", :
        ].index
        merged_summary_filter.drop(garbage, inplace=True)
        ra = merged_summary_filter.apply(
            _exchange_table_creation_output, axis=1
        ).tolist()
        exchanges_list.extend(ra)
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
        elif fuel_type == "CONSTRUCTION":
            combined_name= f"power plant construction - {stage_code}"
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        process_name = f"{combined_name}"
        if (fuel_type == "COAL") & (stage_code in coal_transport):
            final=_process_table_creation_gen(
                    process_name, exchanges_list, "coal_transport"
            )
        else:
            final = _process_table_creation_gen(
                    process_name, exchanges_list, fuel_type
            )
        upstream_process_dict[
            merged_summary_filter.loc[first_row, "stage_code"]
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
