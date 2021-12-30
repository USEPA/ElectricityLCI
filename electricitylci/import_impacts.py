# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import data_dir, output_dir
from electricitylci.model_config import model_specs


def generate_canadian_mixes(us_inventory):
    from electricitylci.combinator import ba_codes

    """
    Uses aggregate U.S.-level inventory to fuel category. These U.S. fuel
    category inventories are then used as proxies for Canadian generation. The
    inventories are weighted by the percent of that type of generation for the
    Canadian balancing authority area (e.g., if the BC Hydro & Power Authority
    generation mix includes 9% biomass, then U.S.-level biomass emissions are
    multiplied by 0.09. The result is a dataframe that includes balancing
    authority level inventories for Canadian imports.

    Parameters
    ----------
    us_inventory: dataframe
        A dataframe containing flow-level inventory for all fuel categories in
        the United States.

    Returns
    ----------
    dataframe
    """

    canadian_egrid_ids = {
        "BCHA": 9999991,
        "HQT": 9999992,
        "IESO": 9999993,
        "MHEB": 9999994,
        "NBSO": 9999995,
        "NEWL": 9999996,
    }
    input_map = {
        "air": False,
        "water": False,
        "input": True,
        "waste": False,
        "output": False,
        "ground": False,
    }

    canada_subregion_map = {
        "British Columbia": "BCHA",
        "Quebec": "HQT",
        "Ontario": "IESO",
        "Manitoba": "MHEB",
        "New Brunswick": "NBSO",
        "Newfoundland and Labrador": "NEWL",
    }
    print("Generating inventory for Canadian balancing authority areas")
    import_mix = pd.read_csv(f"{data_dir}/International_Electricity_Mix.csv")
    import_mix = import_mix.loc[
        import_mix["Year"] == model_specs.eia_gen_year, :
    ]
    canadian_mix = import_mix
    canadian_mix["Code"] = canadian_mix["Subregion"].map(canada_subregion_map)
    baa_codes = list(canadian_mix["Code"].unique())
    canadian_mix["Balancing Authority Name"] = canadian_mix["Code"].map(
        ba_codes["BA_Name"]
    )
    canadian_mix = canadian_mix.rename(
        columns={
            "Subregion": "Province",
            "Generation_Ratio": "FuelCategory_fraction",
        },
        errors="ignore",
    )
    canadian_mix=canadian_mix.drop(columns=["Electricity"],errors="ignore")
    canadian_mix=canadian_mix.dropna(subset=["Code"])
    if "input" in us_inventory.columns:
        us_inventory.loc[us_inventory["input"].isna(), "input"] = us_inventory[
            "Compartment"
        ].map(input_map)
    else:
        us_inventory["input"] = us_inventory["Compartment"].map(input_map)

    # If there are no upstream inventories, then the quantity column will not exist.
    # Let's create it.
    if "quantity" not in list(us_inventory.columns):
        us_inventory["quantity"] = float("nan")
    us_inventory_summary = us_inventory.groupby(
        by=[
            "FuelCategory",
            #                    'Compartment',
            "FlowName",
            #                    'ElementaryFlowPrimeContext',
            "FlowUUID",
            #                    'stage_code',
        ]
    )["FlowAmount", "quantity"].sum()
    us_inventory_electricity = (
        us_inventory.drop_duplicates(
            subset=[
                "FuelCategory",
                "FlowName",
                "FlowUUID",
                "Unit",
                "Electricity",
            ]
        )
        .groupby(
            by=[
                "FuelCategory",
                #                    'Compartment',
                "FlowName",
                #                    'ElementaryFlowPrimeContext',
                "FlowUUID",
                #                    'stage_code',
            ]
        )["Electricity"]
        .sum()
    )
    us_inventory_summary = pd.concat(
        [us_inventory_summary, us_inventory_electricity], axis=1
    )
    us_inventory_summary = us_inventory_summary.reset_index()
    flowuuid_compartment_df = (
        us_inventory[
            ["FlowUUID", "Compartment", "input", "Compartment_path", "Unit"]
        ]
        .drop_duplicates(subset=["FlowUUID"])
        .set_index("FlowUUID")
    )
    us_inventory_summary["Compartment"] = us_inventory_summary["FlowUUID"].map(
        flowuuid_compartment_df["Compartment"]
    )
    us_inventory_summary["input"] = us_inventory_summary["FlowUUID"].map(
        flowuuid_compartment_df["input"]
    )
    us_inventory_summary["Unit"] = us_inventory_summary["FlowUUID"].map(
        flowuuid_compartment_df["Unit"]
    )
    us_inventory_summary["Compartment_path"] = us_inventory_summary[
        "FlowUUID"
    ].map(flowuuid_compartment_df["Compartment_path"])
    ca_mix_list = list()
    for baa_code in baa_codes:
        filter_crit = canadian_mix["Code"] == baa_code
        ca_inventory = pd.merge(
            left=us_inventory_summary,
            right=canadian_mix.loc[filter_crit, :],
            left_on="FuelCategory",
            right_on="FuelCategory",
            how="left",
            suffixes=["","ca_elec"]
        )
        ca_inventory.dropna(subset=["FuelCategory_fraction"], inplace=True)
        ca_mix_list.append(ca_inventory)
    blank_df = pd.DataFrame(columns=us_inventory.columns)
    ca_mix_inventory = pd.concat(ca_mix_list + [blank_df], ignore_index=True)

    ca_mix_inventory[
        ["Electricity", "FlowAmount", "quantity"]
    ] = ca_mix_inventory[["Electricity", "FlowAmount", "quantity"]].multiply(
        ca_mix_inventory["FuelCategory_fraction"], axis="index"
    )
    ca_mix_inventory.drop(
        columns=["Province", "FuelCategory_fraction"], inplace=True
    )
    ca_mix_inventory = ca_mix_inventory.groupby(
        by=[
            "Code",
            "Balancing Authority Name",
            "Compartment",
            "Compartment_path",
            "FlowName",
            "FlowUUID",
            "input",
            "Unit",
        ],
        as_index=False,
    )[["Electricity", "FlowAmount", "quantity"]].sum()
    ca_mix_inventory["stage_code"] = "life cycle"
    ca_mix_inventory.sort_values(
        by=["Code", "Compartment", "FlowName", "stage_code"], inplace=True
    )
    ca_mix_inventory.rename(
        columns={"Code": "Balancing Authority Code"}, inplace=True
    )
    ca_mix_inventory["Source"] = "netl"
    ca_mix_inventory["Year"] = us_inventory["Year"].mode().to_numpy()[0]
    ca_mix_inventory["FuelCategory"] = "ALL"
    ca_mix_inventory["eGRID_ID"] = ca_mix_inventory[
        "Balancing Authority Code"
    ].map(canadian_egrid_ids)
    ca_mix_inventory["FERC_Region"] = ca_mix_inventory[
        "Balancing Authority Code"
    ].map(ba_codes["FERC_Region"])
    ca_mix_inventory["EIA_Region"] = ca_mix_inventory[
        "Balancing Authority Code"
    ].map(ba_codes["EIA_Region"])
    return ca_mix_inventory


if __name__ == "__main__":
    pass
