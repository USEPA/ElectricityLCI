# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import data_dir, output_dir


def generate_canadian_mixes(us_inventory):
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
    print("Generating inventory for Canadian balancing authority areas")
    canadian_mix = pd.read_csv(f"{data_dir}/canadian_imports.csv")
    baa_codes = list(canadian_mix["Code"].unique())
    canadian_mix = canadian_mix.melt(
        id_vars=["Code", "Balancing Authority Name", "Province"],
        var_name="FuelCategory",
        value_name="FuelCategory_fraction",
    )
    us_inventory_summary = us_inventory.groupby(
        by=[
            "FuelCategory",
            #                    'Compartment',
            "FlowName",
            #                    'ElementaryFlowPrimeContext',
            "FlowUUID",
            "Unit",
            #                    'stage_code',
        ],
        as_index=False,
    )["Electricity", "FlowAmount", "quantity"].sum()
    flowuuid_compartment_df = us_inventory[
        ["FlowUUID", "Compartment"]
    ].drop_duplicates(subset=["FlowUUID"])
    flow_compartment_dict = dict(
        zip(
            flowuuid_compartment_df["FlowUUID"],
            flowuuid_compartment_df["Compartment"],
        )
    )
    us_inventory_summary["Compartment"] = us_inventory_summary["FlowUUID"].map(
        flow_compartment_dict
    )
    ca_mix_list = list()
    for baa_code in baa_codes:
        filter_crit = canadian_mix["Code"] == baa_code
        ca_inventory = pd.merge(
            left=us_inventory_summary,
            right=canadian_mix.loc[filter_crit, :],
            left_on="FuelCategory",
            right_on="FuelCategory",
            how="left",
        )
        ca_inventory.dropna(subset=["FuelCategory_fraction"], inplace=True)
        ca_mix_list.append(ca_inventory)
    blank_df = pd.DataFrame(columns=us_inventory.columns)
    ca_mix_inventory = pd.concat(ca_mix_list + [blank_df])

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
            "FlowName",
            "FlowUUID",
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
    return ca_mix_inventory


if __name__ == "__main__":
    pass
