#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Add docstring."""

import pandas as pd
from electricitylci.globals import output_dir, data_dir
import electricitylci.PhysicalQuantities as pq
from electricitylci.eia923_generation import eia923_download_extract


def generate_plant_water_use(year):
    """
    Uses data from an NETL.
    
    Water use analysis to generate water withdrawal and discharge flows for
    power plants. The underlying data represents an analysis of water use in
    2016. The water intensities are used to generate annual amounts of water use
    using generation data from the given year.

    Parameters
    ----------
    year : int
        Year of EIA-923 generation data to use

    Returns
    -------
    dataframe:
        Dataframe ready to be appended to the generation database. Includes
        federal elementary flow name and uuids for the different water flows.
    """
    DATA_FILE = "NETL-EIA_powerplants_water_withdraw_consume_data_2016.csv"
    # For better or worse the flow names and UUIDs are hard-coded based
    # on the federal elementary flows list release - 0.2
    WATER_FLOW_RESOURCE_DICT = {
        "Fresh": {
            "FlowName": "Water, fresh",
            "FlowUUID": "8ba7dd57-b502-397b-944a-f63c6615f754",
        },
        "Reclaimed": {"FlowName": "Water, reclaimed", "FlowUUID": ""},
        "Saline": {
            "FlowName": "Water, saline",
            "FlowUUID": "67297f72-511b-3e86-b7e4-676437a75dbf",
        },
        "Brackish": {
            "FlowName": "Water, brackish",
            "FlowUUID": "a95bae36-325b-3da7-8a47-00ba98b61cfe",
        },
        "Other": {
            "FlowName": "Water",
            "FlowUUID": "e2eb491c-78ff-3123-9e42-494c7d199b44",
        },
    }

    WATER_FLOW_EMISSION_DICT = {
        "Fresh": {
            "FlowName": "Water, fresh",
            "FlowUUID": "a1c1753e-5b37-3aa2-a929-552eb0d6d351",
        },
        "Reclaimed": {"FlowName": "Water, reclaimed", "FlowUUID": ""},
        "Saline": {
            "FlowName": "Water, saline",
            "FlowUUID": "55222571-f94b-36fd-b74a-4e32219dfe2f",
        },
        "Brackish": {
            "FlowName": "Water, brackish",
            "FlowUUID": "f96e52c2-2c7e-35c0-8c7d-91877eb37725",
        },
        "Other": {
            "FlowName": "Water",
            "FlowUUID": "96054103-8509-3049-a638-c4d13cc707cc",
        },
    }

    WATER_FLOW_COMPARTMENTPATH = {
        "withdrawal_annual": "resource/water",
        "discharge_annual": "emission/water",
    }
    WATER_FLOW_COMPARTMENT={
        "withdrawal_annual": "input",
        "discharge_annual": "water",
    }
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data["Plant Id"] = eia_generation_data["Plant Id"].astype(
        int
    )
    water_df = pd.read_csv(
        f"{data_dir}/{DATA_FILE}", index_col=0, low_memory=False
    )
    water_df["annual_withdrawal"] = (
        water_df["Water Withdrawal Intensity Adjusted (gal/MWh)"]
        * water_df["Total net generation (MWh)"]
        * pq.convert(1, "galUS", "l")
    )
    water_df["annual_discharge"] = water_df["annual_withdrawal"] - (
        water_df["Water Consumption Intensity Adjusted (gal/MWh)"]
        * water_df["Total net generation (MWh)"]
        * pq.convert(1, "galUS", "l")
    )
    # The water use analysis is monthly at the boiler level
    water_df_group = water_df.groupby(
        by=[
            "Plant Code",
            "Water Type",
            "Water Source Name",
            "Water Discharge Name",
        ],
        as_index=False,
    )[
        "annual_withdrawal", "annual_discharge", "Total net generation (MWh)"
    ].sum()
    water_df_group.rename(
        columns={
            "Total net generation (MWh)": "net_generation_water",
            "Plant Code": "Plant Id",
        },
        inplace=True,
    )
    water_df_group["Plant Id"] = water_df_group["Plant Id"].astype(int)

    # While the analysis includes water intensities, these intensities are
    # at the boiler level, on a monthly basis, so we calculate new ones based
    # on the annual withdrawal and electricity generated.
    water_df_group["withdrawal_intensity"] = (
        water_df_group["annual_withdrawal"]
        / water_df_group["net_generation_water"]
    )
    water_df_group["discharge_intensity"] = (
        water_df_group["annual_discharge"]
        / water_df_group["net_generation_water"]
    )
    eia_gen_group = eia_generation_data.groupby(
        by=["Plant Id"], as_index=False
    )["Net Generation (Megawatthours)"].sum()
    eia_gen_group.rename(
        columns={"Net Generation (Megawatthours)": "net_generation_eia"},
        inplace=True,
    )
    merged_water = pd.merge(
        left=eia_gen_group, right=water_df_group, on="Plant Id", how="left"
    )
    merged_water.dropna(inplace=True)
    merged_water["total_plant_gen_water"] = merged_water.groupby(
        by=["Plant Id"]
    )["net_generation_water"].transform("sum")
    merged_water["fraction_gen"] = (
        merged_water["net_generation_water"]
        / merged_water["total_plant_gen_water"]
    )
    merged_water["alloc_net_generation_eia"] = (
        merged_water["net_generation_eia"] * merged_water["fraction_gen"]
    )
    clean_merged_water = merged_water[
        [
            "Plant Id",
            "Water Type",
            "withdrawal_intensity",
            "discharge_intensity",
            "alloc_net_generation_eia",
        ]
    ].copy()
    clean_merged_water.loc[:, "withdrawal_annual"] = (
        clean_merged_water["withdrawal_intensity"]
        * clean_merged_water["alloc_net_generation_eia"]
    )
    clean_merged_water.loc[:, "discharge_annual"] = (
        clean_merged_water["discharge_intensity"]
        * clean_merged_water["alloc_net_generation_eia"]
    )
    clean_merged_water.drop(
        columns=["withdrawal_intensity", "discharge_intensity"], inplace=True
    )
    final_water = clean_merged_water.melt(
        id_vars=["Plant Id", "Water Type", "alloc_net_generation_eia"],
        var_name="direction",
    )
    final_water.sort_values(by=["Plant Id", "Water Type"], inplace=True)
    final_water.reset_index(inplace=True, drop=True)
    final_water["Compartment_path"] = final_water["direction"].map(
        WATER_FLOW_COMPARTMENTPATH
    )
    final_water["Compartment"] = final_water["direction"].map(WATER_FLOW_COMPARTMENT)
    resource_filter = final_water["Compartment_path"] == "resource/water"
    final_water.loc[resource_filter, "FlowDict"] = final_water[
        "Water Type"
    ].map(WATER_FLOW_RESOURCE_DICT)
    final_water.loc[~resource_filter, "FlowDict"] = final_water[
        "Water Type"
    ].map(WATER_FLOW_EMISSION_DICT)
    final_water = pd.concat(
        [final_water, final_water["FlowDict"].apply(pd.Series)], axis=1
    )
    final_water.drop(
        columns=["FlowDict", "Water Type", "direction"], inplace=True
    )
    final_water.rename(
        columns={
            "value": "FlowAmount",
            "Plant Id": "FacilityID",
            "alloc_net_generation_eia": "Electricity",
        },
        inplace=True,
    )
    final_water["plant_id"]=final_water["FacilityID"]
    final_water["eGRID_ID"] = final_water["FacilityID"]
    final_water["Year"] = year
    final_water["Source"] = "netl"
    final_water["Unit"] = "kg"
    final_water["stage_code"] = "Power plant"
    final_water["TechnologicalCorrelation"] = 1
    final_water["GeographicalCorrelation"] = 1
    final_water["TemporalCorrelation"] = 1
    final_water["DataCollection"] = 5
    final_water["ReliabilityScore"] = 1
    final_water["input"]=True
    final_water["ElementaryFlowPrimeContext"]="input"
    final_water.loc[final_water["Compartment_path"].str.contains("emission"),"input"]=False
    final_water.loc[final_water["Compartment_path"].str.contains("emission"),"ElementaryFlowPrimeContext"]="emission"
    return final_water


if __name__ == "__main__":
    year = 2016
    water_df = generate_plant_water_use(year)
    water_df.to_csv(f"{output_dir}/plant_water_use_{year}.csv")
