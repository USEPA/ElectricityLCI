#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# hydro_upstream.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia860_facilities import eia860_balancing_authority
from electricitylci.globals import output_dir


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module generates a data frame with values for hydro power
plant emissions from each facility included in EIA-860. Using data from
the data file, hydropower_plant.csv, in the form of CO2, CH4, and water use
(m3) along with EIA-860 data, the entries are sorted by plant and by emission
compartment for comparison to other upstream power generation types.

Last updated:
    2024-08-02
"""
__all__ = [
    "generate_hydro_emissions",
]


##############################################################################
# FUNCTIONS
##############################################################################
def generate_hydro_emissions():
    """
    This generates a dataframe of hydro power plant emissions using data
    from an analysis performed at NETL that estimates biogenic reservoir
    emissions. The reservoir emissions are allocated among all uses for
    the reservoir (e.g., boating, fishing, etc.) using market-based
    allocation. The year for the inventory is fixed to 2016.

    Returns
    -------
    pandas.DataFrame:
        Dataframe populated with CO2 and CH4 emissions as well as consumptive
        water use induced by the hydroelectric generator.

    Notes
    -----
    The 2016 hydropower_plant.csv provided in the data directory of eLCI has
    no emissions data for facility 1784, Twin Falls (MI); therefore, the
    data frame will have a row with NaNs for flow amounts.
    """
    DATA_FILE = "hydropower_plant.csv"
    FLOW_DICTIONARY={
        "co2 (kg)": {
            "FlowName": "Carbon dioxide",
            "FlowUUID": "b6f010fb-a764-3063-af2d-bcb8309a97b7",
            "Compartment_path": "emission/air",
            "Compartment": "air"},
        "ch4 (kg)": {
            "FlowName": "Methane",
            "FlowUUID": "aab83476-ec6c-3742-af85-15d320b7ce80",
            "Compartment_path": "emission/air",
            "Compartment": "air"},
        "water use (m3)": {
            "FlowName": "Water, fresh",
            "FlowUUID": "8ba7dd57-b502-397b-944a-f63c6615f754",
            "Compartment_path": "resource/water",
            "Compartment": "input"}
    }

    hydro_df = pd.read_csv(
        os.path.join(data_dir, DATA_FILE),
        index_col=0,
        low_memory=False,
    )
    hydro_df.rename(
        columns={
            "Plant Id": "FacilityID",
            "Annual Net Generation (MWh)": "Electricity",
            "NERC Region": "NERC",
            },
        inplace=True
    )
    # Remove impact column... we'll handle that later.
    hydro_df.drop(columns=["co2e (kg)"], inplace=True)

    # Convert the emission columns into emission rows.
    hydro_df = hydro_df.melt(
        id_vars=["FacilityID", "Plant Name", "NERC", "Electricity"],
        var_name="flow"
    )

    # This creates a new value column, rename to flow amount
    hydro_df.rename(
        columns={"value": "FlowAmount"},
        inplace=True
    )

    # Map flows through the flow dictionary; new flowDict column (dict)
    hydro_df["FlowDict"] = hydro_df["flow"].map(FLOW_DICTIONARY)

    # Parse out the dictionary into pandas series, making each their own col
    hydro_df = pd.concat(
        [hydro_df, hydro_df["FlowDict"].apply(pd.Series)],
        axis=1
    )

    # Add other necessary metadata
    hydro_df["Year"] = 2016
    hydro_df["Source"] = "netl"

    # Read in 2016 power plant location data (i.e., state, NERC, BA).
    eia860_df = eia860_balancing_authority(2016)
    # Type cast plant ID to integer (for merging)
    eia860_df["Plant Id"] = eia860_df["Plant Id"].astype(int)
    # Merge plant data on plant ID.
    hydro_df = hydro_df.merge(
        eia860_df,
        left_on="FacilityID",
        right_on="Plant Id",
        suffixes=["","_eia"]
    )

    # WARNING: some facilities are not matched to a BA.
    #   NOTE: they are Hawaii and Alaska
    ba_name = 'Balancing Authority Name'
    no_ba = len(hydro_df.loc[hydro_df[ba_name].isna(), 'FacilityID'].unique())
    logging.warning("Failed to match %d hydro facilities to a BA" % no_ba)

    # Drop duplicate and unused columns from data frame
    hydro_df.drop(
        columns=["FlowDict", "flow", "NERC Region", "Plant Id"],
        inplace=True
    )
    # HOTFIX: type in flow name [TWD: 2024-07-30]
    # Unit conversion (e.g., 1000 kg/m3)
    fw_rows = hydro_df["FlowName"] == "Water, fresh"
    hydro_df.loc[fw_rows, "FlowAmount"] *= 1000

    hydro_df["Unit"] = "kg"
    hydro_df["stage_code"] = "Power plant"
    hydro_df["TechnologicalCorrelation"] = 1
    hydro_df["GeographicalCorrelation"] = 1
    hydro_df["TemporalCorrelation"] = 1
    hydro_df["DataCollection"] = 5
    hydro_df["DataReliability"] = 1
    hydro_df["eGRID_ID"] = hydro_df["FacilityID"]
    hydro_df["FuelCategory"] = "HYDRO"
    hydro_df["PrimaryFuel"] = "WAT"
    hydro_df["quantity"] = hydro_df["Electricity"]
    hydro_df["ElementaryFlowPrimeContext"] = "emission"
    hydro_df["input"] = False
    hydro_df.loc[
        hydro_df["FlowName"] == "Water, fresh",
        "ElementaryFlowPrimeContext"]= "resource"
    hydro_df.loc[hydro_df["FlowName"]=="Water, fresh", "input"] = True
    hydro_df["plant_id"] = hydro_df["FacilityID"]
    hydro_df["Compartment"] = hydro_df["Compartment_path"]

    return hydro_df


##############################################################################
# MAIN
##############################################################################
if __name__=="__main__":
    hydro_df=generate_hydro_emissions()
    hydro_df.to_csv(f"{output_dir}/hydro_emissions_2016.csv")
