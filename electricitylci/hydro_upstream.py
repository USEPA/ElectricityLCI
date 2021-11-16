#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
from electricitylci.globals import output_dir, data_dir
import electricitylci.PhysicalQuantities as pq
from electricitylci.eia923_generation import eia923_download_extract
from electricitylci.eia860_facilities import eia860_balancing_authority


def generate_hydro_emissions():
    """
    This generates a dataframe of hydro power plant emissions using data
    from an analysis performed at NETL that estimates biogenic reservoir
    emissions. The reservoir emissions are allocated among all uses for
    the reservoir (e.g., boating, fishing, etc.) using market-based
    allocation. The year for the inventory is fixed to 2016.

    Parameters
    ----------
        None
    Returns
    -------
    dataframe:
        Dataframe populated with CO2 and CH4 emissions as well as consumptive
        water use induced by the hydroelectric generator.
    """
    DATA_FILE="hydropower_plant.csv"
    hydro_df = pd.read_csv(
            f"{data_dir}/{DATA_FILE}", index_col=0, low_memory=False
        )

    hydro_df.rename(columns={"Plant Id":"FacilityID","Annual Net Generation (MWh)":"Electricity"},inplace=True)
    FLOW_DICTIONARY={
            "co2 (kg)":{"FlowName":"Carbon dioxide","FlowUUID":"b6f010fb-a764-3063-af2d-bcb8309a97b7","Compartment_path":"emission/air","Compartment":"air"},
            "ch4 (kg)":{"FlowName":"Methane","FlowUUID":"aab83476-ec6c-3742-af85-15d320b7ce80","Compartment_path":"emission/air","Compartment":"air"},
            "water use (m3)":{"FlowName":"Water, fresh","FlowUUID":"8ba7dd57-b502-397b-944a-f63c6615f754","Compartment_path":"resource/water","Compartment":"input"}
            }
    hydro_df.drop(columns=["co2e (kg)"],inplace=True)
    hydro_df=hydro_df.melt(
            id_vars=["FacilityID","Plant Name","NERC Region","Electricity"],
            var_name="flow"
            )
    hydro_df.rename(columns={"value":"FlowAmount","NERC Region":"NERC"},inplace=True)
    hydro_df["FlowDict"]=hydro_df["flow"].map(FLOW_DICTIONARY)
    hydro_df = pd.concat(
            [hydro_df, hydro_df["FlowDict"].apply(pd.Series)], axis=1
        )

    hydro_df["Year"]=2016
    hydro_df["Source"]="netl"
    eia860_df=eia860_balancing_authority(2016)
    eia860_df["Plant Id"]=eia860_df["Plant Id"].astype(int)
    hydro_df=hydro_df.merge(eia860_df,
                            left_on="FacilityID",
                            right_on="Plant Id",
                            suffixes=["","_eia"])
    hydro_df.drop(columns=["FlowDict","flow","NERC Region","Plant Id"],inplace=True)
    hydro_df.loc[hydro_df["FlowName"]=="Water, fresh","FlowAmount"]=hydro_df.loc[hydro_df["FlowName"]=="Waster, fresh","FlowAmount"]*1000
    hydro_df["Unit"]="kg"
    hydro_df["stage_code"] = "Power plant"
    hydro_df["TechnologicalCorrelation"] = 1
    hydro_df["GeographicalCorrelation"] = 1
    hydro_df["TemporalCorrelation"] = 1
    hydro_df["DataCollection"] = 5
    hydro_df["DataReliability"] = 1
    hydro_df["eGRID_ID"]=hydro_df["FacilityID"]
    hydro_df["FuelCategory"]="HYDRO"
    hydro_df["PrimaryFuel"]="WAT"
    hydro_df["quantity"]=hydro_df["Electricity"]
    hydro_df["ElementaryFlowPrimeContext"]="emission"
    hydro_df["input"]=False
    hydro_df.loc[hydro_df["FlowName"]=="Water, fresh","ElementaryFlowPrimeContext"]="resource"
    hydro_df.loc[hydro_df["FlowName"]=="Water, fresh","input"]=True
    hydro_df["plant_id"]=hydro_df["FacilityID"]
    hydro_df["Compartment"]=hydro_df["Compartment_path"]
    return hydro_df

if __name__=="__main__":
    hydro_df=generate_hydro_emissions()
    hydro_df.to_csv(f"{output_dir}/hydro_emissions_2016.csv")
