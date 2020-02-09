# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import output_dir, data_dir
import numpy as np
from electricitylci.eia923_generation import eia923_download_extract


def generate_upstream_wind(year):
    """
    Generate the annual emissions for wind farm construction for each 
    plant in EIA923. The emissions inventory file has already allocated the
    total emissions to construct the turbines and balance of system for the 
    entire wind farm over the assumed 20 year life of the panels. So the 
    emissions returned below represent 1/20th of the total site construction 
    emissions.
    
    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.
    
    Returns
    ----------
    dataframe
    """
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data["Plant Id"] = eia_generation_data["Plant Id"].astype(
        int
    )
    column_filt = eia_generation_data["Reported Fuel Type Code"] == "WND"
    wind_generation_data = eia_generation_data.loc[column_filt, :]
    wind_df = pd.read_csv(f"{data_dir}/wind_inventory.csv", header=[0, 1])
    columns = pd.DataFrame(wind_df.columns.tolist())
    columns.loc[columns[0].str.startswith("Unnamed:"), 0] = np.nan
    columns[0] = columns[0].fillna(method="ffill")
    wind_df.columns = pd.MultiIndex.from_tuples(
        columns.to_records(index=False).tolist()
    )
    wind_df_t = wind_df.transpose()
    wind_df_t = wind_df_t.reset_index()
    new_columns = wind_df_t.loc[0, :].tolist()
    new_columns[0] = "Compartment"
    new_columns[1] = "FlowName"
    wind_df_t.columns = new_columns
    wind_df_t.drop(index=0, inplace=True)
    wind_df_t_melt = wind_df_t.melt(
        id_vars=["Compartment", "FlowName"],
        var_name="plant_id",
        value_name="FlowAmount",
    )
    wind_df_t_melt = wind_df_t_melt.astype({'plant_id' : int})
    wind_upstream = wind_df_t_melt.merge(
        right=wind_generation_data,
        left_on="plant_id",
        right_on="Plant Id",
        how="left",
    )
    wind_upstream.rename(
        columns={"Net Generation (Megawatthours)": "quantity"}, inplace=True
    )
    wind_upstream["Electricity"]=wind_upstream["quantity"]
    wind_upstream.drop(
        columns=[
            "Plant Id",
            "NAICS Code",
            "Reported Fuel Type Code",
            "YEAR",
            "Total Fuel Consumption MMBtu",
            "State",
            "Plant Name",
        ],
        inplace=True,
    )
    # These emissions will later be aggregated with any inventory power plant
    # emissions because each facility has its own construction impacts.
    wind_upstream["stage_code"] = "Power plant"
    wind_upstream["fuel_type"] = "WIND"
    compartment_map = {"Air": "air", "Water": "water", "Energy": "input"}
    wind_upstream["Compartment"] = wind_upstream["Compartment"].map(
        compartment_map
    )
    wind_upstream["input"]=False
    wind_upstream.loc[wind_upstream["Compartment"]=="input","input"]=True
    wind_upstream["Unit"]="kg"
    # wind_upstream['Compartment']=wind_upstream['Compartment'].str.lower()
    return wind_upstream


if __name__ == "__main__":
    year = 2016
    wind_upstream = generate_upstream_wind(year)
    wind_upstream.to_csv(f"{output_dir}/upstream_wind_{year}.csv")
