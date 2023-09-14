# -*- coding: utf-8 -*-

"""Add docstring."""

import pandas as pd
from electricitylci.globals import data_dir
import numpy as np
from electricitylci.eia923_generation import eia923_download_extract


def generate_upstream_solarthermal(year):
    """
    Generate the annual emissions.

    For solar thermal plant construction for each plant in EIA923. The emissions
    inventory file has already allocated the total emissions to construct the
    entire power plant over its assumed 30 year life. So the emissions returned
    below represent 1/30th of the total site construction emissions.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    dataframe
    """
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data['Plant Id']=eia_generation_data['Plant Id'].astype(int)
    column_filt = (eia_generation_data['Reported Fuel Type Code'] == 'SUN')
    solar_generation_data=eia_generation_data.loc[column_filt,:]
    solar_df = pd.read_csv(
        f'{data_dir}/solar_thermal_inventory.csv',
        header=[0,1]
    )
    columns = pd.DataFrame(solar_df.columns.tolist())
    columns.loc[columns[0].str.startswith('Unnamed:'), 0] = np.nan
    columns[0] = columns[0].fillna(method='ffill')
    solar_df.columns = pd.MultiIndex.from_tuples(columns.to_records(index=False).tolist())
    solar_df_t=solar_df.transpose()
    solar_df_t=solar_df_t.reset_index()
    new_columns = solar_df_t.loc[0,:].tolist()
    new_columns[0]='Compartment'
    new_columns[1]='FlowName'
    solar_df_t.columns=new_columns
    solar_df_t.drop(index=0, inplace=True)
    solar_df_t_melt=solar_df_t.melt(
            id_vars=['Compartment','FlowName'],
            var_name='plant_id',
            value_name='FlowAmount'
    )
    solar_df_t_melt = solar_df_t_melt.astype({'plant_id' : int})
    solarthermal_upstream=solar_df_t_melt.merge(
            right=solar_generation_data,
            left_on='plant_id',
            right_on='Plant Id',
            how='left'
    )
    solarthermal_upstream.rename(columns=
            {
                    'Net Generation (Megawatthours)':'Electricity',
            },
            inplace=True
    )
    solarthermal_upstream["quantity"]=solarthermal_upstream["Electricity"]
    solarthermal_upstream.drop(columns=[
            'Plant Id',
            'NAICS Code',
            'Reported Fuel Type Code',
            'YEAR',
            'Total Fuel Consumption MMBtu',
            'State',
            'Plant Name',
            ],inplace=True)
    # These emissions will later be aggregated with any inventory power plant
    # emissions because each facility has its own construction impacts.
    solarthermal_upstream['stage_code']="Power plant"
    solarthermal_upstream['fuel_type']='SOLARTHERMAL'
    compartment_map={
            'Air':'air',
            'Water':'water',
            'Energy':'input'
    }
    solarthermal_upstream['Compartment']=solarthermal_upstream['Compartment'].map(compartment_map)
    solarthermal_upstream["Unit"]="kg"
    solarthermal_upstream["input"]=False
    return solarthermal_upstream


if __name__=='__main__':
    from electricitylci.globals import output_dir
    year=2016
    solarthermal_upstream=generate_upstream_solarthermal(year)
    solarthermal_upstream.to_csv(f'{output_dir}/upstream_solarthermal_{year}.csv')
