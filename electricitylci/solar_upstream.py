# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import output_dir, data_dir
import numpy as np
from electricitylci.eia923_generation import eia923_download_extract

def generate_upstream_solar(year):
    eia_generation_data = eia923_download_extract(year)
    eia_generation_data['Plant Id']=eia_generation_data['Plant Id'].astype(int)
    column_filt = (eia_generation_data['Reported Fuel Type Code'] == 'SUN')
    solar_generation_data=eia_generation_data.loc[column_filt,:]
    solar_df = pd.read_csv(
        f'{data_dir}/solar_pv_inventory.csv',
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
    solar_upstream=solar_df_t_melt.merge(
            right=solar_generation_data,
            left_on='plant_id',
            right_on='Plant Id',
            how='left'
    )
    solar_upstream.rename(columns=
            {
                    'Net Generation (Megawatthours)':'electricity',
            },
            inplace=True
    )
    solar_upstream.drop(columns=[
            'Plant Id',
            'NAICS Code',
            'Reported Fuel Type Code',
            'YEAR',
            'Total Fuel Consumption MMBtu',
            'State',
            'Plant Name',
            ],inplace=True)
    solar_upstream['stage_code']='pv_mfg'
    solar_upstream['fuel_type']='SOLAR'
    compartment_map={
            'Air':'air',
            'Water':'water',
            'Energy':'input'
    }
    solar_upstream['Compartment']=solar_upstream['Compartment'].map(compartment_map)
    #solar_upstream['Compartment']=solar_upstream['Compartment'].str.lower()
    
    return solar_upstream

if __name__=='__main__':
    year=2016
    solar_upstream=generate_upstream_solar(year)
    solar_upstream.to_csv(f'{output_dir}/upstream_solar_{year}.csv')
