# -*- coding: utf-8 -*-

"""Add docstring."""
if __name__=='__main__':
    import electricitylci.model_config as config
    config.model_specs = config.build_model_class()
import pandas as pd
from electricitylci.globals import output_dir, data_dir
import numpy as np
from electricitylci.eia923_generation import eia923_download_extract


def generate_upstream_solar(year):
    """
    Generate the annual emissions.
    
    For solar panel construction for each plant in EIA923. The emissions
    inventory file has already allocated the total emissions to construct panels
    and balance of system for the entire power plant over the assumed 30 year
    life of the panels. So the emissions returned below represent 1/30th of the
    total site construction emissions.

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
    solar_const_df = pd.read_csv(
        f'{data_dir}/solar_PV_2020_construction_LCI.csv',
        header=[0,1],
        na_values=["#VALUE!", "#DIV/0!"]
    )
    columns = pd.DataFrame(solar_const_df.columns.tolist())
    columns.loc[columns[0].str.startswith('Unnamed:'), 0] = np.nan
    columns[0] = columns[0].fillna(method='ffill')
    solar_const_df.columns = pd.MultiIndex.from_tuples(columns.to_records(index=False).tolist())
    solar_df_t=solar_const_df.transpose()
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
    solar_upstream=solar_df_t_melt.merge(
            right=solar_generation_data,
            left_on='plant_id',
            right_on='Plant Id',
            how='left'
    )
    solar_upstream.rename(columns=
            {
                    'Net Generation (Megawatthours)':'quantity',
            },
            inplace=True
    )
    solar_upstream["Electricity"]=solar_upstream["quantity"]
    solar_upstream.drop(columns=[
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
    solar_upstream['stage_code']="solar_pv_const"
    solar_upstream['fuel_type']='SOLAR'
    compartment_map={
            'Air':'air',
            'Water':'water',
            'Energy':'input'
    }
    solar_upstream['Compartment']=solar_upstream['Compartment'].map(compartment_map)
    solar_upstream["Unit"]="kg"
    solar_upstream["input"]=False
    solar_upstream.loc[solar_upstream["FlowName"]=="Electricity","input"]=True
    
    # solar_upstream['Compartment']=solar_upstream['Compartment'].str.lower()

    #Operations inventory will be pulled in below. Unlike the construction
    #inventory, operations are on the basis of per MWh, so in order for the
    #data to integrate correctly with the rest of the inventory, we'll need to 
    #multiply all inventory by electricity generation (in MWh) for the target year.
    solar_operations_df= pd.read_csv(
        f'{data_dir}/solar_PV_2020_OM_LCI.csv',
        header=[0,1],
        na_values=["#VALUE!", "#DIV/0!"]
    )
    columns = pd.DataFrame(solar_operations_df.columns.tolist())
    columns.loc[columns[0].str.startswith('Unnamed:'), 0] = np.nan
    columns[0] = columns[0].fillna(method='ffill')
    solar_operations_df.columns = pd.MultiIndex.from_tuples(columns.to_records(index=False).tolist())
    solar_op_df_t=solar_operations_df.transpose()
    solar_op_df_t=solar_op_df_t.reset_index()
    new_columns = solar_op_df_t.loc[0,:].tolist()
    new_columns[0]='Compartment'
    new_columns[1]='FlowName'
    solar_op_df_t.columns=new_columns
    solar_op_df_t.drop(index=0, inplace=True)
    solar_op_df_t_melt=solar_op_df_t.melt(
            id_vars=['Compartment','FlowName'],
            var_name='plant_id',
            value_name='FlowAmount'
    )
    solar_op_df_t_melt = solar_op_df_t_melt.astype({'plant_id' : int})
    solar_ops=solar_op_df_t_melt.merge(
            right=solar_generation_data,
            left_on='plant_id',
            right_on='Plant Id',
            how='left'
    )
    solar_ops.rename(columns=
            {
                    'Net Generation (Megawatthours)':'quantity',
            },
            inplace=True
    )
    solar_ops["Electricity"]=solar_ops["quantity"]
    solar_ops["FlowAmount"]=solar_ops["FlowAmount"]*solar_ops["Electricity"]
    solar_ops.drop(columns=[
            'Plant Id',
            'NAICS Code',
            'Reported Fuel Type Code',
            'YEAR',
            'Total Fuel Consumption MMBtu',
            'State',
            'Plant Name',
            ],inplace=True)
    solar_ops['stage_code']="Power plant"
    solar_ops['fuel_type']='SOLAR'

    solar_ops['Compartment']=solar_ops['Compartment'].map(compartment_map)
    solar_ops["Unit"]="kg"
    solar_ops["input"]=False
    solar_ops.loc[solar_ops["FlowName"]=="Electricity","input"]=True
    solar_total=pd.concat([solar_upstream,solar_ops])
    return solar_total


if __name__=='__main__':
    year=2020
    solar_upstream=generate_upstream_solar(year)
    solar_upstream.to_csv(f'{output_dir}/upstream_solar_{year}.csv')
