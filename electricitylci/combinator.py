# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import output_dir, data_dir
import electricitylci.alt_generation as altg
import electricitylci.import_impacts as import_impacts
def concat_map_upstream_databases(*arg):
    mapped_column_dict={
        'UUID (EPA)':'FlowUUID',
        'FlowName':'model_flow_name',
        'Flow name (EPA)':'FlowName'
    }
    print(f'Concatenating and flow-mapping {len(arg)} upstream databases.')
    upstream_df_list = list()
    for df in arg:
        upstream_df_list.append(df)
    upstream_df = pd.concat(upstream_df_list)
    netl_epa_flows = pd.read_csv(
            data_dir+'/Elementary_Flows_NETL.csv',
            skiprows=2,
            usecols=[0,1,2,6,7,8]
    )
    netl_epa_flows['Category']=netl_epa_flows['Category'].str.replace(
            'Emissions to ','',).str.lower()
    netl_epa_flows['Category']=netl_epa_flows['Category'].str.replace(
            'emission to ','',).str.lower()
    
    upstream_df = upstream_df.groupby(
            ['fuel_type','stage_code','FlowName','Compartment','plant_id']
            ,as_index=False
            ).agg({
                    'FlowAmount':'sum',
                    'quantity':'mean'
            })
    upstream_mapped_df = pd.merge(
                left=upstream_df,
                right=netl_epa_flows,
                left_on=['FlowName','Compartment'],
                right_on=['NETL Flows','Category'],
                how='left'
        )
    upstream_mapped_df = upstream_mapped_df.rename(
            columns=mapped_column_dict,copy=False)
    upstream_mapped_df.drop_duplicates(
            subset=['FlowName','Compartment','FlowAmount'], inplace=True)
    upstream_mapped_df.dropna(subset=['FlowName'],inplace=True)
    garbage=upstream_mapped_df.loc[
            upstream_mapped_df['FlowName']=='[no match]',:].index
    upstream_mapped_df.drop(garbage, inplace=True)
    upstream_mapped_df.drop(
            columns=[
                    'model_flow_name',
                    'Category',
                    'CAS',
                    'Category.2',
                    'NETL Flows'
                    ],
            inplace=True
    )
    upstream_mapped_df.rename(
            columns={
                    'fuel_type':'FuelCategory',        
            },
            inplace=True
    )
    upstream_mapped_df['FuelCategory']=(
            upstream_mapped_df['FuelCategory'].str.upper())
    upstream_mapped_df['ElementaryFlowPrimeContext']='emission'
    upstream_mapped_df['Unit']='kg'
    return upstream_mapped_df

def concat_clean_upstream_and_plant(pl_df, up_df):
    region_cols=[
            'NERC',
            'Balancing Authority Code',
            'Balancing Authority Name',
            'Subregion',
    ]
    up_df=up_df.merge(
            right=pl_df[['eGRID_ID']+region_cols].drop_duplicates(),
            left_on='plant_id',
            right_on='eGRID_ID',
            how='left'
    )
    up_df.dropna(subset=region_cols,inplace=True)
    combined_df = pd.concat(
            [pl_df,up_df],
            ignore_index=True
    )
    combined_df.drop(columns=[
            'plant_id',
            ],
            inplace=True
    )
    combined_df['FacilityID']=combined_df['eGRID_ID']
    return combined_df
    
if __name__=='__main__':
    import electricitylci.coal_upstream as coal
    import electricitylci.natural_gas_upstream as ng
    import electricitylci.petroleum_upstream as petro
    import electricitylci.geothermal as geo
    import electricitylci.solar_upstream as solar
    import electricitylci.wind_upstream as wind
    coal_df = coal.generate_upstream_coal(2016)
    ng_df = ng.generate_upstream_ng(2016)
    petro_df = petro.generate_petroleum_upstream(2016)
    geo_df = geo.generate_upstream_geo(2016)
    solar_df = solar.generate_upstream_solar(2016)
    wind_df = wind.generate_upstream_wind(2016)
    upstream_df = concat_map_upstream_databases(
            coal_df, ng_df, petro_df, geo_df,solar_df,wind_df
    )
    plant_df = altg.create_generation_process_df()
    plant_df['stage_code']='Power plant'
    combined_df=concat_clean_upstream_and_plant(plant_df, upstream_df)
    canadian_inventory = import_impacts.generate_canadian_mixes(combined_df)
    combined_df=pd.concat([combined_df,canadian_inventory])
    combined_df.sort_values(by=['eGRID_ID','Compartment','FlowName','stage_code'], inplace=True)
    combined_df.to_csv(f'{output_dir}/combined_df.csv')
