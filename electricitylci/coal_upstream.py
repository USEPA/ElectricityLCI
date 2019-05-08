#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import data_dir, output_dir
from electricitylci.eia923_generation import eia923_download
import os
from os.path import join
from electricitylci.utils import find_file_in_folder
import requests

def eia_7a_download(year):
    eia7a_base_url = 'http://www.eia.gov/coal/data/public/xls/'
    name = 'coalpublic{}'.format(year)+'.xls'
    url = eia7a_base_url + name
    try:
        print('Downloading EIA 7-A data...')
        eia_7a_file = requests.get(url)
        #eia_7a_file.retrieve(url,data_dir+'/'+name)
        open(data_dir+'/'+name,'wb').write(eia_7a_file.content)
        #download_unzip(url,data_dir)
    except:
        print('Error downloading eia-7a: try manually downloading from:\n'+
              url)
def clean_columns(df):
   'Remove special characters and convert column names to snake case'
   df.columns = (df.columns.str.lower()
                             .str.replace('[^0-9a-zA-Z\-]+', ' ')
                             .str.replace('-', '')
                             .str.strip()
                             .str.replace(' ', '_'))

def read_eia923_fuel_receipts(eia923_path):
    pass

coal_type_codes={'BIT':'B',
                 'LIG':'L',
                 'SUB':'S',
                 'WC':'W'}
mine_type_codes={'Surface':'S',
                 'Underground':'U',
                 'Facility':'F'}
basin_codes={'Central Appalachia':'CA',
             'Central Interior':'CI',
             'Gulf Lignite':'GL',
             'Illinois Basin':'IB',
             'Lignite':'L',
             'Northern Appalachia':'NA',
             'Powder River Basin':'PRB',
             'Rocky Mountain':'RM',
             'Southern Appalachia':'SA',
             'West/Northwest':'WNW',
             'Import':'IMP'}

def coal_code(row):
    #coal_code_str = basin_codes[basin] + '-' + coal_type_codes[coal_type] + '-' + mine_type
    coal_code_str=(basin_codes[row['netl_basin']]+'-'+
                   coal_type_codes[row['energy_source']]+'-'+
                   row['coalmine_type'])
    return coal_code_str

def generate_upstream_coal_map(year):
    expected_923_folder = join(data_dir, 'f923_{}'.format(year))
    if not os.path.exists(expected_923_folder):
        print('Downloading EIA-923 files')
        eia923_download(year=year, save_path=expected_923_folder)
        
        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match='2_3_4_5',
            return_name=True
        )
    else:
        all_files = os.listdir(expected_923_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        print('Loading data from previously downloaded excel file')
        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match='2_3_4_5',
            return_name=True
        )
    eia_fuel_receipts_df = pd.read_excel(
            eia923_path, sheet_name='Page 5 Fuel Receipts and Costs',
            skiprows=4)
    clean_columns(eia_fuel_receipts_df)
    name = 'coalpublic{}'.format(year)+'.xls'
    if not os.path.exists(data_dir+'/'+name):
        eia_7a_download(year)
        eia7a_path,eia7a_name = find_file_in_folder(
            folder_path=data_dir,
            file_pattern_match='coalpublic',
            return_name=True)
    else:
        eia7a_path,eia7a_name = find_file_in_folder(
            folder_path=data_dir,
            file_pattern_match='coalpublic',
            return_name=True)
    eia7a_df = pd.read_excel(eia7a_path,sheet_name='Hist_Coal_Prod',
                             skiprows=3)
    clean_columns(eia7a_df)
    coal_criteria = eia_fuel_receipts_df['fuel_group']=='Coal'
    eia_fuel_receipts_df = eia_fuel_receipts_df.loc[coal_criteria,:]
    eia_fuel_receipts_df = eia_fuel_receipts_df.merge(
            eia7a_df[['msha_id','coal_supply_region']],how='left',
            left_on='coalmine_msha_id',right_on='msha_id',
            )
    eia_fuel_receipts_df.drop(columns=['msha_id'],inplace=True)
    eia_fuel_receipts_df.rename(
            columns={'coal_supply_region':'eia_coal_supply_region'},
            inplace=True)
    state_region_map = pd.read_csv(data_dir+'/coal_state_to_basin.csv')
    nan_msha = eia_fuel_receipts_df['coalmine_msha_id'].isna()
    eia_fuel_receipts_df = eia_fuel_receipts_df.merge(
            state_region_map[['state','basin1','basin2']],
            left_on='coalmine_state',right_on='state', how='left')
    eia_fuel_receipts_df.drop(columns=['state'],inplace=True)
    eia_netl_basin = pd.read_csv(data_dir+'/eia_to_netl_basin.csv')
    eia_fuel_receipts_df=eia_fuel_receipts_df.merge(
            eia_netl_basin,how='left',
            left_on='eia_coal_supply_region',right_on='eia_basin')
    eia_fuel_receipts_df.drop(columns=['eia_basin'],inplace=True)
    gulf_lignite = ((eia_fuel_receipts_df['energy_source']=='LIG') &
                    (eia_fuel_receipts_df['eia_coal_supply_region']=='Interior'))
    eia_fuel_receipts_df.loc[gulf_lignite,['netl_basin']] = 'Gulf Lignite'
    lignite = ((eia_fuel_receipts_df['energy_source']=='LIG') &
                    (eia_fuel_receipts_df['eia_coal_supply_region']=='Western'))
    eia_fuel_receipts_df.loc[lignite,['netl_basin']]='Lignite'
    netl_na = (eia_fuel_receipts_df['netl_basin'].isna())
    minimerge = pd.merge(left=eia_fuel_receipts_df,
                         right=eia_netl_basin,
                         left_on='basin1',right_on='eia_basin',
                         how='left')
    eia_fuel_receipts_df.loc[netl_na,'netl_basin']=(
            minimerge.loc[netl_na,'netl_basin_y'])
    eia_fuel_receipts_df[['netl_basin','energy_source','coalmine_type']]
    eia_fuel_receipts_df.dropna(
        subset=['coalmine_type'],inplace=True)
    eia_fuel_receipts_df['coal_source_code']=eia_fuel_receipts_df.apply(
            coal_code,axis=1)
    final_df=eia_fuel_receipts_df.groupby(
            ['plant_id','coal_source_code'])['quantity'].sum()
    return eia_fuel_receipts_df, eia7a_df

#eia_fuel_receipts_df['coal_mine_code']=
eia_fuel_receipts_df, eia7a_df= generate_upstream_coal_map(2016)
eia_fuel_receipts.info(verbose=True)

def generate_upstream_coal():
    """
    Generate the annual coal mining and transportation emissions (in kg) for 
    each plant in EIA923.
    
    Parameters
    ----------
    None
    
    Returns
    ----------
    dataframe
    """
    
    #Reading the coal input from eia
    coal_input_eia = pd.read_csv(data_dir+
                                 '/2016_Coal_Input_By_Plant_EIA923.csv')
    
    #Reading coal transportation data
    coal_transportation = pd.read_csv(data_dir+
                                      '/2016_Coal_Trans_By_Plant_ABB_Data.csv')
    
    #Creating a list with the different transportation modes
    #trans_modes = coal_transportation.columns[1:]
    
    #Reading the coal emissions to air from mining (units = kg/kg coal)
    coal_inventory_air_mining = pd.read_excel(
            data_dir+'/Coal_model_Basin_and_transportation_inventory.xlsx',
            sheet_name = 'air_mining')
    
    #Reading coal inventory for emissions to water from mining 
    #(units = kg/kg coal)
    coal_inventory_water_mining = pd.read_excel(
            data_dir+'/Coal_model_Basin_and_transportation_inventory.xlsx',
            sheet_name = 'water_mining')   
    
    #Creating a list with the emission names
    column_air_emission = list(coal_inventory_air_mining.columns[2:])
        
    #Creating a column with water emission names.
    column_water_emission = list(coal_inventory_water_mining.columns[2:])
        
    #Reading coal inventory for transportation emissions due transportation 
    #(units = kg/ton-mile) 
    coal_inventory_transportation = pd.read_excel(
            data_dir+'/Coal_model_Basin_and_transportation_inventory.xlsx',
            sheet_name = 'transportation')
    
    #Merge the coal input with the coal mining air emissions dataframe using 
    #the coal code (basin-coal_type-mine_type) as the common entity
    merged_input_eia_coal_a = coal_input_eia.merge(
            coal_inventory_air_mining, 
            left_on = ['Coal Source Code'], 
            right_on =['Coal Code'],
            how = 'left')
    
    #Multiply coal mining emission factor by coal quantity; 
    #convert to kg - coal input in tons (US)
    merged_input_eia_coal_a[column_air_emission] = (
            907.185*merged_input_eia_coal_a[column_air_emission].multiply(
                    merged_input_eia_coal_a['QUANTITY (tons)'],axis = "index")
            )
    
    #Keep the plant ID and air emissions columns
    merged_input_eia_coal_a = merged_input_eia_coal_a[['Plant Id'] + 
                                                      column_air_emission]
    
    #Groupby the plant ID since some plants have multiple row entries 
    #(receive coal from multiple basins)
    merged_input_eia_coal_a = (
            merged_input_eia_coal_a.groupby(
                    ['Plant Id'])[column_air_emission].sum())
    merged_input_eia_coal_a = merged_input_eia_coal_a.reset_index()
    
    #Melting the database on Plant ID
    melted_database_air = merged_input_eia_coal_a.melt(
            id_vars = ['Plant Id'], 
            var_name = 'FlowName', 
            value_name = 'FlowAmount')
    
    #Adding to new columns for the compartment (air) and 
    #The source of the emissisons (mining). 
    melted_database_air['Compartment'] = 'air'
    melted_database_air['Source'] = 'Extraction'
    
    #Repeat the same methods for emissions from mining to water 
    #Merge the coal input with the coal mining water emissions dataframe using 
    #the coal code (basin-coal_type-mine_type) as the common entity
    merged_input_eia_coal_w = coal_input_eia.merge(
            coal_inventory_water_mining, 
            left_on = ['Coal Source Code'], 
            right_on =['Coal Code'],
            how = 'left')
    #multiply coal mining emission factor by coal quantity; 
    #convert to kg - coal input in tons (US)
    merged_input_eia_coal_w[column_water_emission] = (
            907.185*merged_input_eia_coal_w[column_water_emission].multiply(
                    merged_input_eia_coal_w['QUANTITY (tons)'],axis = "index"))
    
    #Groupby the plant ID since some plants have multiple row entries 
    #(receive coal from multiple basins)
    merged_input_eia_coal_w = merged_input_eia_coal_w.groupby(
            ['Plant Id'])[column_water_emission].sum()
    merged_input_eia_coal_w = merged_input_eia_coal_w.reset_index()
    
    #Keep the plant ID and water emissions columns
    merged_input_eia_coal_w = merged_input_eia_coal_w[['Plant Id'] + 
                                                      column_water_emission]
    #Melting the database on Plant ID
    melted_database_water = merged_input_eia_coal_w.melt(
            id_vars = ['Plant Id'], 
            var_name = 'FlowName', 
            value_name = 'FlowAmount')
    #Adding to new columns for the compartment (water) and 
    #The source of the emissisons (mining). 
    melted_database_water['Compartment'] = 'water'
    melted_database_water['Source'] = 'Mining'
    
    #Repeat the same methods for emissions from transportation 
    coal_transportation = coal_transportation.melt(
            'Plant Government ID',var_name = 'Transport')
    merged_transport_coal = coal_transportation.merge(
            coal_inventory_transportation, 
            left_on = ['Transport'], 
            right_on =['Modes'],
            how = 'left')
    
    #multiply transportation emission factor (kg/kg-mi) by total transportation
    #(ton-miles)
    merged_transport_coal[column_air_emission] = (
            907.185*merged_transport_coal[column_air_emission].multiply(
                    merged_transport_coal['value'],axis = "index"))
    merged_transport_coal['Plant Id'] = (
            merged_transport_coal['Plant Government ID'])
    
    #Groupby the plant ID since some plants have multiple row entries 
    #(receive coal from multiple basins)
    merged_transport_coal= merged_transport_coal.groupby(
            ['Plant Id'])[column_air_emission].sum()
    merged_transport_coal= merged_transport_coal.reset_index()
    
    #Keep the plant ID and emissions columns
    merged_transport_coal = (
            merged_transport_coal[['Plant Id'] + column_air_emission])
    #Melting the database on Plant ID
    melted_database_transport = merged_transport_coal.melt(
            id_vars = ['Plant Id'], 
            var_name = 'FlowName', 
            value_name = 'FlowAmount')
    #Adding to new columns for the compartment (water) and 
    #The source of the emissisons (mining). 
    melted_database_transport['Compartment'] = 'air'
    melted_database_transport['Source'] = 'Transportation'
    
    merged_coal_upstream = pd.concat([melted_database_air, 
                                      melted_database_water, 
                                      melted_database_transport])
    merged_coal_upstream.sort_values(
            ['Plant Id','Source','FlowName'],inplace=True)
    merged_coal_upstream.reset_index(drop=True,inplace=True)
    return merged_coal_upstream

if __name__=='__main__':
    df = generate_upstream_coal()
    df.to_csv(output_dir+'/coal_emissions.csv')
