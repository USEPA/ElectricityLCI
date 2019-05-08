#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import data_dir, output_dir
from electricitylci.eia923_generation import eia923_download
import os
from os.path import join
from electricitylci.utils import find_file_in_folder
import requests
import PhysicalQuantities as pq

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

transport_dict={'Avg Barge Ton*Miles':'Barge',
                'Avg Lake Vessel Ton*Miles':'Lake Vessel',
                'Avg Ocean Vessel Ton*Miles':'Ocean Vessel',
                'Avg Railroad Ton*Miles':'Railroad',
                'Avg Truck Ton*Miles':'Truck'}

def eia_7a_download(year, save_path):
    eia7a_base_url = 'http://www.eia.gov/coal/data/public/xls/'
    name = 'coalpublic{}'.format(year)+'.xls'
    url = eia7a_base_url + name
    try:
        os.makedirs(save_path)
        print('Downloading EIA 7-A data...')
        eia_7a_file = requests.get(url)
        #eia_7a_file.retrieve(url,data_dir+'/'+name)
        open(save_path+'/'+name,'wb').write(eia_7a_file.content)
        #download_unzip(url,data_dir)
    except:
        print('Error downloading eia-7a: try manually downloading from:\n'+
              url)
def _clean_columns(df):
   'Remove special characters and convert column names to snake case'
   df.columns = (df.columns.str.lower()
                             .str.replace('[^0-9a-zA-Z\-]+', ' ')
                             .str.replace('-', '')
                             .str.strip()
                             .str.replace(' ', '_'))

def read_eia923_fuel_receipts(year):
    expected_923_folder = join(data_dir, 'f923_{}'.format(year))
    if not os.path.exists(expected_923_folder):
        print('Downloading EIA-923 files')
        eia923_download(year=year, save_path=expected_923_folder)
        
        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match='2_3_4_5',
            return_name=True
        )
        eia_fuel_receipts_df = pd.read_excel(
                eia923_path, sheet_name='Page 5 Fuel Receipts and Costs',
                skiprows=4, usecols="A:C,H:M,P:Q")
        csv_fn = eia923_name.split('.')[0] + '_page_5_reduced.csv'
        csv_path = join(expected_923_folder,csv_fn)
        eia_fuel_receipts_df.to_csv(csv_path,index=False)
    else:
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        print('Loading data from previously downloaded excel file')
        all_files = os.listdir(expected_923_folder)
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [f for f in all_files
                    if '.csv' in f
                    and '_page_5_reduced.csv' in f]
        csv_path = os.path.join(expected_923_folder,csv_file[0])
        if os.path.exists(csv_path):
            eia_fuel_receipts_df=pd.read_csv(csv_path)
        else:
            eia923_path, eia923_name = find_file_in_folder(
                    folder_path=expected_923_folder,
                    file_pattern_match='2_3_4_5',
                    return_name=True)
            eia_fuel_receipts_df = pd.read_excel(
                eia923_path, sheet_name='Page 5 Fuel Receipts and Costs',
                skiprows=4, usecols="A:C,H:M,P:Q")
            csv_fn = eia923_name.split('.')[0] + '_page_5_reduced.csv'
            csv_path = join(expected_923_folder,csv_fn)
            eia_fuel_receipts_df.to_csv(csv_path,index=False)
    _clean_columns(eia_fuel_receipts_df)
    return eia_fuel_receipts_df

def _coal_code(row):
    #_coal_code_str = basin_codes[basin] + '-' + coal_type_codes[coal_type] + '-' + mine_type
    coal_code_str=(basin_codes[row['netl_basin']]+'-'+
                   coal_type_codes[row['energy_source']]+'-'+
                   row['coalmine_type'])
    return coal_code_str

def _transport_code(row):
    transport_str=transport_dict[row['coal_source_code']]
    return transport_str

def generate_upstream_coal_map(year):
    eia_fuel_receipts_df=read_eia923_fuel_receipts(year)
    expected_7a_folder=join(data_dir,'f7a_{}'.format(year))
    if not os.path.exists(expected_7a_folder):
        eia_7a_download(year,expected_7a_folder)
        eia7a_path,eia7a_name = find_file_in_folder(
            folder_path=expected_7a_folder,
            file_pattern_match='coalpublic',
            return_name=True)
    else:
        eia7a_path,eia7a_name = find_file_in_folder(
            folder_path=expected_7a_folder,
            file_pattern_match='coalpublic',
            return_name=True)
    eia7a_df = pd.read_excel(eia7a_path,sheet_name='Hist_Coal_Prod',
                             skiprows=3)
    _clean_columns(eia7a_df)
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
    eia_fuel_receipts_df = eia_fuel_receipts_df.merge(
            state_region_map[['state','basin1','basin2']],
            left_on='coalmine_state',right_on='state', how='left')
    eia_fuel_receipts_df.drop(columns=['state'],inplace=True)
    eia_netl_basin = pd.read_csv(data_dir+'/eia_to_netl_basin.csv')
    eia_fuel_receipts_df=eia_fuel_receipts_df.merge(
            eia_netl_basin,how='left',
            left_on='eia_coal_supply_region',right_on='eia_basin')
    eia_fuel_receipts_df.drop(columns=['eia_basin'],inplace=True)
    gulf_lignite = (
            (eia_fuel_receipts_df['energy_source']=='LIG') &
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
        subset=['netl_basin','energy_source','coalmine_type'],inplace=True)
    eia_fuel_receipts_df['coal_source_code']=eia_fuel_receipts_df.apply(
            _coal_code,axis=1)
    eia_fuel_receipts_df['heat_input']=eia_fuel_receipts_df['quantity']*eia_fuel_receipts_df['average_heat_content']
    final_df=eia_fuel_receipts_df.groupby(
            ['plant_id','coal_source_code'],
            as_index=False)['quantity','heat_input'].sum()
    return final_df

def generate_upstream_coal(year):
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
#    coal_input_eia = pd.read_csv(data_dir+
#                                 '/2016_Coal_Input_By_Plant_EIA923.csv')
    coal_input_eia = generate_upstream_coal_map(year)
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
            left_on = ['coal_source_code'], 
            right_on =['Coal Code'],
            how = 'left')
    
    #Multiply coal mining emission factor by coal quantity; 
    #convert to kg - coal input in tons (US)
    merged_input_eia_coal_a[column_air_emission] = (
            pq.convert(1,'ton','kg')*
            merged_input_eia_coal_a[column_air_emission].multiply(
                    merged_input_eia_coal_a['quantity'],axis = "index")
            )
    
    #Keep the plant ID and air emissions columns
    merged_input_eia_coal_a = merged_input_eia_coal_a[
            ['plant_id','coal_source_code'] + column_air_emission]
    
    #Groupby the plant ID since some plants have multiple row entries 
    #(receive coal from multiple basins)
    merged_input_eia_coal_a = (
            merged_input_eia_coal_a.groupby(
                    ['plant_id','coal_source_code'],
                    as_index=False)[column_air_emission].sum())
    merged_input_eia_coal_a = merged_input_eia_coal_a.reset_index()
    
    #Melting the database on Plant ID
    melted_database_air = merged_input_eia_coal_a.melt(
            id_vars = ['plant_id','coal_source_code'], 
            var_name = 'FlowName', 
            value_name = 'FlowAmount')
    
    #Adding to new columns for the compartment (air) and 
    #The source of the emissisons (mining). 
    melted_database_air['Compartment'] = 'air'
    melted_database_air['Source'] = 'Mining'
    
    #Repeat the same methods for emissions from mining to water 
    #Merge the coal input with the coal mining water emissions dataframe using 
    #the coal code (basin-coal_type-mine_type) as the common entity
    merged_input_eia_coal_w = coal_input_eia.merge(
            coal_inventory_water_mining, 
            left_on = ['coal_source_code'], 
            right_on =['Coal Code'],
            how = 'left')
    #multiply coal mining emission factor by coal quantity; 
    #convert to kg - coal input in tons (US)
    merged_input_eia_coal_w[column_water_emission] = (
            pq.convert(1,'ton','kg')*
            merged_input_eia_coal_w[column_water_emission].multiply(
                    merged_input_eia_coal_w['quantity'],axis = "index"))
    
    #Groupby the plant ID since some plants have multiple row entries 
    #(receive coal from multiple basins)
    merged_input_eia_coal_w = merged_input_eia_coal_w.groupby(
            ['plant_id','coal_source_code'], 
            as_index=False)[column_water_emission].sum()
    merged_input_eia_coal_w = merged_input_eia_coal_w.reset_index()
    
    #Keep the plant ID and water emissions columns
    merged_input_eia_coal_w = merged_input_eia_coal_w[
            ['plant_id','coal_source_code'] + column_water_emission]
    #Melting the database on Plant ID
    melted_database_water = merged_input_eia_coal_w.melt(
            id_vars = ['plant_id','coal_source_code'], 
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
            pq.convert(1,'ton','kg')*
            merged_transport_coal[column_air_emission].multiply(
                    merged_transport_coal['value'],axis = "index"))
    
    merged_transport_coal.rename(columns={'Plant Government ID':'plant_id'},
                                 inplace=True)
    
    #Groupby the plant ID since some plants have multiple row entries 
    #(receive coal from multiple basins)
    merged_transport_coal= merged_transport_coal.groupby(
            ['plant_id','Transport'])[column_air_emission].sum()
    merged_transport_coal= merged_transport_coal.reset_index()
    
    #Keep the plant ID and emissions columns
    merged_transport_coal = (
            merged_transport_coal[
                    ['plant_id','Transport'] + column_air_emission])
    merged_transport_coal.rename(columns={'Transport':'coal_source_code'},
                                 inplace=True)
    #Melting the database on Plant ID
    melted_database_transport = merged_transport_coal.melt(
            id_vars = ['plant_id','coal_source_code'], 
            var_name = 'FlowName', 
            value_name = 'FlowAmount')
    melted_database_transport['coal_source_code']=melted_database_transport.apply(
            _transport_code,axis=1)
    #Adding to new columns for the compartment (water) and 
    #The source of the emissisons (mining). 
    melted_database_transport['Compartment'] = 'air'
    melted_database_transport['Source'] = 'Transportation'
    
    merged_coal_upstream = pd.concat([melted_database_air, 
                                      melted_database_water, 
                                      melted_database_transport])
    merged_coal_upstream['fuel_type']='Coal'
    merged_coal_upstream.rename(columns={
            'coal_source_code':'stage_code',
            'Source':'stage'
            },inplace=True)
    merged_coal_upstream.sort_values(
            ['plant_id','stage','stage_code','Compartment','FlowName'],
            inplace=True)
    merged_coal_upstream.reset_index(drop=True,inplace=True)
    return merged_coal_upstream

if __name__=='__main__':
    year=2014
    df = generate_upstream_coal(year)
    df.to_csv(output_dir+'/coal_emissions_{}.csv'.format(year))
