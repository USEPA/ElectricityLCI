#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import data_dir, output_dir

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
