# -*- coding: utf-8 -*-
"""
Created on Mon Feb 18 11:39:15 2019

@author: cooneyg
"""
import pandas as pd
from electricitylci.globals import (
    data_dir,
    output_dir)

from electricitylci.eia923_generation import eia923_download_extract
import PhysicalQuantities as pq


strings = [' air','unspecified','Organic intermediate products',
           ' water','agricultural soil'
]

replacement = ['air','air','air','water','soil']

mapping = dict(zip(strings,replacement))

def determine_compartment(df):
    result = []
    for searchcol in df['FlowName']:
        for s in mapping:
            if s in searchcol:
                result.append(mapping[s])
                break
        else:
            result.append(float('nan'))
    df['Compartment']=result
#    if ' air' in df_row['FlowName']:
#        return 'air'
#    elif 'unspecified' in df_row['FlowName']:
#        return 'air'
#    elif ' water' in df_row['FlowName']:
#        return 'water'
#    elif 'agricultural soil' in df_row['FlowName']:
#        return 'soil'
#    else:
#        return float('nan')

def _remove_brackets(text):
    start = text.find(' [')
    if start!=1:
        return text[0:start]
    else:
        pass

def generate_upstream_ng(year):
    """
    Generate the annual gas extraction, processing and transportation 
    emissions (in kg) for each plant in EIA923.
    
    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.
    
    Returns
    ----------
    dataframe
    """
    
    #Get the EIA generation data for the specified year, this dataset includes
    #the fuel consumption for generating electricity for each facility
    #and fuel type. Filter the data to only include NG facilities and on positive
    #fuel consumption. Group that data by Plant Id as it is possible to have
    #multiple rows for the same facility and fuel based on different prime
    #movers (e.g., gas turbine and combined cycle). 
    eia_generation_data = eia923_download_extract(year)
    
    column_filt = ((eia_generation_data['Reported Fuel Type Code'] == 'NG') & 
                   (eia_generation_data['Total Fuel Consumption MMBtu'] > 0))
    ng_generation_data = eia_generation_data[column_filt]
    
    ng_generation_data = ng_generation_data.groupby('Plant Id').agg(
            {'Total Fuel Consumption MMBtu':'sum'}).reset_index()
    ng_generation_data['Plant Id'] = ng_generation_data['Plant Id'].astype(int)
    
    
    #Import the mapping file which has the source gas basin for each Plant Id. 
    #Merge with ng_generation dataframe.
    
    ng_basin_mapping = pd.read_csv(data_dir + '/gas_supply_basin_mapping.csv')
    
    subset_cols = ['Plant Code', 'NG_LCI_Name']
    
    ng_basin_mapping = ng_basin_mapping[subset_cols]
    
    ng_generation_data_basin = pd.merge(
            left = ng_generation_data, 
            right = ng_basin_mapping, 
            left_on = 'Plant Id', 
            right_on = 'Plant Code')
    
    ng_generation_data_basin = ng_generation_data_basin.drop(
            columns = ['Plant Code'])
    
    #Read the NG LCI excel file
    ng_lci = pd.read_csv(data_dir + '/NG_LCI.csv') 
            #sheet_name = 'Basin_Mean_Data')
    
    #Getting list of column values which are basically the emissions names
    emissions_list = list(ng_lci.columns[2:])
    
    # This line can go away once the elementary flow names are replaced in the 
    #NG_LCI.xlsx file.
    emissions_list = [emission.replace(' (kg/MJ)','') for emission in emissions_list] 
   
    ng_lci_cols = ['Basin', 'Stage'] + emissions_list
    
    ng_lci.columns = ng_lci_cols
    
    ng_lci['Basin']=ng_lci['Basin'].str.strip()
    #Merge basin data with LCI dataset
    ng_lci_basin = pd.merge(
            ng_lci, 
            ng_generation_data_basin, 
            left_on = 'Basin', 
            right_on = 'NG_LCI_Name',
            how='left')
    
    
    #Multiplying with the EIA 923 fuel consumption; conversion factor is 
    #for MMBtu to MJ
    for i in emissions_list:
        ng_lci_basin[i] = (
                ng_lci_basin[i] * 
                ng_lci_basin['Total Fuel Consumption MMBtu'] * 
                pq.convert(10**6,'Btu','MJ'))
        
    ng_lci_basin = ng_lci_basin.rename(columns=
            {'Total Fuel Consumption MMBtu':'quantity'})
    
    #Output is kg emission for the specified year by facility Id, 
    #not normalized to electricity output
    ng_lci_basin_melt = ng_lci_basin.melt(
            id_vars = ['Plant Id', 'Stage','NG_LCI_Name',
                       'quantity'],
            value_vars = emissions_list,
            var_name = 'FlowName',
            value_name = 'FlowAmount')

    #Aggregate process stages 'PRODUCTION', 'GATHERING & BOOSTING', 
    #'PROCESSING', 'TRANSMISSION','STORAGE', 'PIPELINE' to just 
    #"extraction" and "transportation"
    
    ng_stage_dict = {'PRODUCTION':'extraction', 
                     'GATHERING & BOOSTING':'extraction',
                     'PROCESSING':'extraction', 
                     'TRANSMISSION':'transportation', 
                     'STORAGE':'transportation', 
                     'PIPELINE':'transportation'}
    ng_lci_basin_melt['Stage']=ng_lci_basin_melt['Stage'].map(ng_stage_dict)
    #Group emissiosn by new stage designation - extraction and transportation    
    
    ng_lci_basin_grouped = ng_lci_basin_melt.groupby(
            ['Plant Id',
             'NG_LCI_Name',
             'Stage',
             'FlowName',
             'quantity']).agg({'FlowAmount':'sum'}).reset_index()
#    ng_lci_basin_grouped['Compartment']=ng_lci_basin_grouped.apply(
#        determine_compartment, axis=1)
    determine_compartment(ng_lci_basin_grouped)    
    ng_lci_basin_grouped['fuel_type']='Natural gas'
    ng_lci_basin_grouped.rename(columns={
            'Plant Id':'plant_id',
            'NG_LCI_Name':'stage_code',
            'Stage':'stage'
            },inplace=True)
    ng_lci_basin_grouped['FlowName']=ng_lci_basin_grouped['FlowName'].map(_remove_brackets)
    return ng_lci_basin_grouped

if __name__=='__main__':
    year=2016
    df = generate_upstream_ng(year)
    df.to_csv(output_dir+'/ng_emissions_{}.csv'.format(year))
