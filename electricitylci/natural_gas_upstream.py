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
import electricitylci.PhysicalQuantities as pq
import logging

module_logger = logging.getLogger(name="natural_gas_upstream.py")

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
    module_logger.info("Generating natural gas inventory")
    #Get the EIA generation data for the specified year, this dataset includes
    #the fuel consumption for generating electricity for each facility
    #and fuel type. Filter the data to only include NG facilities and on
    #positive fuel consumption. Group that data by Plant Id as it is possible
    #to have multiple rows for the same facility and fuel based on different
    #prime movers (e.g., gas turbine and combined cycle).
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
    ng_lci = pd.read_csv(data_dir + '/NG_LCI.csv',index_col=[0,1,2,3,4,5])
            #sheet_name = 'Basin_Mean_Data')
    ng_lci_columns=[
            "Compartment",
            "FlowName",
            "FlowUUID",
            "Unit",
            "FlowType",
            "input",
            "Basin",
            "FlowAmount"
            ]
    ng_lci_stack = pd.DataFrame(ng_lci.stack()).reset_index()
    ng_lci_stack.columns=ng_lci_columns

    #Merge basin data with LCI dataset
    ng_lci_basin = pd.merge(
            ng_lci_stack,
            ng_generation_data_basin,
            left_on = 'Basin',
            right_on = 'NG_LCI_Name',
            how='left')


    #Multiplying with the EIA 923 fuel consumption; conversion factor is
    #for MMBtu to MJ
    btu_to_MJ=pq.convert(10**6,'Btu','MJ')
    ng_lci_basin["FlowAmount"]=(
            ng_lci_basin["FlowAmount"]
            * ng_lci_basin['Total Fuel Consumption MMBtu']
            * btu_to_MJ
    )

    ng_lci_basin = ng_lci_basin.rename(columns=
            {'Total Fuel Consumption MMBtu':'quantity'})
    ng_lci_basin["quantity"]=ng_lci_basin["quantity"]*btu_to_MJ
    #Output is kg emission for the specified year by facility Id,
    #not normalized to electricity output


    ng_lci_basin['FuelCategory']='GAS'
    ng_lci_basin.rename(columns={
            'Plant Id':'plant_id',
            'NG_LCI_Name':'stage_code',
            'Stage':'stage'
            },inplace=True)
    ng_lci_basin["Year"]=year
    ng_lci_basin["Source"]="netl"
    ng_lci_basin["ElementaryFlowPrimeContext"]="emission"
    ng_lci_basin.loc[ng_lci_basin["Compartment"].str.contains("resource/"),"ElementaryFlowPrimeContext"]="resource"
    ng_lci_basin.loc[ng_lci_basin["Compartment"].str.contains("Technosphere/"),"ElementaryFlowPrimeContext"]="technosphere"
    return ng_lci_basin

if __name__=='__main__':
    year=2016
    df = generate_upstream_ng(year)
    df.to_csv(output_dir+'/ng_emissions_{}.csv'.format(year))
