#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# natural_gas_upstream.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os
import sys

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.eia923_generation import eia923_download_extract
import electricitylci.PhysicalQuantities as pq
from electricitylci.generation import add_temporal_correlation_score
from electricitylci.model_config import model_specs
from electricitylci.utils import download_edx
from electricitylci.globals import paths
##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses LCA emissions data to calculate the upstream
component of natural gas power plant operation (extraction, processing, and
transportation) for every plant in EIA-923.

Created:
    2019-02-18
Last updated:
    2025-10-15
"""
__all__ = [
    "generate_upstream_ng",
]


#############################################################################
# GLOBALS
##############################################################################
technobasins_basins = {
    'Appalachian': ['FI - App Shale'],
    'Alaska Offshore': ['FI - Alaska Offshore'],
    'Anadarko': ['FI - Anadarko Conv','FI - Anadarko Shale', 'FI - Anadarko Tight'],
    'Arkla': ['FI - Arkla Conv','FI - Arkla Shale','FI - Arkla Tight'],
    'Arkoma': ['FI - Arkoma Conv','FI - Arkoma Shale'],
    'East Texas': ['FI - East Texas Conv', 'FI - East Texas Shale', 'FI - East Texas Tight'],
    'Fort Worth': ['FI - Fort Worth Shale'],
    'Green River': ['FI - Green River Conv', 'FI - Green River Tight'],
    'Gulf': ['FI - Gulf Conv', 'FI - Gulf Shale', 'FI - Gulf TIght'], ## This not a typo - the title of the sheet in the excel file is 'FI - Gulf TIght'
    'Permian': ['FI - Permian Conv', 'FI - Permian Shale'],
    'Piceance': ['FI - Piceance Tight'],
    'San Juan': ['FI - San Juan CBM', 'FI - San Juan Shale'],
    'South Oklahoma': ['FI - South OK Shale'],
    'Strawn': ['FI - Strawn Shale'],
    'Uinta': ['FI - Uinta Conv', 'FI - Uinta Tight'],
    'GoM': ['FI - GoM Offshore']
}   

# Aliases to account for different naming conventions of technobasins used in the excel file
# the below dictionary is hardcoded

aliases = {
    'Appalachian Shale': 'FI - App Shale',
    'Alaska Offshore': 'FI - Alaska Offshore',
    'GoM Offshore': 'FI - GoM Offshore',
    'Arkla Shale': 'FI - Arkla Shale',
    'Arkla Tight': 'FI - Arkla Tight',
    'Green River Conv': 'FI - Green River Conv',
    'Green River Tight': 'FI - Green River Tight',
    'Permian Conv': 'FI - Permian Conv',
    'Gulf Tight': 'FI - Gulf TIght', ## This not a typo - the title of the sheet in the excel file is 'FI - Gulf TIght'
    'Uinta Conv': 'FI - Uinta Conv',
    'Gulf Conv': 'FI - Gulf Conv',
    'Gulf Shale': 'FI - Gulf Shale',
    'Permian Shale': 'FI - Permian Shale',
    'Anadarko Shale': 'FI - Anadarko Shale',
    'South Oklahoma Shale': 'FI - South OK Shale',
    'Uinta Tight': 'FI - Uinta Tight',
    'East Texas Tight': 'FI - East Texas Tight',
    'East Texas Shale': 'FI - East Texas Shale',
    'Strawn Shale': 'FI - Strawn Shale',
    'Piceance Tight': 'FI - Piceance Tight',
    'Fort Worth Shale': 'FI - Fort Worth Shale',
    'Arkla Conv': 'FI - Arkla Conv',
    'East Texas Conv': 'FI - East Texas Conv',
    'Arkoma Shale': 'FI - Arkoma Shale',
    'Anadarko Conv': 'FI - Anadarko Conv',
    'San Juan CBM': 'FI - San Juan CBM',
    'Anadarko Tight': 'FI - Anadarko Tight',
    'Arkoma Conv': 'FI - Arkoma Conv',
    'San Juan Shale': 'FI - San Juan Shale'
}

##############################################################################
# MAN FUNCTION
##############################################################################
def generate_upstream_ng(year):
    """
    Generate the annual gas extraction, processing and transportation
    emissions (in kg) for each plant in EIA923.

    Notes
    -----
    Depends on the data file, gas_supply_basin_mapping.csv, which includes the
    identification information for every natural gas plant in the U.S.
    Once imported, this data frame is simplified to contain just the plant
    code and its NG_LCI_Name.

    Also depends on the data file, NG_LCI.csv, which includes the LCA impact
    species determined for every natural gas basin in the U.S.
    Flows are separated by specific upstream process: production, gathering
    & boosting, processing, transmission, storage, and pipeline.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use.

    Returns
    ----------
    pandas.DataFrame
    """
    logging.info("Generating natural gas inventory")

    # Get the EIA generation data for the specified year, this dataset includes
    # the fuel consumption for generating electricity for each facility
    # and fuel type. Filter the data to only include NG facilities and on
    # positive fuel consumption. Group that data by Plant Id as it is possible
    # to have multiple rows for the same facility and fuel based on different
    # prime movers (e.g., gas turbine and combined cycle).
    eia_generation_data = eia923_download_extract(year)

    column_filt = ((eia_generation_data['Reported Fuel Type Code'] == 'NG') &
                   (eia_generation_data['Total Fuel Consumption MMBtu'] > 0))
    ng_generation_data = eia_generation_data[column_filt]

    ng_generation_data = ng_generation_data.groupby('Plant Id').agg(
        {'Total Fuel Consumption MMBtu':'sum'}).reset_index()
    ng_generation_data['Plant Id'] = ng_generation_data['Plant Id'].astype(int)

    # Import the mapping file which has the source gas basin for each Plant Id.
    # NOTE:
    #   This is a 2 MB file that provides about 100 kB of info!
    ng_basin_mapping = pd.read_csv(
        os.path.join(data_dir, 'gas_supply_basin_mapping.csv')
    )
    subset_cols = ['Plant Code', 'NG_LCI_Name']
    ng_basin_mapping = ng_basin_mapping[subset_cols]

    # Merge with ng_generation dataframe.
    ng_generation_data_basin = pd.merge(
        left = ng_generation_data,
        right = ng_basin_mapping,
        left_on = 'Plant Id',
        right_on = 'Plant Code'
    )
    ng_generation_data_basin = ng_generation_data_basin.drop(
        columns=['Plant Code']
    )

    # Read the NG LCI file
    # if year = 2016 - this step will directly ready NG_LCI.csv from the data_dir
    # if year = 2020 - this step will require edx api, download ng model and mapping 
    # document from edx, and generate lci
    ng_lci = get_ng_lci(model_specs.ng_model_year)

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

    # Merge basin data with LCI dataset
    ng_lci_basin = pd.merge(
        ng_lci_stack,
        ng_generation_data_basin,
        left_on = 'Basin',
        right_on = 'NG_LCI_Name',
        how='left'
    )

    # Multiplying with the EIA 923 fuel consumption; conversion factor is
    # for MMBtu to MJ
    btu_to_MJ = pq.convert(10**6,'Btu','MJ')
    ng_lci_basin["FlowAmount"]=(
        ng_lci_basin["FlowAmount"]
        * ng_lci_basin['Total Fuel Consumption MMBtu']
        * btu_to_MJ
    )

    ng_lci_basin = ng_lci_basin.rename(
        columns={'Total Fuel Consumption MMBtu':'quantity'})
    ng_lci_basin["quantity"]=ng_lci_basin["quantity"]*btu_to_MJ

    # Output is kg emission for the specified year by facility Id,
    # not normalized to electricity output

    ng_lci_basin['FuelCategory'] = 'GAS'
    ng_lci_basin.rename(
        columns={
            'Plant Id':'plant_id',
            'NG_LCI_Name':'stage_code',
            'Stage':'stage'},
        inplace=True
    )
    ng_lci_basin["Year"] = year
    ng_lci_basin["Source"] = "netlgaseiafuel"
    ng_lci_basin["ElementaryFlowPrimeContext"] = "emission"
    ng_lci_basin.loc[
        ng_lci_basin["Compartment"].str.contains("resource/"),
        "ElementaryFlowPrimeContext"] = "resource"
    ng_lci_basin.loc[
        ng_lci_basin["Compartment"].str.contains("Technosphere/"),
        "ElementaryFlowPrimeContext"] = "technosphere"
    # Issue #296 - adding DQI information for upstream processes
    ng_lci_basin["Year"] = 2016
    ng_lci_basin["DataReliability"] = 3
    ng_lci_basin["TemporalCorrelation"] = add_temporal_correlation_score(
        ng_lci_basin["Year"], model_specs.electricity_lci_target_year
    )
    ng_lci_basin["GeographicalCorrelation"] = 1
    ng_lci_basin["TechnologicalCorrelation"] = 1
    ng_lci_basin["DataCollection"] = 1
    #3/20/2025 MBJ - replacing renewable vintage here so that temporal correlation
    #is based on the year the inventory is based on, but when electricity
    #generation is combined, it needs to be based on the target year for the
    #inventory.
    ng_lci_basin["Year"]=year

    # Issue: the current basin-to-plant mapping document does not include the Alaska Offshore and GoM Offshore basins
    #        on the other hand, the ng_lci generated above includes emissions for both of there basins
    #        this causes NaN values in the 'ng_lci_basin' dataframe and then returns errors when converting to int32
    #        a quick fix involves omitting NaN values from the 'ng_lci_basin' dataframe - but this assumes that Offshore 
    #        gas production is not used in electricity production
    #        A fix for the future involves updating the mapping document: 'gas_supply_basin_mapping.csv' to account for 
    #        offshore gas used in electricity production
    
    ng_lci_basin = ng_lci_basin.dropna(subset=['FlowAmount'])
    
    return ng_lci_basin

##############################################################################
# HELPER FUNCTIONS
##############################################################################

def get_ng_lci(year):
    """
    Get the natural gas life cycle inventory for a given year.
    Depending on the year, the natural gas life cycle inventory is either:
        ** retrieved from existing data
        ** calculated using the natural gas life cycle inventory model 

    Parameters
    ----------
    year : str, int
        The year for which to get the natural gas life cycle inventory.
        This is retrieved from the model configuration
    
    Returns
    -------
    a dataframe containing the emissions associated with the natural gas 
    production through transportation for each basin during the given year.

    Notes
    -----
    This method depends on:
        ** the configuration parameter: ------------
        ** the NG_LCI csv file (if the old model is selected in the configuration)
        ** the EDx API (if the new model is selected in the configuration)
        ** the elci flow mapping csv file (if the new model is selected in the configuration)
    """
    if isinstance(year, int):
        year = str(year)
    if year == "2016":
        logging.info(f"Retrieving the 2016 natural gas life cycle inventory by basin.")
        ng_lci = pd.read_csv(
            os.path.join(data_dir, "NG_LCI.csv"),
            index_col=[0,1,2,3,4,5]
        )
    else:
        data_folder = os.path.join(paths.local_path, 'netl')
        #check if the ng_lci_2020rev1.csv already exists - if it does then we can skip all the below
        if os.path.exists(os.path.join(data_folder, "ng_lci_2020rev1.csv")):
            logging.info(f"NG LCI already exists in your data directory.")
            ng_lci = pd.read_csv(
                os.path.join(data_folder, "ng_lci_2020rev1.csv"),
                index_col=[0,1,2,3,4,5]
            )
        else:
            # if it does not exist, then we need to generate it
            logging.info(f"Retrieving the {year} natural gas life cycle inventory by basin.")
            # this step will require downloading files from edx      
            # retrieve ng model
            # check if model is data_dir
            if os.path.exists(os.path.join(data_folder, "ng_model_2020Rev1.xlsx")):
                logging.info(f"NG model already exists in your data directory.")
                excel_file_path = os.path.join(data_folder, "ng_model_2020Rev1.xlsx")
            else:
                # download model from edx
                logging.info(f"Downloading natural gas model from EDx.")
                edx_api = model_specs.edx_api_key
                r_id_ng_2020rev1 = 'cb8c8cf2-47ce-4ff0-b285-be73ba9294b9' 
                # resource id of 2020 Rev1 ng model on EDx
                try:
                    download_edx(resource_id = r_id_ng_2020rev1, api_key = edx_api, output_dir = data_folder)
                    excel_file_path = os.path.join(data_folder, "Appendix_F_2020_Full_Inventory_Results_US_Avg_ProdThruTrans.xlsx")
                except Exception as e:
                    logging.error(f"Error downloading natural gas model from EDx. Error: {e}")
                    sys.exit(1)
            # retrieve flow mapping document from edx [elci.csv]
            # check if flowmapping csv exists in data_dir
            if os.path.exists(os.path.join(data_folder, "elci.csv")):
                logging.info(f"ELCI flow mapping document already exists in your data directory.")
                flow_mapping_path = os.path.join(data_folder, "elci.csv")
            else:
                # download flowmapping document from edx
                logging.info(f"Downloading ELCI flow mapping document from EDx.")
                r_id_elci = 'e2c8f934-e95e-470a-879b-17ebe4afd39e' # resource id of elci flow mapping document on EDx
                try:
                    download_edx(resource_id = r_id_elci, api_key = edx_api, output_dir = data_folder)
                    flow_mapping_path = os.path.join(data_folder, "elci.csv")  
                except Exception as e:
                    logging.error(f"Error downloading ELCI flow mapping document from EDx. Error: {e}")
                    sys.exit(1)
            # production sheet name
            production_sheet_name = '2020 Production Shares'
            # run the generate_ng_lci function and save it in data_dir
            try:
                generate_lci (technobasins_basins, excel_file_path, flow_mapping_path, production_sheet_name, destination_path = data_folder, final_table_name = "ng_lci_2020rev1")
                ng_lci = pd.read_csv(
                    os.path.join(data_folder, "ng_lci_2020rev1.csv"),
                    index_col=[0,1,2,3,4,5]
                )
            except Exception as e:
                logging.error(f"Error generating natural gas life cycle inventory. Error: {e}")
                sys.exit(1)
    return ng_lci

def generate_lci(technobasins_basins, excel_file_path, flow_mapping_path, production_sheet_name, destination_path, final_table_name):
    """
    This function reads an excel file, extracts the data, and generates a LCI for NG with the same format as the currently used file.

    Args:
        technobasins_basins (dict): A dictionary that maps technobasins to basins
        excel_file_path (str): The path to the excel file
        production_sheet_name (str): The name of the sheet that contains the production shares
        destination_path (str): !!This is an optional input!! 
                                The path to the destination folder. If not provided, the function 
                                will save the file in the current working directory.
        final_table_name (str): The name of the final table to be saved
                                Optional input. If not provided, the function will save the file with the name 'final_table.xlsx'.

    Returns:
        final_table (pd.DataFrame): A dataframe with the LCI for NG with the same format as the currently used file.

    Notes:
        - The function is senstive to the naming convention of the technobasins in the excel file.
        - The current naming convention is: 'FI - <basin> <type>'. 
        - Specifically, the current script is set up for the following sheet names:
            - 'FI - App Shale', 'FI - Alaska Offshore', 'FI - Anadarko Conv', 'FI - Anadarko Shale', 'FI - Anadarko Tight', 
            'FI - Arkla Conv', 'FI - Arkla Shale', 'FI - Arkla Tight', 'FI - Arkoma Conv', 'FI - Arkoma Shale', 'FI - East Texas Conv', 
            'FI - East Texas Shale', 'FI - East Texas Tight', 'FI - Fort Worth Shale', 'FI - Green River Conv', 'FI - Green River Tight', 
            'FI - Gulf Conv', 'FI - Gulf Shale', 'FI - Gulf TIght', 'FI - Permian Conv', 'FI - Permian Shale', 'FI - Piceance Tight', 
            'FI - San Juan CBM', 'FI - San Juan Shale', 'FI - South OK Shale', 'FI - Strawn Shale', 'FI - Uinta Conv', 'FI - Uinta Tight', 
            'FI - GoM Offshore'
    """
    # 0. Develop dictionary for basin, technobasins, and production shares
    technobasins_basins = final_dictionary (technobasins_basins, excel_file_path, production_sheet_name)
    print(technobasins_basins)

    final_table = pd.DataFrame()

    # 1. Read excel file
    input_data = pd.ExcelFile(excel_file_path)
    sheet_names = input_data.sheet_names
    sheet_names = [name for name in sheet_names if name.startswith("FI")]
    sheet_names = sheet_names[1:] # Drop the US Average sheet

    # Get unused ground and water emissions based on average US emissions "FI - US Average"
    unused_ground_emissions, unused_water_emissions = get_unused_flows(excel_file_path, "FI - US Average")

    for sheet in sheet_names:
        # Extract air, water, and ground emissions data for the selected sheet (i.e., technobasin)
        air_emissions_data, water_emissions_data, ground_emissions_data = read_technobasin_data(excel_file_path, sheet)
        
        # Air emissions Get the correct flow names, compartment, and uuid for each flow
        full_air_emissions_data = correct_netl_flow_names(air_emissions_data, flow_mapping_path)
        full_air_emissions_data = full_air_emissions_data[full_air_emissions_data['FlowUUID'].notna()] # drop rows with FlowUUID NaN
        
        # Water emissions - drop unused flows
        if unused_water_emissions is not None:
            for flow in unused_water_emissions['FlowName']:
                water_emissions_data = water_emissions_data.drop(water_emissions_data[water_emissions_data['FlowName'] == flow].index)        
        # Water emissions - get the correct flow names, compartment, and uuid for each flow
        full_water_emissions_data = correct_netl_flow_names(water_emissions_data, flow_mapping_path)
        full_water_emissions_data = full_water_emissions_data[full_water_emissions_data['FlowUUID'].notna()] # drop rows with FlowUUID NaN
        
        # Ground emissions - drop unused flows
        if unused_ground_emissions is not None:
            for flow in unused_ground_emissions['FlowName']:
                ground_emissions_data = ground_emissions_data.drop(ground_emissions_data[ground_emissions_data['FlowName'] == flow].index)
        # Ground emissions - get the correct flow names, compartment, and uuid for each flow
        full_ground_emissions_data = correct_netl_flow_names(ground_emissions_data, flow_mapping_path)
        full_ground_emissions_data = full_ground_emissions_data[full_ground_emissions_data['FlowUUID'].notna()] # drop rows with FlowUUID NaN

        # combine dataframes
        df1 = pd.concat([full_air_emissions_data, full_water_emissions_data, full_ground_emissions_data])
        df1 = df1.sort_values(by='FlowUUID') # sort by FlowUUID
        basin_name = find_basin (technobasins_basins, sheet)
        df1['FlowAmount'] = df1['FlowAmount'].astype(float)
        df1['FlowAmount'] = df1['FlowAmount'].fillna(0)
        norm_value = get_normalized_values(technobasins_basins, sheet)
        df1['norm'] = norm_value
        df1['norm'] = df1['norm'].astype(float)
        df1['normalized_emissions'] = df1['FlowAmount'] * df1['norm']

        # create final_table structure in 1st iteration
        if final_table.empty:
            final_table = df1[['FlowName', 'Compartment', 'Unit', 'input', 'FlowUUID']]
            final_table = final_table.sort_values(by='FlowUUID')
            final_table ['flow_type'] = 'ELEMENTARY_FLOW'
            #reorder and rename columns
            final_table = final_table[['Compartment', 'FlowName', 'FlowUUID', 'Unit', 'flow_type', 'input']]
            final_table.columns = ['compartment', 'flow_name', 'uuid', 'unit', 'flow_type', 'is_input']
            # add a column for each basin
            basins_columns = list (technobasins_basins.keys())
            for basin in basins_columns:
                final_table[basin] = 0
        final_table.head()
        final_table.shape
        
        # Compute normalized emissions and add to final table   
        try:
            final_table['normalized_emissions'] = df1['normalized_emissions'].values
            final_table[basin_name] += final_table['normalized_emissions']
            final_table = final_table.drop(columns=['normalized_emissions'])
        except Exception as e:
            sys.exit(f"Error reading sheet. Make sure your excel file follows the correct naming convention.For reference, refer to the source code, lines 70-78. Error: {e}")


    # 2. Save final table to excel
    save_ng_lci(final_table, final_table_name ,destination_path)
    print(f"Final table saved to {destination_path}/{final_table_name}.csv")
    
    return final_table

def get_unused_flows(excel_file_path, sheet_name):
    """
    This function extracts the unused ground and water emissions from a given natural gas results dataset

    Inputs:
    - excel_file_path: path to the excel file
    - sheet_name: name of the sheet to extract the data from

    Outputs:
    - unused_ground_emissions: dataframe containing the unused ground emissions
    - unused_water_emissions: dataframe containing the unused water emissions
    """
    us_average_data = pd.read_excel(excel_file_path, sheet_name=sheet_name,skiprows=0,header=None) 
    us_average_data.iloc[0] = us_average_data.iloc[0].ffill()
    us_average_data.iloc[1] = us_average_data.iloc[1].ffill()
    us_average_data.columns = us_average_data.iloc[2]
    us_average_data = us_average_data.drop(columns=["P2.5", "P97.5"])
    us_average_data.columns = us_average_data.iloc[0]
    us_average_data = us_average_data.drop(us_average_data.index[0])
    #extract ground data from us_average sheet
    ground_emissions_data = us_average_data.iloc[:, [us_average_data.shape[1]-3, us_average_data.shape[1]-2]]
    ground_emissions_data.columns.values[0] = "FlowName"
    ground_emissions_data.columns.values[1] = "FlowAmount"
    ground_emissions_data = ground_emissions_data.dropna()
    ground_emissions_data = ground_emissions_data.iloc[1:]
    #extract water data from us_average sheet
    water_emissions_data = us_average_data.iloc[:, [us_average_data.shape[1]-3, us_average_data.shape[1]-1]]
    water_emissions_data.columns.values[0] = "FlowName"
    water_emissions_data.columns.values[1] = "FlowAmount"
    water_emissions_data = water_emissions_data.iloc[2:]
    water_emissions_data = water_emissions_data.dropna()
    #unused ground emissions
    unused_ground_emissions = ground_emissions_data[ground_emissions_data['FlowAmount'] == 0.00e+00]
    #unused water emissions
    unused_water_emissions = water_emissions_data[water_emissions_data['FlowAmount'] == 0.00e+00]

    return unused_ground_emissions, unused_water_emissions

def read_technobasin_data(excel_file_path, sheet_name):
    """
    This function reads an excel file, extracts the data, and generates a df for NG emissions for air, water, and ground.
    The df includes the flow name and flow amount (P2.5 and P97.5 values are dropped).

    Inputs:
    - excel_file_path: path to the excel file
    - sheet_name: name of the sheet to extract the data from

    Outputs:
    - air_emissions_data: dataframe containing the air emissions data
    - water_emissions_data: dataframe containing the water emissions data
    - ground_emissions_data: dataframe containing the ground emissions data
    """
    print(f"Processing sheet: {sheet_name}")
    # create empty database
    df = pd.DataFrame()
    # Extract all the data from the sheet
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, skiprows=0, header=None)
    # Adjustments: 1) changing header, 2) dropping P2.5 and P97.5 columns
    df.iloc[0] = df.iloc[0].ffill()
    df.iloc[1] = df.iloc[1].ffill()
    df.columns = df.iloc[2]
    df = df.drop(columns=["P2.5", "P97.5"])
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    # separate water, soil, ground, and air emissions - and map them to FEDEFL elementary flows
    # Air emissions
    air_emissions_data = df.drop(columns=[col for col in df.columns if col != df.columns[1]])  
    air_emissions_data = air_emissions_data.iloc[:, :-2]    # drop the last two columns (empty columns from excel)
    air_emissions_data[f'FlowAmount'] = air_emissions_data.iloc[:, 1:11].sum(axis=1)  # sum columns 2:11 for each row
    air_emissions_data = air_emissions_data.iloc[2:]
    air_emissions_data = air_emissions_data.iloc[:, [0,-1]]
    air_emissions_data['Compartment'] = 'Air' # add compartment
    air_emissions_data.columns.values[0] = 'FlowName' # change header
    air_emissions_data['Unit'] = 'kg' # add unit
    air_emissions_data ['input'] = False # add input
    # Water emissions
    water_emissions_data = df.iloc[:, [df.shape[1]-3, df.shape[1]-1]]
    water_emissions_data.columns.values[0] = "FlowName"
    water_emissions_data.columns.values[1] = "FlowAmount"
    water_emissions_data = water_emissions_data.iloc[2:]
    water_emissions_data = water_emissions_data.dropna()
    water_emissions_data['Compartment'] = 'Water'
    water_emissions_data['Unit'] = 'kg'
    water_emissions_data ['input'] = False
    # Ground emissions
    ground_emissions_data = df.iloc[:, [df.shape[1]-3, df.shape[1]-2]]
    ground_emissions_data.columns.values[0] = "FlowName"
    ground_emissions_data.columns.values[1] = "FlowAmount"
    ground_emissions_data = ground_emissions_data.dropna()
    ground_emissions_data = ground_emissions_data.iloc[1:]
    ground_emissions_data['Compartment'] = 'Ground'
    ground_emissions_data['Unit'] = 'kg'
    ground_emissions_data ['input'] = False

    return air_emissions_data, water_emissions_data, ground_emissions_data


# Helper function to calculate normalized values for each technobasin
def get_normalized_values(technobasins_basins, technobasin):
    for outer, inner in technobasins_basins.items():
        if technobasin in inner:
            total = sum(inner.values())
            return float(inner[technobasin] / total)
    return None

# helper function to find basins for a given technobasin
def find_basin(technobasins_basins, technobasin_name):
    for outer, inner in technobasins_basins.items():
        if technobasin_name in inner:
            return outer
    return None

# Helper function to use aliases to normalize technobasin naming
def _normalize_technobasin_naming(name):
    name_lower = name.lower().strip()
    
    # Check exact or partial match
    for alias, canonical in aliases.items():
        alias_clean = alias.lower()
        if name_lower in alias_clean or alias_clean in name_lower:
            return canonical

# Helper function to create the final dictionary including basin, technobasin, and production share
def final_dictionary(technobasins_basins, excel_file_path, production_sheet_name):
    production_shares_2020 = pd.read_excel(excel_file_path, sheet_name=production_sheet_name)
    production_shares_2020 = production_shares_2020.iloc[1:]
    production_shares_2020['Scenario Normalized'] = production_shares_2020['Scenario'].apply(lambda x: _normalize_technobasin_naming(x))
    production_shares_2020 = production_shares_2020.drop(columns=production_shares_2020.columns[0])
    production_shares_2020.columns.values[1] = 'Scenario'
    production_shares_2020 = production_shares_2020[['Scenario', 'Production Shares (%)']]
    # final dictionary including basin, technobasin, and production share
    technobasins_basins = {
        key: {num: production_shares_2020.set_index('Scenario').loc[num, 'Production Shares (%)'] for num in nums}
        for key, nums in technobasins_basins.items()
    }
    return technobasins_basins

def save_ng_lci(df, filename, destination_path):
    """
    This function saves the final table to an excel file.
    """
    if destination_path is None:
        destination_path = f"{os.getcwd()}/"
    if filename is None:
        filename = 'final_table'
    full_path = os.path.join(destination_path, f"{filename}.csv")
    df.to_csv(full_path, index=False)

def correct_netl_flow_names(df, flow_mapping_path, amount_col="FlowAmount"):
    """A helper method that replaces NETL air, water, and ground emissions
    with Federal Elementary Flow List equivalents based on a subset of
    flows defined in USEPA's eLCI mapping using the Python package
    `fedelemflowlist <https://github.com/USEPA/fedelemflowlist>`_

    Parameters
    ----------
    df : pandas.DataFrame
        A life cycle inventory data frame with columns, 'FlowName',
        'Compartment', 'Unit', and ``amount_col``.
    amount_col : str, optional
        The column title representing the flow amount, by default "FlowAmount"

    Returns
    -------
    pandas.DataFrame
        A new data frame with the same number of rows and columns as the
        sent data frame. Flow names, compartments, units, and flow amounts
        are updated based on emissions matches with the FEDEFL. All unmatched
        flows are returned 'as is'. If FlowUUID was not in the column list,
        it is created; otherwise, the matched UUIDs are updated.
    """
    # This data frame has about 4k source flow names and contexts associated
    # with NETL unit process models (e.g., petro, nuclear, coal).
    flow_mapping = pd.read_csv(flow_mapping_path, encoding='ISO-8859-1')

    # Matching occurs on name, compartment and units; help this along by
    # lowering the case (improves coal UP matches from 10% to 42%).
    df["FlowName_orig"] = df["FlowName"]
    df["Compartment_orig"] = df["Compartment"]
    df["FlowName"] = df["FlowName"].str.lower().str.rstrip()
    df["Compartment"] = df["Compartment"].str.lower().str.rstrip()

    # In the map, also lower-case names and compartments and remove trailing
    # space; note this introduces duplicate entries in the map, so remove them.
    # The duplicates are from later entries, so ignore mapper, verifier and
    # last updated cols when searching for duplicates. [250917; TWD]
    flow_mapping['SourceFlowName'] = flow_mapping[
        'SourceFlowName'].str.lower().str.rstrip()
    flow_mapping['SourceFlowContext'] = flow_mapping[
        "SourceFlowContext"].str.lower().str.rstrip()
    ignore_cols = ['Mapper', 'Verifier', 'LastUpdated']
    flow_mapping = flow_mapping.drop_duplicates(
        subset=[x for x in flow_mapping.columns if x not in ignore_cols]
    )

    # Some compartments in NETL UPs are complex (e.g., 'Emission to water/fresh
    # water'), but are listed simply in the FEDEFL eLCI mapper (e.g., 'emission/
    # water'). Improves coal mining UP matches from 42% to 62%.
    is_emission = df['input'] == False
    is_water = df['Compartment'].str.contains('water')
    is_air = df['Compartment'].str.contains('air')
    is_ground = df['Compartment'].str.contains('ground')

    df.loc[is_emission * is_water, 'Compartment'] = 'emission/water'
    df.loc[is_emission * is_air, 'Compartment'] = 'emission/air'
    df.loc[is_emission * is_ground, 'Compartment'] = 'emission/ground'

    # HOTFIX: Map against source units [250205; TWD]
    # For coal mining, reduces matches from >62% to <62% (about 2k less rows)
    logging.info("Mapping emissions to FEDEFL")
    mapped_df = pd.merge(
        df,
        flow_mapping,
        left_on=["FlowName", "Compartment", "Unit"],
        right_on=["SourceFlowName", "SourceFlowContext", "SourceUnit"],
        how="left",
    )

    # If TargetFlowName is present, there was a match.
    is_match = mapped_df["TargetFlowName"].notnull()
    logging.info("Correcting %d NETL flows" % is_match.sum())

    # Quality Check (coal_df)
    #   Check that target unit matches source unit.
    #   No! Hydrogen, Uranium, and Lead-210/kg have mis-matched units.
    #   Therefore, unit conversions are necessary.

    # Return flow names and compartments back to their original values.
    df["FlowName"] = df["FlowName_orig"]
    df["Compartment"] = df["Compartment_orig"]
    del df['FlowName_orig']      # use this syntax since you're editing
    del df['Compartment_orig']   # a reference object that isn't returned
    mapped_df['FlowName'] = mapped_df['FlowName_orig']
    mapped_df["Compartment"] = mapped_df["Compartment_orig"]
    mapped_df = mapped_df.drop(columns=['FlowName_orig', 'Compartment_orig'])

    # Replace FlowName, Unit, and Compartment with new names (where matched)
    mapped_df.loc[is_match, "FlowName"] = mapped_df.loc[
        is_match, "TargetFlowName"]
    mapped_df.loc[is_match, "Compartment"] = mapped_df.loc[
        is_match, "TargetFlowContext"]
    mapped_df.loc[is_match, "Unit"] = mapped_df.loc[is_match, "TargetUnit"]

    # Correct values using the conversion factor
    mapped_df.loc[is_match, amount_col] *= mapped_df.loc[
        is_match, 'ConversionFactor']

    if 'FlowUUID' in mapped_df.columns:
        # Update existing values with new UUIDs
        mapped_df.loc[is_match, 'FlowUUID'] = mapped_df.loc[
            is_match, 'TargetFlowUUID']
    else:
        # Set UUIDs to target values
        mapped_df = mapped_df.rename(columns={"TargetFlowUUID": "FlowUUID"})

    # Drop all unneeded cols
    drop_cols = [x for x in flow_mapping.columns if x in mapped_df.columns]
    mapped_df = mapped_df.drop(columns=drop_cols)

    return mapped_df

##############################################################################
# MAIN
##############################################################################
if __name__=='__main__':
    from electricitylci.globals import output_dir
    year=2016
    df = generate_upstream_ng(year)
    df.to_csv(output_dir+'/ng_emissions_{}.csv'.format(year))
