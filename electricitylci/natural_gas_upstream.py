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

# Supporting Dicts
# #######################################################################################################
region_sheets_dict = {
    'Pacific': 'FI - Pacific Delivery',
    'Rocky Mountain': 'FI - Rocky Mountain Delivery',
    'Southwest': 'FI - Southwest Delivery',
    'Midwest': 'FI - Midwest Delivery',
    'Southeast': 'FI - Southeast Delivery',
    'Northeast': 'FI - Northeast Delivery'
 }

r_ids_2020 = {
    'Appendix_F_2020_Full_Inventory_Results_Midwest_ProdThruTrans.xlsx':'5665de40-fc2b-4643-b647-ceec226af2bb', 
    'Appendix_F_2020_Full_Inventory_Results_Northeast_ProdThruTrans.xlsx' :'b396eb50-72ac-45f0-8231-9b613457c6d8', 
    'Appendix_F_2020_Full_Inventory_Results_Pacific_ProdThruTrans.xlsx' :'347a0cd8-5ff2-4cb3-be0a-f31a56bac9c6', 
    'Appendix_F_2020_Full_Inventory_Results_Rocky_Mountain_ProdThruTrans.xlsx' :'d08f4da2-543a-40b2-9ffd-c7138ed4f8c6', 
    'Appendix_F_2020_Full_Inventory_Results_Southeast_ProdThruTrans.xlsx' :'4590712b-db21-4428-b488-6ded3b65d18b', 
    'Appendix_F_2020_Full_Inventory_Results_Southwest_ProdThruTrans.xlsx':'9dd7a6e5-df1a-461e-87e7-0b9d8d600f26'
}

region_state_mapping = {
    'WA':'Pacific','CA':'Pacific','OR':'Pacific','MT':'Rocky Mountain','ID':'Rocky Mountain','CO':'Rocky Mountain','NV':'Rocky Mountain','UT':'Rocky Mountain','WY':'Rocky Mountain',
    'AZ':'Southwest','NM':'Southwest','OK':'Southwest','TX':'Southwest','MN':'Midwest','ND':'Midwest','IA':'Midwest','KS':'Midwest',
    'MO':'Midwest','NE':'Midwest','SD':'Midwest','IL':'Midwest','IN':'Midwest','OH':'Midwest','WI':'Midwest','MI':'Midwest',
    'AR':'Southeast','LA':'Southeast','AL':'Southeast','FL':'Southeast','GA':'Southeast','MS':'Southeast','SC':'Southeast','KY':'Southeast',
    'NC':'Southeast','TN':'Southeast','VA':'Southeast','WV':'Southeast','DE':'Southeast','MD':'Southeast','CT':'Northeast','MA':'Northeast',
    'NH':'Northeast','RI':'Northeast','VT':'Northeast','NJ':'Northeast','NY':'Northeast','PA':'Northeast','ME':'Northeast',
} #TOTAL 48 -- EXCLUDING AL, HI, AND DC

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

    # get plant data and map each plant to its ng source: basin or region
    # the 2016 ng emissions inventory is only available by basin
    #   as such, plants can only be connected to upstream emissions via basin assignment
    # newer data (2020) is available by region 
    #   plants are connected to upstream ng emissions via region assignment

    if model_specs.ng_model_year == 2016:
        ng_generation_data_mapped = map_ng_by_basin(year) # 'year' refers to eia_generation_year
    else:
        ng_generation_data_mapped = map_ng_by_region(year) # 'year' refers to eia_generation_year

    # Read the NG LCI file
    # if year = 2016 - this step will directly ready NG_LCI.csv from the data_dir - returns lci (by basin)
    # if year = 2020 - this step will require edx api, download ng model and mapping - returns lci (by region)
    # document from edx, and generate lci
    ng_lci = get_ng_lci(model_specs.ng_model_year)

    # merge ng lci and plants based on the common parameter: region or basin
    if model_specs.ng_model_year == 2016:
        ng_lci_mapped = map_ng_lci_to_plants_by_basin(ng_lci, ng_generation_data_mapped)
    else:
        ng_lci_mapped = map_ng_lci_to_plants_by_region(ng_lci, ng_generation_data_mapped)

    # Multiplying with the EIA 923 fuel consumption; conversion factor is
    # for MMBtu to MJ
    btu_to_MJ = pq.convert(10**6,'Btu','MJ')
    ng_lci_mapped["FlowAmount"]=(
        ng_lci_mapped["FlowAmount"]
        * ng_lci_mapped['Total Fuel Consumption MMBtu']
        * btu_to_MJ
    )

    ng_lci_mapped = ng_lci_mapped.rename(
        columns={'Total Fuel Consumption MMBtu':'quantity'})
    ng_lci_mapped["quantity"]=ng_lci_mapped["quantity"]*btu_to_MJ

    # Output is kg emission for the specified year by facility Id,
    # not normalized to electricity output

    ng_lci_mapped['FuelCategory'] = 'GAS'
    ng_lci_mapped.rename(
        columns={
            'Plant Id':'plant_id',
            'NG_LCI_Region': 'stage_code',
            'NG_LCI_Name':'stage_code',
            'Stage':'stage'},
        inplace=True
    )
    ng_lci_mapped["Year"] = year
    ng_lci_mapped["Source"] = "netlgaseiafuel"
    ng_lci_mapped["ElementaryFlowPrimeContext"] = "emission"
    ng_lci_mapped.loc[
        ng_lci_mapped["Compartment"].str.contains("resource/"),
        "ElementaryFlowPrimeContext"] = "resource"
    ng_lci_mapped.loc[
        ng_lci_mapped["Compartment"].str.contains("Technosphere/"),
        "ElementaryFlowPrimeContext"] = "technosphere"
    # Issue #296 - adding DQI information for upstream processes
    ng_lci_mapped["Year"] = 2016
    ng_lci_mapped["DataReliability"] = 3
    ng_lci_mapped["TemporalCorrelation"] = add_temporal_correlation_score(
        ng_lci_mapped["Year"], model_specs.electricity_lci_target_year
    )
    ng_lci_mapped["GeographicalCorrelation"] = 1
    ng_lci_mapped["TechnologicalCorrelation"] = 1
    ng_lci_mapped["DataCollection"] = 1
    #3/20/2025 MBJ - replacing renewable vintage here so that temporal correlation
    #is based on the year the inventory is based on, but when electricity
    #generation is combined, it needs to be based on the target year for the
    #inventory.
    ng_lci_mapped["Year"]=year
    
    return ng_lci_mapped

##############################################################################
# HELPER FUNCTIONS
##############################################################################

def map_ng_lci_to_plants_by_basin (ng_lci, ng_generation_data_mapped):
    """
    Map the natural gas generation data by basin.
    """
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
    ng_lci_mapped = pd.merge(
        ng_lci_stack,
        ng_generation_data_mapped,
        left_on = 'Basin',
        right_on = 'NG_LCI_Name',
        how='left'
    )   
    return ng_lci_mapped

def map_ng_lci_to_plants_by_region (ng_lci, ng_generation_data_mapped):
    """
    Map the natural gas generation data by basin.
    """
    ng_lci_columns=[
        "Compartment",
        "FlowName",
        "FlowUUID",
        "Unit",
        "FlowType",
        "input",
        "Region",
        "FlowAmount"
    ]
    ng_lci_stack = pd.DataFrame(ng_lci.stack()).reset_index()
    ng_lci_stack.columns=ng_lci_columns

    # Merge basin data with LCI dataset
    ng_lci_mapped = pd.merge(
        ng_lci_stack,
        ng_generation_data_mapped,
        left_on = 'Region',
        right_on = 'NG_LCI_Region',
        how='left'
    )   
    return ng_lci_mapped


def map_ng_by_region (year):
    """
    Map the natural gas generation data by region.
    This includes 6 regions: Pacific, Rocky Mountain, Southwest, Midwest, Southeast, and Northeast.

    Notes
    -----
    * Downloads eia plant data for the specified year
    * Filters the data to only include NG facilities and on positive fuel consumption
    * Groups the data by Plant Id and aggregates the fuel consumption by summing the total fuel consumption
    * Maps each plant to a region using the region_state_mapping dictionary

    Parameters
    ----------
    year: int, str
        The year of the eia923 plant data to use.

    Returns
    ----------
    pandas.DataFrame
        A dataframe with the natural gas generation data by region.
    """
    if isinstance(year, str):
        year = int(year)
    
    eia_generation_data = eia923_download_extract(year)

    column_filt = ((eia_generation_data['Reported Fuel Type Code'] == 'NG') &
                   (eia_generation_data['Total Fuel Consumption MMBtu'] > 0))

    ng_generation_data = eia_generation_data[column_filt]

    ng_generation_data = ng_generation_data.groupby('Plant Id').agg(
        {'Total Fuel Consumption MMBtu':'sum','State':'first'}).reset_index()
    ng_generation_data['Plant Id'] = ng_generation_data['Plant Id'].astype(int)

    ng_generation_data_region = ng_generation_data.copy()

    ng_generation_data_region['NG_LCI_Region'] = ng_generation_data['State'].map(region_state_mapping)
    
    return ng_generation_data_region


def map_ng_by_basin (year):
    """
    Map the natural gas generation data by basin.

    Notes
    -----
    * Downloads eia plant data for the specified year
    * Filters the data to only include NG facilities and on positive fuel consumption
    * maps each plant to a basin using the gas_supply_basin_mapping.csv file
    
    Parameters
    ----------
    year: int, str
        The year of the eia923 plant data to use.

    Returns
    ----------
    pandas.DataFrame
        A dataframe with the natural gas generation data by region.
    """
    if isinstance(year, str):
        year = int(year)
    
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
    return ng_generation_data_basin

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
        # create new directory for ng if non existing
        if not os.path.exists(os.path.join(data_folder,"2020_ng")):
            os.makedirs(os.path.join(data_folder,"2020_ng"))
        data_folder = os.path.join(data_folder,"2020_ng")
        # check if the ng_lci_2020rev1.csv already exists - if it does then we can skip all the below
        if os.path.exists(os.path.join(data_folder, "ng_lci_2020rev1.csv")):
            logging.info(f"NG LCI already exists in your data directory.")
            ng_lci = pd.read_csv(
                os.path.join(data_folder, "ng_lci_2020rev1.csv"),
                index_col=[0,1,2,3,4,5]
            )
        else:
            # if it does not exist, then we need to generate it
            logging.info(f"Retrieving the {year} natural gas life cycle inventory by region.")
            # this step will require downloading files from edx      
            # retrieve ng model
            # check if model is data_dir
            if not os.path.exists(os.path.join(data_folder,"2020_ng_model")):
                os.makedirs(os.path.join(data_folder,"2020_ng_model"))
                model_folder = os.path.join(data_folder,"2020_ng_model")
            else:
                model_folder = os.path.join(data_folder,"2020_ng_model")
                for ngmodel in r_ids_2020.keys():
                    if os.path.exists(os.path.join(model_folder, ngmodel)):
                        logging.info(f"{ngmodel} already exists in your data directory.")
                    else:
                        logging.info(f"Downloading {ngmodel} from EDx.")
                        try:
                            download_edx(resource_id = r_ids_2020[ngmodel], api_key = model_specs.edx_api_key, output_dir = model_folder)
                        except Exception as e:
                            logging.error(f"Error downloading {ngmodel} from EDx. Error: {e}")
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
                    download_edx(resource_id = r_id_elci, api_key = model_specs.edx_api_key, output_dir = data_folder)
                    flow_mapping_path = os.path.join(data_folder, "elci.csv")  
                except Exception as e:
                    logging.error(f"Error downloading ELCI flow mapping document from EDx. Error: {e}")
                    sys.exit(1)

            # run the generate_ng_lci function and save it in data_dir
            try:
                generate_lci (excel_folder_path = model_folder, flow_mapping_path = flow_mapping_path, destination_path = data_folder, final_table_name = "ng_lci_2020rev1")
                ng_lci = pd.read_csv(
                    os.path.join(data_folder, "ng_lci_2020rev1.csv"),
                    index_col=[0,1,2,3,4,5]
                )
            except Exception as e:
                logging.error(f"Error generating natural gas life cycle inventory. Error: {e}")
                sys.exit(1)
    return ng_lci

def generate_lci(excel_folder_path, flow_mapping_path, destination_path, final_table_name):
    """
    This function reads an excel file, extracts the data, and generates a LCI for NG with the same format as the currently used file.

    Args:
        excel_folder_path (str): The path to the folder containing the excel files (ng models/inventories)
        flow_mapping_path (str): The path to the flow mapping file
        destination_path (str): !!This is an optional input!! 
                                The path to the destination folder. If not provided, the function 
                                will save the file in the current working directory.
        final_table_name (str): The name of the final table to be saved
                                Optional input. If not provided, the function will save the file with the name 'final_table.xlsx'.

    Returns:
        final_table (pd.DataFrame): A dataframe with the LCI for NG with the same format as the currently used file.

    Notes:
        - The function is senstive to the naming convention of the regions in the excel file.
    """
    final_table = pd.DataFrame()

    # determine folder path containing the excel files
    
    # 1. Read excel files in the folder path containing the model
    for filename in os.listdir(excel_folder_path):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(excel_folder_path, filename)
            logging.info(f"Reading file: {file_path}")
            input_data = pd.ExcelFile(file_path)
            sheet_names = input_data.sheet_names
            sheet_name = [name for name in sheet_names if name in region_sheets_dict.values()][0]

        # Extract air, water, and ground emissions data for the selected sheet (i.e., technobasin)
        air_emissions_data, water_emissions_data, ground_emissions_data = read_region_data(file_path, sheet_name)
        
        # Air emissions Get the correct flow names, compartment, and uuid for each flow
        full_air_emissions_data = correct_netl_flow_names(air_emissions_data, flow_mapping_path)
        full_air_emissions_data = full_air_emissions_data[full_air_emissions_data['FlowUUID'].notna()] # drop rows with FlowUUID NaN
        
        # Water emissions - get the correct flow names, compartment, and uuid for each flow
        full_water_emissions_data = correct_netl_flow_names(water_emissions_data, flow_mapping_path)
        full_water_emissions_data = full_water_emissions_data[full_water_emissions_data['FlowUUID'].notna()] # drop rows with FlowUUID NaN
        
        # Ground emissions - get the correct flow names, compartment, and uuid for each flow
        full_ground_emissions_data = correct_netl_flow_names(ground_emissions_data, flow_mapping_path)
        full_ground_emissions_data = full_ground_emissions_data[full_ground_emissions_data['FlowUUID'].notna()] # drop rows with FlowUUID NaN

        # combine dataframes
        df1 = pd.concat([full_air_emissions_data, full_water_emissions_data, full_ground_emissions_data])
        df1 = df1.sort_values(by='FlowUUID') # sort by FlowUUID
        region = [key for key, v in region_sheets_dict.items() if v == sheet_name][0]
        df1['FlowAmount'] = df1['FlowAmount'].astype(float)
        df1['FlowAmount'] = df1['FlowAmount'].fillna(0)

        # create final_table structure in 1st iteration
        if final_table.empty:
            final_table = df1[['FlowName', 'Compartment', 'Unit', 'input', 'FlowUUID']]
            final_table = final_table.sort_values(by='FlowUUID')
            final_table ['flow_type'] = 'ELEMENTARY_FLOW'
            #reorder and rename columns
            final_table = final_table[['Compartment', 'FlowName', 'FlowUUID', 'Unit', 'flow_type', 'input']]
            final_table.columns = ['compartment', 'flow_name', 'uuid', 'unit', 'flow_type', 'is_input']
            # add a column for each basin
            region_columns = list (region_sheets_dict.keys())
            for r in region_columns:
                final_table[r] = 0
        
        # add region emissions to final table   
        try:
            logging.info(f"Adding emissions for {region}")
            logging.info(f"df1: {df1['FlowAmount'].head(5)}")
            final_table[region] = df1['FlowAmount']
        except Exception as e:
            sys.exit(f"Error reading sheet. Make sure your excel file follows the correct naming convention.For reference, refer to the source code, lines 70-78. Error: {e}")

    # 2. Save final table to excel
    save_ng_lci(final_table, final_table_name ,destination_path)
    print(f"Final table saved to {destination_path}/{final_table_name}.xlsx")
    
    return final_table

def read_region_data(excel_file_path, sheet_name):
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
