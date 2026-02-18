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

import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.globals import paths
from electricitylci.eia923_generation import eia923_download_extract
import electricitylci.PhysicalQuantities as pq
from electricitylci.generation import add_temporal_correlation_score
from electricitylci.model_config import model_specs
from electricitylci.elementaryflows import correct_netl_flow_names
from electricitylci.utils import download_edx
from electricitylci.utils import check_output_dir


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses LCA emissions data to calculate the upstream
component of natural gas power plant operation (extraction, processing, and
transportation) for every plant in EIA-923.

Created:
    2019-02-18
Last updated:
    2026-01-29
"""
__all__ = [
    "generate_lci",
    "generate_upstream_ng",
    "get_ng_lci",
    "map_ng_by_basin",
    "map_ng_by_region",
    "map_ng_lci_to_plants_by_basin",
    "map_ng_lci_to_plants_by_region",
    "read_region_data",
    "save_ng_lci",
]


##############################################################################
# GLOBALS
##############################################################################
REGION_SHEETS_DICT = {
    'Pacific': 'FI - Pacific Delivery',
    'Rocky Mountain': 'FI - Rocky Mountain Delivery',
    'Southwest': 'FI - Southwest Delivery',
    'Midwest': 'FI - Midwest Delivery',
    'Southeast': 'FI - Southeast Delivery',
    'Northeast': 'FI - Northeast Delivery'
 }
'''dict : Region names mapped to Excel workbook sheet names.'''

R_IDS_2020 = {
    'Appendix_F_2020_Full_Inventory_Results_Midwest_ProdThruTrans.xlsx':'5665de40-fc2b-4643-b647-ceec226af2bb',
    'Appendix_F_2020_Full_Inventory_Results_Northeast_ProdThruTrans.xlsx' :'b396eb50-72ac-45f0-8231-9b613457c6d8',
    'Appendix_F_2020_Full_Inventory_Results_Pacific_ProdThruTrans.xlsx' :'347a0cd8-5ff2-4cb3-be0a-f31a56bac9c6',
    'Appendix_F_2020_Full_Inventory_Results_Rocky_Mountain_ProdThruTrans.xlsx' :'d08f4da2-543a-40b2-9ffd-c7138ed4f8c6',
    'Appendix_F_2020_Full_Inventory_Results_Southeast_ProdThruTrans.xlsx' :'4590712b-db21-4428-b488-6ded3b65d18b',
    'Appendix_F_2020_Full_Inventory_Results_Southwest_ProdThruTrans.xlsx':'9dd7a6e5-df1a-461e-87e7-0b9d8d600f26'
}
'''dict : Excel workbook file names mapped to EDX resource IDs.'''

REGION_STATE_MAPPING = {
    'WA':'Pacific','CA':'Pacific','OR':'Pacific','MT':'Rocky Mountain','ID':'Rocky Mountain','CO':'Rocky Mountain','NV':'Rocky Mountain','UT':'Rocky Mountain','WY':'Rocky Mountain',
    'AZ':'Southwest','NM':'Southwest','OK':'Southwest','TX':'Southwest','MN':'Midwest','ND':'Midwest','IA':'Midwest','KS':'Midwest',
    'MO':'Midwest','NE':'Midwest','SD':'Midwest','IL':'Midwest','IN':'Midwest','OH':'Midwest','WI':'Midwest','MI':'Midwest',
    'AR':'Southeast','LA':'Southeast','AL':'Southeast','FL':'Southeast','GA':'Southeast','MS':'Southeast','SC':'Southeast','KY':'Southeast',
    'NC':'Southeast','TN':'Southeast','VA':'Southeast','WV':'Southeast','DE':'Southeast','MD':'Southeast','CT':'Northeast','MA':'Northeast',
    'NH':'Northeast','RI':'Northeast','VT':'Northeast','NJ':'Northeast','NY':'Northeast','PA':'Northeast','ME':'Northeast', 'DC':'Northeast',
}
'''dict : U.S. state abbreviations mapped to region. Excludes AK and HI.'''


##############################################################################
# FUNCTIONS
##############################################################################
def generate_lci(excel_folder_path,
                 destination_path,
                 final_table_name):
    """
    Read Excel file, extract data, and generate NG LCI in the correct format.

    Parameters
    ----------
    excel_folder_path : str
        The path to the folder containing the excel files (i.e., NG models and
        inventories).
    destination_path : str, optional
        The path to the destination folder. If not provided, the function
        will save the file in the current working directory.
    final_table_name : str, optional
        The name of the final table to be saved. If not provided, the function
        will save the file with the name 'final_table.xlsx'.

    Returns
    -------
    pandas.DataFrame
        A dataframe with the LCI for NG with the same format as the currently
        used file.

    Notes
    -----
    The function is sensitive to the naming convention of the regions in the
    Excel file.

    Updated to utilize the :func:`correct_netl_flow_names` found in
    elementaryflows.py.
    """
    final_table = pd.DataFrame()

    # 1. Read excel files in the folder path containing the model
    for filename in os.listdir(excel_folder_path):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(excel_folder_path, filename)
            logging.info(f"Reading file: {file_path}")
            input_data = pd.ExcelFile(file_path)
            sheet_names = input_data.sheet_names
            try:
                sheet_name = [
                    name for name in sheet_names if name in REGION_SHEETS_DICT.values()
                ][0]
            except IndexError as e:
                err_str = (
                    "Failed to find named worksheet in %s. Check your "
                    "workbook against REGION_SHEETS_DICT keys." % filename
                )
                logging.error(err_str)
                raise IndexError(err_str)

        # Extract air, water, and ground emissions data for the selected sheet
        air_emissions_data, water_emissions_data, ground_emissions_data = read_region_data(file_path, sheet_name)

        # Air emissions
        # - Get the correct flow names, compartment, and uuid for each flow
        logging.info("Processing natural gas air emissions")
        full_air_emissions_data = correct_netl_flow_names(air_emissions_data)
        # Drop rows with FlowUUID NaN.
        full_air_emissions_data = full_air_emissions_data[
            full_air_emissions_data['FlowUUID'].notna()
        ]

        # Water emissions
        # - get the correct flow names, compartment, and uuid for each flow.
        logging.info("Processing natural gas water emissions")
        full_water_emissions_data = correct_netl_flow_names(
            water_emissions_data
        )
        # Drop rows with FlowUUID NaN.
        full_water_emissions_data = full_water_emissions_data[
            full_water_emissions_data['FlowUUID'].notna()
        ]

        # Ground emissions
        # - get the correct flow names, compartment, and uuid for each flow
        logging.info("Processing natural gas ground emissions")
        full_ground_emissions_data = correct_netl_flow_names(
            ground_emissions_data
        )
        full_ground_emissions_data = full_ground_emissions_data[
            full_ground_emissions_data['FlowUUID'].notna()
        ]

        # Combine dataframes.
        df1 = pd.concat([
            full_air_emissions_data,
            full_water_emissions_data,
            full_ground_emissions_data
        ])
        df1 = df1.sort_values(by='FlowUUID')
        region = [
            key for key, v in REGION_SHEETS_DICT.items() if v == sheet_name
        ][0]
        df1['FlowAmount'] = df1['FlowAmount'].astype(float)
        df1['FlowAmount'] = df1['FlowAmount'].fillna(0)

        # Create final_table structure in 1st iteration.
        if final_table.empty:
            final_table = df1[[
                'FlowName', 'Compartment', 'Unit', 'input', 'FlowUUID'
            ]]
            final_table = final_table.sort_values(by='FlowUUID')
            final_table ['flow_type'] = 'ELEMENTARY_FLOW'
            # Reorder and rename columns.
            final_table = final_table[[
                'Compartment',
                'FlowName',
                'FlowUUID',
                'Unit',
                'flow_type',
                'input'
            ]]
            final_table.columns = [
                'compartment',
                'flow_name',
                'uuid',
                'unit',
                'flow_type',
                'is_input'
            ]
            # Add a column for each basin
            region_columns = list(REGION_SHEETS_DICT.keys())
            for r in region_columns:
                final_table[r] = 0

        # Add region emissions to final table
        try:
            logging.info(f"Adding emissions for {region}")
            logging.debug(f"df1: {df1['FlowAmount'].head(5)}")
            final_table[region] = df1['FlowAmount']
        except Exception as e:
            err_str = (
                "Error reading sheet. "
                "Make sure your excel file follows the correct naming "
                "convention. For reference, refer to the source code, "
                f"lines 70-78. Error: {e}"
            )
            logging.error(err_str)
            raise IOError(err_str)

    # 2. Save final table to excel
    save_ng_lci(final_table, final_table_name ,destination_path)
    logging.info(
        f"Final table saved to {destination_path}/{final_table_name}.xlsx"
    )

    return final_table


def generate_upstream_ng(year):
    """
    Generate the annual gas extraction, processing and transportation
    emissions (in kg) for each plant in EIA923.

    Notes
    -----
    This is the main method called outside this module.

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

    # Get plant data and map each plant to its ng source: basin or region.
    # The 2016 ng emissions inventory is only available by basin.
    # As such, plants can only be connected to upstream emissions via basin
    # assignment newer data (2020) is available by region plants are connected
    # to upstream ng emissions via region assignment

    # NOTE: 'year' parameter refers to eia_gen_year
    if model_specs.ng_model_year == 2016:
        ng_generation_data_mapped = map_ng_by_basin(year)
    else:
        ng_generation_data_mapped = map_ng_by_region(year)

    # Read the NG LCI file
    # If year = 2016
    # - this step will directly ready NG_LCI.csv from the data_dir
    # - returns LCI (by basin)
    # If year = 2020
    # - this step requires EDX API (to download ng model) and mapping
    # - returns LCI (by region)
    ng_lci = get_ng_lci(model_specs.ng_model_year)

    # merge ng lci and plants based on the common parameter: region or basin
    if model_specs.ng_model_year == 2016:
        ng_lci_mapped = map_ng_lci_to_plants_by_basin(
            ng_lci, ng_generation_data_mapped
        )
    else:
        ng_lci_mapped = map_ng_lci_to_plants_by_region(
            ng_lci, ng_generation_data_mapped
        )

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
    ng_lci_mapped["quantity"] = ng_lci_mapped["quantity"]*btu_to_MJ

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
    ng_lci_mapped["Year"] = model_specs.ng_model_year
    ng_lci_mapped["DataReliability"] = 3
    ng_lci_mapped["TemporalCorrelation"] = add_temporal_correlation_score(
        ng_lci_mapped["Year"], model_specs.electricity_lci_target_year
    )
    ng_lci_mapped["GeographicalCorrelation"] = 1
    ng_lci_mapped["TechnologicalCorrelation"] = 1
    ng_lci_mapped["DataCollection"] = 1

    # 3/20/2025 MBJ - replacing renewable vintage here so that temporal
    # correlation is based on the year the inventory is based on, but when
    # electricity generation is combined, it needs to be based on the target
    # year for the inventory.
    ng_lci_mapped["Year"] = year

    return ng_lci_mapped


def get_ng_lci(year):
    """
    Get the natural gas life cycle inventory for a given year.
    Depending on the year, the natural gas life cycle inventory is either:

    - retrieved from existing data (e.g., 2016)
    - calculated using the natural gas life cycle inventory model (e.g., 2020)

    Parameters
    ----------
    year : str, int
        The year for which to get the natural gas life cycle inventory.
        This should reflect the model configuration, ``ng_model_year``.

    Returns
    -------
    pandas.DataFrame
        A dataframe containing the emissions associated with the natural gas
        production through transportation for each basin during the given year.

    Notes
    -----
    This method depends on:

    -   the NG_LCI CSV file (if the old model is selected in the configuration)
    -   the EDX API (if the new model is selected in the configuration)
    -   the eLCI flow mapping CSV file (if the new model is selected in the
        configuration)
    """
    if isinstance(year, int):
        year = str(year)
    if year == "2016":
        logging.info(
            f"Retrieving the 2016 natural gas life cycle inventory by basin."
        )
        ng_lci = pd.read_csv(
            os.path.join(data_dir, "NG_LCI.csv"),
            index_col=[0,1,2,3,4,5]
        )
    else:
        data_folder = os.path.join(paths.local_path, 'netl')
        # Create new directory for ng if non existing.
        check_output_dir(os.path.join(data_folder,"2020_ng"))
        data_folder = os.path.join(data_folder,"2020_ng")
        # Check if the ng_lci_2020rev1.csv already exists
        # - if it does then we can skip all the below
        if os.path.exists(os.path.join(data_folder, "ng_lci_2020rev1.csv")):
            logging.info("NG LCI already exists in your data directory.")
            ng_lci = pd.read_csv(
                os.path.join(data_folder, "ng_lci_2020rev1.csv"),
                index_col=[0,1,2,3,4,5]
            )
        else:
            # If it does not exist, then generate it.
            logging.info(
                f"Retrieving the {year} natural gas life cycle inventory "
                "by region."
            )
            # This step will require downloading files from EDX.
            # - retrieve ng model
            # - check if model is data_dir
            check_output_dir(os.path.join(data_folder, "2020_ng_model"))
            model_folder = os.path.join(data_folder, "2020_ng_model")
            for ngmodel in R_IDS_2020.keys():
                if os.path.exists(os.path.join(model_folder, ngmodel)):
                    logging.info(
                        f"{ngmodel} already exists in your data directory."
                    )
                else:
                    logging.info(f"Downloading {ngmodel} from EDX.")
                    try:
                        download_edx(
                            resource_id=R_IDS_2020[ngmodel],
                            api_key=model_specs.edx_api_key,
                            output_dir=model_folder
                        )
                    except Exception as e:
                        logging.error(
                            f"Error downloading {ngmodel} from EDX. Error: {e}"
                        )

            # Run the generate_ng_lci function and save it in data_dir.
            try:
                generate_lci(
                    excel_folder_path=model_folder,
                    destination_path=data_folder,
                    final_table_name="ng_lci_2020rev1"
                )
                ng_lci = pd.read_csv(
                    os.path.join(data_folder, "ng_lci_2020rev1.csv"),
                    index_col=[0,1,2,3,4,5]
                )
            except Exception as e:
                err_str = (
                    "Error generating natural gas life cycle inventory. "
                    f"Error: {e}"
                )
                logging.error(err_str)
                raise IOError(err_str)
    return ng_lci


def map_ng_by_basin(year):
    """
    Map the natural gas generation data by basin.

    Notes
    -----
    -   Downloads EIA plant data for the specified year.
    -   Filters the data to only include NG facilities and on positive fuel
        consumption.
    -   Maps each plant to a basin using the gas_supply_basin_mapping.csv file.

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
    # NOTE: This is a 2 MB file that provides about 100 kB of info!
    # TODO: Move this resource to EDX.
    ng_basin_mapping = pd.read_csv(
        os.path.join(data_dir, 'gas_supply_basin_mapping.csv')
    )
    subset_cols = ['Plant Code', 'NG_LCI_Name']
    ng_basin_mapping = ng_basin_mapping[subset_cols]

    # Merge with ng_generation dataframe.
    ng_generation_data_basin = pd.merge(
        left=ng_generation_data,
        right=ng_basin_mapping,
        left_on='Plant Id',
        right_on='Plant Code'
    )
    ng_generation_data_basin = ng_generation_data_basin.drop(
        columns=['Plant Code']
    )
    return ng_generation_data_basin


def map_ng_by_region(year):
    """
    Map the natural gas inventory data by downstream delivery region.
    This includes six regions: Pacific, Rocky Mountain, Southwest, Midwest,
    Southeast, and Northeast.

    Notes
    -----
    -   Downloads EIA plant data for the specified year.
    -   Filters the data to only include NG facilities and on positive fuel
        consumption.
    -   Groups the data by Plant Id and aggregates the fuel consumption by
        summing the total fuel consumption.
    -   Maps each plant to a region using the REGION_STATE_MAPPING dictionary.

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

    ng_generation_data_region['NG_LCI_Region'] = ng_generation_data[
        'State'].map(REGION_STATE_MAPPING)

    return ng_generation_data_region


def map_ng_lci_to_plants_by_basin(ng_lci, ng_generation_data_mapped):
    """
    Map the natural gas inventory data by upstream production technobasin.
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
    ng_lci_stack.columns = ng_lci_columns

    # Merge basin data with LCI dataset
    ng_lci_mapped = pd.merge(
        ng_lci_stack,
        ng_generation_data_mapped,
        left_on='Basin',
        right_on='NG_LCI_Name',
        how='left'
    )
    return ng_lci_mapped


def map_ng_lci_to_plants_by_region(ng_lci, ng_generation_data_mapped):
    """
    Map the natural gas generation data by basin.
    """
    ng_lci_columns = [
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
    ng_lci_stack.columns = ng_lci_columns

    # Merge basin data with LCI dataset
    ng_lci_mapped = pd.merge(
        ng_lci_stack,
        ng_generation_data_mapped,
        left_on='Region',
        right_on='NG_LCI_Region',
        how='left'
    )
    return ng_lci_mapped


def _proc_region_data(df):
    """Implementation of DRY (don't repeat yourself) for repetitive methods
    applied to air, water, and ground emissions data frames in
    :func:`read_region_data`. This method sets the column headers to the
    LCA stages, drops the LCA stages and stats rows, which are text-based,
    drops rows that are all NaNs (e.g., with soil and water), then reconstructs
    the data frame so that the emissions columns are numeric (floats) and the
    emissions names are strings (or objects), complying with new pandas 3.0.

    Parameters
    ----------
    df : pandas.DataFrame
        A natural gas compartmentalized emissions table (e.g., air, water,
        or soil), as created within :func:`read_region_data`

    Returns
    -------
    pandas.DataFrame
        The same data frame that was received. See description for what's
        changed.
    """
    # Set columns to unique headers (lca stages)
    df.columns = df.iloc[0]

    # Drop the textual rows (i.e., the stage names and statistic name).
    df = df.drop(df.index[0:2])

    # Remove lingering rows with NaNs (thanks, Excel)
    df = df.dropna(how='all')

    # Reconstruct the data frame with emission names and floating point values
    df = pd.concat(
        [
            df.iloc[:, [0]],
            df.iloc[:, 1:].astype(float)
        ],
        axis=1
    )
    return df


def read_region_data(excel_file_path, sheet_name):
    """
    Read Excel file, extract data, and generate a data frame for NG emissions
    for air, water, and ground. The data frame includes the flow name and flow
    amount (P2.5 and P97.5 values are dropped).

    Parameters
    ----------
    excel_file_path : str
        Path to the Excel file.
    sheet_name : str
        Name of the sheet to extract the data from.

    Returns
    -------
    tuple
        A tuple of length three:

        - pandas.DataFrame, the air emissions data
        - pandas.DataFrame, the water emissions data
        - pandas.DataFrame, the ground emissions data
    """
    logging.info(f"Processing sheet: {sheet_name}")
    # Extract all the data from the sheet
    df = pd.read_excel(
        excel_file_path,
        sheet_name=sheet_name,
        skiprows=0,
        header=None
    )

    # HOTFIX: set all column data types to str for pandas 3.0 [26.01.30;TWD]
    df = df.astype(str)

    # HOTFIX: in pandas 2.3, NaNs become literal 'nan', which won't ffill()!
    df = df.replace('nan', None)

    # Adjustments: 1) change header, 2) drop all P2.5 and P97.5 columns
    df.iloc[0] = df.iloc[0].ffill()
    df.iloc[1] = df.iloc[1].ffill()
    df.columns = df.iloc[2]
    df = df.drop(columns=["P2.5", "P97.5"])
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Separate water, soil, ground, and air emissions - and map them to
    # FEDEFL elementary flows

    # Air emissions
    # Drop all columns that aren't labeled as 'air' (Cell A1)
    air_emissions_data = df.drop(
        columns=[col for col in df.columns if col != df.columns[1]]
    )
    # Drop the last two columns (empty columns from excel)
    air_emissions_data = air_emissions_data.iloc[:, :-2]

    # At this point, we have air emissions where the first column is the
    # emission name, and the following columns are the mean amounts across
    # the life cycle stages (e.g., production, gathering, processing, storage)

    air_emissions_data = _proc_region_data(air_emissions_data)

    # Sum across columns for each row
    air_emissions_data[f'FlowAmount'] = air_emissions_data.iloc[:, 1:].sum(
        axis=1
    )

    # Save only 'Emission' and 'FlowAmount' columns.
    air_emissions_data = air_emissions_data.iloc[:, [0,-1]]

    # Add metadata
    air_emissions_data['Compartment'] = 'Air'
    air_emissions_data.columns.values[0] = 'FlowName' # change header
    air_emissions_data['Unit'] = 'kg'
    air_emissions_data['input'] = False # not an input

    # Water emissions
    # Grab two of the last three columns (emissions and water amounts)
    water_emissions_data = df.iloc[:, [df.shape[1]-3, df.shape[1]-1]]
    water_emissions_data = _proc_region_data(water_emissions_data)

    water_emissions_data.columns.values[0] = "FlowName"
    water_emissions_data.columns.values[1] = "FlowAmount"
    water_emissions_data['Compartment'] = 'Water'
    water_emissions_data['Unit'] = 'kg'
    water_emissions_data['input'] = False

    # Ground emissions
    # Grab two of the last three columns (emission and ground amounts)
    ground_emissions_data = df.iloc[:, [df.shape[1]-3, df.shape[1]-2]]
    ground_emissions_data = _proc_region_data(ground_emissions_data)

    ground_emissions_data.columns.values[0] = "FlowName"
    ground_emissions_data.columns.values[1] = "FlowAmount"
    ground_emissions_data['Compartment'] = 'Ground'
    ground_emissions_data['Unit'] = 'kg'
    ground_emissions_data['input'] = False

    return air_emissions_data, water_emissions_data, ground_emissions_data


def save_ng_lci(df, filename, destination_path):
    """
    Save the final table to CSV file.
    """
    if destination_path is None:
        destination_path = f"{os.getcwd()}/"
    if filename is None:
        filename = 'final_table'
    full_path = os.path.join(destination_path, f"{filename}.csv")
    df.to_csv(full_path, index=False)


##############################################################################
# MAIN
##############################################################################
if __name__=='__main__':
    from electricitylci.utils import get_logger
    import electricitylci.model_config as config

    log = get_logger(True, False)
    config.model_specs = config.build_model_class("ELCI_2023")

    from electricitylci.natural_gas_upstream import generate_upstream_ng
    year = 2023
    df = generate_upstream_ng(year)
