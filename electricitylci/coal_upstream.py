#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# coal_upstream.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import numpy as np
import pandas as pd
import requests

from ast import literal_eval
from electricitylci.globals import paths
from electricitylci.globals import data_dir
from electricitylci.globals import STATE_ABBREV
from electricitylci.eia860_facilities import eia860_balancing_authority
from electricitylci.eia923_generation import eia923_download
from electricitylci.eia923_generation import eia923_generation_and_fuel
from electricitylci.elementaryflows import map_emissions_to_fedelemflows
from electricitylci.model_config import model_specs
import electricitylci.PhysicalQuantities as pq
from electricitylci.utils import download
from electricitylci.utils import find_file_in_folder


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module generates the annual emissions from coal mining and
transportation using life cycle inventory developed at the National Energy
Technology Laboratory combined with fuel receipt data from EIA.
The provided life cycle inventory for coal mining is based on 2016 data
(measured methane emissions, etc.) and is provided on a mass basis: per kg of
coal. As such, the annual inventory developed will not be completely
representative of any year outside of 2016; however, this isn't necessarily any
different than other life cycle data that was developed using data in a given
year and remains static until the next update. Likewise the coal transportation
data is fixed for the year 2016; however, it does not respond to the EIA data
in the same manner. The kg-km amounts of transportation by mode are dependent
only on the power plant location and source of coal, as mapped by the module;
therefore, the only thing that could change the annual coal transportation
inventory is if a particular plant began receiving coal of a different type,
from a different type of mine, or from a different location.

For the 2023 coal model, see: https://www.osti.gov/biblio/2370100.

Last updated:
    2025-02-04
"""
__all__ = [
    "COAL_MINING_LCI_VINTAGE",
    "COAL_TRANSPORT_LCI_VINTAGE",
    "coal_type_codes",
    "mine_type_codes",
    "basin_codes",
    "transport_dict",
    "eia_7a_download",
    "generate_upstream_coal",
    "get_2023_coal_transport_lci",
    "generate_upstream_coal_map",
    "read_coal_mining",
    "read_coal_transportation",
    "read_eia7a_public_coal",
    "read_eia923_fuel_receipts",
]


##############################################################################
# GLOBALS
##############################################################################
COAL_MINING_LCI_VINTAGE = 2023
'''int : The life cycle inventory vintage for coal mining (2020 or 2023).'''

COAL_TRANSPORT_LCI_VINTAGE = 2023
'''int : The life cycle inventory vintage for coal transportation (2020/23).'''

coal_type_codes = {
    'BIT': 'B',
    'LIG': 'L',
    'SUB': 'S',
    'WC': 'W',
    'RC' : 'RC',
}
'''dict : Map between EIA coal fuel source codes and NETL coal codes.'''

mine_type_codes = {
    'Surface': 'S',
    'Underground': 'U',
    'Facility': 'F',
    'Processing': 'P',
}
'''dict : A map between coal mine type and their abbreviation.'''

basin_codes = {
    'Central Appalachia': 'CA',
    'Central Interior': 'CI',
    'Gulf Lignite': 'GL',
    'Illinois Basin': 'IB',
    'Lignite': 'L',
    'Northern Appalachia': 'NA',
    'Powder River Basin': 'PRB',
    'Rocky Mountain': 'RM',
    'Southern Appalachia': 'SA',
    'West/Northwest': 'WNW',
    'Import': 'IMP',
}
'''dict : A map between NETL coal basin names and their abbreviations.'''

transport_dict = {
    'Avg Barge Ton*Miles': 'Barge',
    'Avg Lake Vessel Ton*Miles': 'Lake Vessel',
    'Avg Ocean Vessel Ton*Miles': 'Ocean Vessel',
    'Avg Railroad Ton*Miles': 'Railroad',
    'Avg Truck Ton*Miles': 'Truck',
}
'''dict : A map from 2016 coal model transport columns to their short names.'''


##############################################################################
# FUNCTIONS
##############################################################################
def _clean_columns(df):
   """Remove special characters and convert column names to snake case."""
   df.columns = (
       df.columns.str.lower().str.replace(
           '[^0-9a-zA-Z\\-]+', ' ',
           regex=True
        ).str.replace(
            '-', '',
            regex=True
        ).str.strip().str.replace(' ', '_', regex=True)
    )

   return df


def _coal_code(row):
    """Generate coal basin + energy source + mine type string-based code."""
    coal_code_str = (
        f'{basin_codes[row["netl_basin"]]}-'
        f'{coal_type_codes[row["energy_source"]]}-'
        f'{row["coalmine_type"]}'
    ).upper()

    return coal_code_str


def _process_2023_coal_transport_lci(df, name):
    """Map the 41 air emissions for coal transport to the 2023 transport LCI.

    Parameters
    ----------
    df : pandas.DataFrame
        A coal transport output inventory.
    name : str
        The coal transport mode (e.g. 'Truck' or 'Train')

    Returns
    -------
    pandas.DataFrame
        A data frame with flow names, amounts, units, UUIDs, and compartment
        paths.
    """
    # Get the 41 air emissions we want:
    air_emissions = pd.read_excel(
        os.path.join(data_dir, 'Coal_model_transportation_inventory.xlsx'),
        sheet_name='flowmapping',
        usecols="C:F"
    )
    air_emissions = air_emissions.merge(
        df,
        left_on=['TargetFlowName', 'TargetFlowContext', 'TargetUnit'],
        right_on=['Name', 'Compartment', 'Unit'],
        how='left'
    )
    # Quality check that all flows are matched (241024; TWD)
    # - Conveyor Belt... matched!
    # - Truck... matched!
    # - Barge... missing Mercury and Aldehydes, CS.
    # - Ocean vessel... missing Mercury and Aldehydes, CS.
    # - Train... matched!

    # Fill in missing values (for barge and ocean vessel)
    air_emissions['Name'] = air_emissions['Name'].fillna(
        air_emissions['TargetFlowName'])
    air_emissions['Compartment'] = air_emissions[
        'Compartment'].fillna(air_emissions['TargetFlowContext'])
    air_emissions['Unit'] = air_emissions['Unit'].fillna(
        air_emissions['TargetUnit'])
    air_emissions['Amount (per kg*km)'] = air_emissions[
        'Amount (per kg*km)'].fillna(0)

    # Add the transport source code name
    air_emissions['coal_source_code'] = name
    # Remove duplicate columns:
    drop_cols = [
        'TargetFlowName',
        'TargetFlowContext',
        'TargetUnit',
        'Category',
    ]
    air_emissions = air_emissions.drop(columns=drop_cols)
    # Rename remaining columns:
    air_emissions = air_emissions.rename(columns={
        'TargetFlowUUID': "FlowUUID",
        'Amount (per kg*km)': "FlowAmount",
    })

    return air_emissions


def _make_2023_coal_transport_data(year):
    """Generate essentially the same the data as the CSV file from the 2016
    baseline, updated with transportation data from the 2023 coal model,
    where gaps are filled using the U.S. average.

    Transportation data units are kg*km
    (kilograms of coal x kilometers of distance transported).

    Parameters
    ----------
    year : int
        The year used for facility data from EIA 860.

    Returns
    -------
    pandas.DataFrame
        A data frame with plant IDs, coal basins, NERC regions, and kg coal*km
        coal transported data for: Belt, Truck, Barge, Ocean Vessel, and Train.

    Raises
    ------
    OSError
        If the data file is not found.
    """
    # Generate the coal upstream map, which labels each facility with its
    # coal source code: a three-part combo of coal basin, coal type, and
    # mine type. We only want the coal basin data from this.
    coal_map_df = generate_upstream_coal_map(year)
    coal_map_df["Basin"] = coal_map_df["coal_source_code"].str.split("-").str[0]

    # Now, let's find the NERC region for each facility.
    ba_region_df = eia860_balancing_authority(year, regional_aggregation=None)

    # Let's create a dictionary that maps facilities to their NERC region,
    # fixing the plant ID from string to integer along the way.
    # We don't need the heat input or the old coal source code, so let's drop
    # them.
    region_dict = dict(
        zip(ba_region_df["Plant Id"], ba_region_df["NERC Region"])
    )
    region_dict = {int(k): v for k, v in region_dict.items()}
    coal_map_df['NERC Region'] = coal_map_df['plant_id'].map(region_dict)
    coal_map_df = coal_map_df.drop(columns=['coal_source_code', 'heat_input'])

    # Read the 2023 coal model transportation data
    # Source: https://github.com/USEPA/ElectricityLCI/discussions/273
    coal_dir = os.path.join(data_dir, "coal", "2023")
    coal_file = os.path.join(coal_dir, "coal_transport_dist.csv")
    if not os.path.isfile(coal_file):
        raise OSError(
            "Failed to find 2023 coal transportation "
            "data file, '%s'" % coal_file)
    coal_trans_df = pd.read_csv(coal_file)

    # NOTE: the 2023 coal model uses a slightly different naming scheme
    # for WNW coal basin, so let's fix it.
    basin_codes_new = {k:v for k, v in basin_codes.items()}
    del basin_codes_new["West/Northwest"]
    basin_codes_new["West/North West"] = "WNW"

    # Now, map the basin names to their basin codes.
    # NOTE this works for all basins except for "U.S. Average"
    coal_trans_df["Basin"] = coal_trans_df["Basin"].map(basin_codes_new)

    # Some facilities may not map to our coal model, so let's save the
    # U.S. average and use it for them.
    # TODO: Consider saving the weighted averages for regions as well!
    us_ave_coal_trans = coal_trans_df.loc[coal_trans_df['Basin'].isna(), :]
    us_ave_coal_trans = us_ave_coal_trans.reset_index(drop=True)

    # Drop the NaNs from our coal transportation data frame
    # (i.e., the U.S. average that we saved separately).
    coal_trans_df = coal_trans_df.dropna().copy()

    # Put it all together by merging our transportation data and the
    # coal data using the NERC region and coal basin codes as the
    # common attributes.
    final_df = pd.merge(
        left=coal_map_df,
        right=coal_trans_df,
        on=['Basin', 'NERC Region'],
        how='left',
    )

    # there are facilities not mapped to transportation; let's give them the
    # U.S. average values
    # TODO: consider using weighted-average regional values.
    final_df = final_df.fillna({
        'Belt': us_ave_coal_trans.loc[0, 'Belt'],
        'Truck': us_ave_coal_trans.loc[0, 'Truck'],
        'Barge': us_ave_coal_trans.loc[0, 'Barge'],
        'Ocean Vessel': us_ave_coal_trans.loc[0, 'Ocean Vessel'],
        'Train': us_ave_coal_trans.loc[0, 'Train'],
    })

    # The transportation data from the coal model are in miles.
    # Let's convert miles to kilometers, and calculate the kg*km values by
    # multiplying the quantity (kg of coal) by transportation distance
    # (miles converted to km).
    mi_to_km = pq.convert(1, 'mi', 'km')

    trans_cols = ["Belt", "Truck", "Barge", "Ocean Vessel", "Train"]
    final_df[trans_cols] = final_df[trans_cols].mul(mi_to_km)
    final_df[trans_cols] = final_df[trans_cols].mul(
        final_df["quantity"],
        axis=0
    )

    return final_df


def _make_ave_transport(trans_df, lci_df):
    """Estimate the average facility coal transport (kg*km) by mode.

    Uses the U.S. average transport distances by transport mode data from
    the 2023 Coal Baseline Report (NETL, 2023), and multiplies it by the
    estimated kilograms of coal for each transport mode (based on the
    quantities provided in the coal mining LCI).

    Parameters
    ----------
    trans_df : pandas.DataFrame
        Coal transportation LCI.
    lci_df : pandas.DataFrame
        Coal mining LCI.

    Returns
    -------
    pandas.DataFrame
        U.S. average coal transport LCI.
        Columns include:

        - 'coal_source_code', transportation code (e.g., 'Barge')
        - 'kg', average facility kilograms by mode (float)
        - 'km', average facility transport distance (float)
        - 'kgkm', average facility coal transport (float)

    Examples
    --------
    >>> coal_input_eia = generate_upstream_coal_map(2020)
    >>> coal_transport_df = read_coal_transportation()
    >>> _make_ave_transport(coal_transport_df, coal_input_eia)
    """
    # Pull just the transportation modes and kg*km for each facility.
    us_ave_kgkm = trans_df[['plant_id', 'coal_source_code', 'quantity']].drop_duplicates().reset_index(drop=True)
    us_ave_kgkm = us_ave_kgkm.rename(columns={'quantity': 'kgkm'})

    # Pull just the short-tons of coal consumption per facility;
    # convert tons to kg.
    us_ave_kg = lci_df[['plant_id', 'quantity']].drop_duplicates().reset_index(
        drop=True)
    us_ave_kg['quantity'] *= pq.convert(1, "ton", "kg")
    us_ave_kg = us_ave_kg.rename(columns={'quantity': 'kg'})

    # Merge by facility to get kg*km by mode and kg of coal by facility
    us_ave = us_ave_kgkm.merge(us_ave_kg, how='inner', on='plant_id')

    # Calculate the travel distances
    non_zero_kg = us_ave['kg'] != 0
    us_ave['km'] = 0.0
    us_ave.loc[non_zero_kg, 'km'] = us_ave.loc[non_zero_kg, 'kgkm']
    us_ave.loc[non_zero_kg, 'km'] /= us_ave.loc[non_zero_kg, 'kg']

    # Convert travel distances back to kg
    # (effectively zero any non-modes of transport)
    non_zero_km = us_ave['km'] != 0
    us_ave['kg_new'] = 0.0
    us_ave.loc[non_zero_km, 'kg_new'] = us_ave.loc[non_zero_km, 'kgkm']
    us_ave.loc[non_zero_km, 'kg_new'] /= us_ave.loc[non_zero_km, 'km']

    # Calculate the facility average kg of coal by transport mode
    us_ave = us_ave.groupby(
        by='coal_source_code').agg({'kg_new': 'mean'}).reset_index(drop=False)
    us_ave = us_ave.rename(columns={'kg_new': 'kg'})

    # Take the U.S. average transport distances, km (Coal Baseline 2023, NETL)
    us_ave_km = pd.DataFrame({
        'coal_source_code': ['Barge', 'Ocean Vessel', 'Railroad', 'Truck'],
        'km': [56.47542079, 67.81356183, 929.0283925, 6.080599763]
    })
    us_ave = us_ave.merge(us_ave_km, how='left', on='coal_source_code')

    # Calculate U.S. facility average kg*km for coal transport.
    us_ave['kgkm'] = us_ave['kg'] * us_ave['km']

    return us_ave


def _transport_code(row):
    """Generate a transport code based on coal source code."""
    try:
        transport_str = transport_dict[row['coal_source_code']]
    except KeyError:
        transport_str = row['coal_source_code']

    return transport_str


def eia_7a_download(year, save_path):
    """Download public coal Excel workbook from EIA.

    Example site for 2020 public coal spreadsheet:
    https://www.eia.gov/coal/data/public/xls/coalpublic2020.xls

    Parameters
    ----------
    year : int
        Year associated with public coal Excel workbook (e.g., 2016).
    save_path : str
        A folder path.
        If the folder does not exist, the method tries to create it.

    Notes
    -----
    Some years are provided in XML format and require re-saving to work with
    the remainder of the code. If you run into troubles with the download,
    see https://github.com/USEPA/ElectricityLCI/issues/230 for a solution.
    """
    eia7a_base_url = 'http://www.eia.gov/coal/data/public/xls/'
    name = 'coalpublic{}.xls'.format(year)
    url = eia7a_base_url + name
    try:
        os.makedirs(save_path)
        logging.info('Downloading EIA 7-A data...')
        eia_7a_file = requests.get(url)
        file_path = os.path.join(save_path, name)
        open(file_path, 'wb').write(eia_7a_file.content)
    except:
        logging.info(
            'Error downloading eia-7a: try manually downloading from %s' % url)


# TODO: consider moving this to eia923_generation.py, which handles
#   EIA Form 923 data management; also consider a secondary parameter for
#   energy-source filtering, which includes coal, natural gas, and petroleum
def read_eia923_fuel_receipts(year):
    """Return data frame of EIA 923 fuel receipts.

    Source: EIA923 Schedules 2,3,4,5,M,12, page 5.

    Parameters
    ----------
    year : int
        The year associated with Form EIA 923.

    Returns
    -------
    pandas.DataFrame
        A data frame with monthly plant info. Columns include:

        -   'year' (int): four digit year
        -   'month' (int)
        -   'plant_id' (int): EIA plant identification (1-5 digit)
        -   'plant_name' (str)
        -   'plant_state' (str): two-character standard state code
        -   'energy_source' (str): 2-3 character fuel code (e.g., ANT, BIT, PC)
        -   'fuel_group' (str): energy group for fuel (e.g. 'Coal', 'Petroleum')
        -   'coalmine_type' (str): type of coal mine (e.g., 'S' for surface)
        -   'coalmine_state' (str): state/country abbreviation for coal mine
        -   'coalmine_county' (str): country FIPS code
        -   'coalmine_msha_id' (str): mine safety & health admin identifier
        -   'quantity' (int): quantity of coal in tons, barrels or Mcf
        -   'average_heat_content' (float): heat content in millions of Btus
            per unit quantity

    Notes
    -----
    The EIA923 Schedules 2, 3, 4, 5, M, 12, includes on Page 5, 'Fuel Receipts
    and Costs,' the primary and secondary transportation modes.
    """
    expected_923_folder = os.path.join(paths.local_path, 'f923_{}'.format(year))

    # Download if not available.
    if not os.path.exists(expected_923_folder):
        logging.info('Downloading EIA-923 files')
        eia923_download(year=year, save_path=expected_923_folder)

    # Check for CSV file
    # HOTFIX find_file_in_folder to not crash when search fails. [241009;TWD]
    csv_file = find_file_in_folder(
        folder_path=expected_923_folder,
        file_pattern_match=['2_3_4_5', '_page_5_reduced', 'csv'],
        return_name=False
    )
    if csv_file:
        logging.info('Loading data from reduced CSV file')
        csv_path = os.path.join(expected_923_folder, csv_file)
        eia_fuel_receipts_df = pd.read_csv(csv_path, low_memory=False)
    else:
        logging.info('Loading data from downloaded Excel file')
        eia923_path, eia923_name = find_file_in_folder(
                folder_path=expected_923_folder,
                file_pattern_match=['2_3_4_5', 'xlsx'],
                return_name=True)
        eia_fuel_receipts_df = pd.read_excel(
            eia923_path,
            sheet_name='Page 5 Fuel Receipts and Costs',
            skiprows=4,
            usecols="A:E,H:M,P:Q",
            engine="openpyxl")
        csv_fn = eia923_name.split('.')[0] + '_page_5_reduced.csv'
        csv_path = os.path.join(expected_923_folder, csv_fn)
        eia_fuel_receipts_df = _clean_columns(eia_fuel_receipts_df)
        eia_fuel_receipts_df.to_csv(csv_path, index=False)

    return eia_fuel_receipts_df


def read_eia7a_public_coal(year):
    """Read public coal Excel workbook from EIA.

    Parameters
    ----------
    year : int
        The EIA generation year.
        Will download the workbook for this year, if not already done so.

    Returns
    -------
    pandas.DataFrame
        Columns include:

        - 'year', same as parameter, year (e.g., 2020)
        - 'msha_id', mine safety and health admin ID (int)
        - 'mine_name', mine name (e.g., John Poe Mine)
        - 'mine_state', U.S. state name (e.g. Alabama)
        - 'mine_county', U.S. county name (e.g., De Kalb)
        - 'mine_status', status code (e.g., Active, Temporarily closed)
        - 'mine_type', mine type (e.g., Surface)
        - 'company_type', company type (e.g., Independent Producer Operator)
        - 'operation_type', operation type (e.g., Mine only, Preparation Plant)
        - 'operating_company', name of company (e.g., Blue Diamond Coal Co)
        - 'operating_company_address', street, state, zip address
        - 'union_code', name of union (e.g., Western Energy Workers)
        - 'coal_supply_region', coal region (e.g., Powder River Basin)
        - 'production_short_tons', coal production (int)
        - 'average_employees', average number of employees (int)
        - 'labor_hours', labor hours (int)

    """
    expected_7a_folder = os.path.join(paths.local_path, 'f7a_{}'.format(year))
    if not os.path.exists(expected_7a_folder):
        logging.info("Downloading EIA public coal Excel workbook")
        eia_7a_download(year, expected_7a_folder)

    eia7a_path = find_file_in_folder(
        folder_path=expected_7a_folder,
        file_pattern_match=['coalpublic'],
        return_name=False)
    # If you're here, then see the following for hotfix:
    # https://github.com/USEPA/ElectricityLCI/issues/230
    eia7a_df = pd.read_excel(
        eia7a_path,
        sheet_name='Hist_Coal_Prod',
        skiprows=3
    )
    eia7a_df = _clean_columns(eia7a_df)

    return eia7a_df


def fix_coal_mining_lci(df):
    """A helper function to clean un-mapped elementary flows from the 2023
    coal model mining LCI.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame of coal mining LCI (e.g., from :func:`read_coal_mining`).

    Returns
    -------
    pandas.DataFrame
        The same data frame sent, but with rows removed that are not found
        in the FEDEFL.
    """
    # HOTFIX non-FEDEFL mapped flows in this inventory [250203; TWD]
    logging.info("Mapping coal mining flows to FEDEFL")

    # Preserve the original UUID
    if 'FlowUUID' in df.columns:
        df = df.rename(columns={'FlowUUID': 'FlowUUID_orig'})

    # Map flows to FEDEFL; knowing some flows are technosphere.
    df_mapped = map_emissions_to_fedelemflows(df)

    # Find unmatched elementary flows and neutralize them!
    unmapped_idx = df_mapped.query(
        "(FlowType == 'ELEMENTARY_FLOW') & (FlowUUID != FlowUUID)").index
    df = df.drop(unmapped_idx)

    # Fix UUID column name:
    if 'FlowUUID_orig' in df.columns:
        df = df.rename(columns={'FlowUUID_orig': 'FlowUUID'})

    return df


def generate_upstream_coal_map(year):
    """Generate data frame of source code, quantity, and heat input.

    Parameters
    ----------
    year : int
        The year for EIA 923 coal fuel receipts and public coal data.

    Returns
    -------
    pandas.DataFrame
        A data frame mapping coal facilities (by ID) to their source code,
        quantity, and heat input values. Columns include:

        - plant_id (int): EIA plant identifier
        - coal_source_code (str): coal basin-coal type-mine type format
        - quantity (float): short tons of coal
        - heat_input (float): millions of BTUs

    Notes
    -----
    This function relies on several data files, namely:

    -   coal_state_to_basin.csv

        Includes columns for 'state' (two-letter abbreviation), base_name
        (forward slash concatenated basin list), 'basin1' (EIA basin name),
        'basin2' (secondary basin name). States not listed have no basin.

    -   eia_to_netl_basin.csv

        Includes columns to match 'eia_basin' names to 'netl_basin' names.

    -   fips_codes.csv

        Includes columns for 'State Abbreviation' (two-letter state
        abbreviation), 'State FIPS Code' (two-digit, zero-padded state FIPS
        code), 'County FIPS Code' (three-digit, zero-padded county FIPS code),
        'FIPS Entity Code' (five-digit zero-padded entity FIPS code), 'ANSI
        Code' (eight-digit, zero-padded ANSI code), 'GU Name', and 'Entity
        Description' (e.g., borough, city, town, County)
    """
    # Filter EIA 923 receipts for coal data.
    eia_fuel_receipts_df = read_eia923_fuel_receipts(year)
    coal_criteria = eia_fuel_receipts_df['fuel_group']=='Coal'
    eia_fuel_receipts_df = eia_fuel_receipts_df.loc[coal_criteria, :]

    # Add EIA coal supply regions from public coal data
    eia7a_df = read_eia7a_public_coal(year)
    eia_fuel_receipts_df = eia_fuel_receipts_df.merge(
        eia7a_df[['msha_id', 'coal_supply_region']],
        how='left',
        left_on='coalmine_msha_id',
        right_on='msha_id',
    )
    eia_fuel_receipts_df.drop(columns=['msha_id'], inplace=True)
    eia_fuel_receipts_df.rename(
        columns={'coal_supply_region': 'eia_coal_supply_region'},
        inplace=True
    )

    # Find where EIA coal supply regions failed to match
    eia_fuel_receipts_na = eia_fuel_receipts_df.loc[
        eia_fuel_receipts_df["eia_coal_supply_region"].isnull(), :]
    eia_fuel_receipts_good = eia_fuel_receipts_df.loc[
        ~eia_fuel_receipts_df["eia_coal_supply_region"].isnull(), :]

    # Start process of correcting the failed matches

    # STEP 1: FIPS code matching
    # Create a summary dataset of mining states, counties, and supply regions
    # with a mine count.
    county_basin = eia7a_df.groupby(
        by=["mine_state", "mine_county", "coal_supply_region"],
        as_index=False
    )["production_short_tons"].count()

    # Remove region or coal type specifiers from state names.
    # NOTE: There are six entries with state called 'Refuse Recovery'
    #       that are 'Refuse' mines in VA, PA, WV, and CO.
    county_basin["mine_state"] = county_basin["mine_state"].str.replace(
        r" \(.*\)", "", regex=True
    )

    # Map state names to their abbreviations.
    #   Note that only 'Refuse Recovery' is unmatched
    county_basin["mine_state_abv"] = county_basin[
        "mine_state"].str.lower().map(STATE_ABBREV).str.upper()

    # Make county names lowercase to ease matching.
    county_basin["mine_county"] = county_basin["mine_county"].str.lower()

    # Read U.S. county FIPS codes and make them strings.
    #   Note that gu name is the county name, which is also made lowercase
    fips_codes = pd.read_csv(os.path.join(data_dir, "fips_codes.csv"))
    fips_codes = _clean_columns(fips_codes)
    fips_codes["gu_name"] = fips_codes["gu_name"].str.lower()

    # Convert county FIPS code to string
    fips_codes["county_fips_code"] = fips_codes[
        "county_fips_code"].astype(str).str.replace(".0", "", regex=False)

    # Add county FIPS code to county_basin data frame
    # NOTE expands the rows where multiple FIPS codes are mapped to same county
    #      this provides multiple match possibilities with EIA receipts
    county_basin = county_basin.merge(
        right=fips_codes[[
            "state_abbreviation", "county_fips_code", "gu_name"]],
        left_on=["mine_state_abv", "mine_county"],
        right_on=["state_abbreviation", "gu_name"],
        how="left"
    )
    county_basin.drop_duplicates(
        subset=["mine_state_abv", "county_fips_code"],
        inplace=True
    )

    # Add county basin data (i.e., coal supply regions) to EIA fuel receipts
    # without coal supply regions by matching the state-county FIPS info.
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
        right=county_basin[[
            "mine_state_abv", "county_fips_code", "coal_supply_region"]],
        left_on=["coalmine_state", "coalmine_county"],
        right_on=["mine_state_abv", "county_fips_code"],
        how="left"
    )
    eia_fuel_receipts_na["eia_coal_supply_region"] = eia_fuel_receipts_na[
        "coal_supply_region"]

    # Update the "good" and "na" data frames based on the matches from above.
    #    Note for 2022, 1350 na rows drop down to 181 after "made good"
    eia_fuel_receipts_made_good = eia_fuel_receipts_na.loc[
        ~eia_fuel_receipts_na["eia_coal_supply_region"].isnull(),
        :].reset_index(drop=True)
    eia_fuel_receipts_na = eia_fuel_receipts_na.loc[
        eia_fuel_receipts_na["eia_coal_supply_region"].isnull(),
        :].reset_index(drop=True)
    eia_fuel_receipts_made_good.drop(
        columns=["mine_state_abv", "county_fips_code", "coal_supply_region"],
        inplace=True
    )
    eia_fuel_receipts_good = pd.concat(
        [eia_fuel_receipts_good, eia_fuel_receipts_made_good],
        ignore_index=True,
        sort=False
    )

    # Read in EIA-to-NETL basin mapping and merge with EIA fuel receipts.
    eia_netl_basin = pd.read_csv(
        os.path.join(data_dir, "eia_to_netl_basin.csv")
    )
    eia_fuel_receipts_good = eia_fuel_receipts_good.merge(
        eia_netl_basin,
        left_on='eia_coal_supply_region',
        right_on ="eia_basin",
        how="left").reset_index(drop=True)

    # STEP 2: match basins at the state level
    # Read in U.S. state to EIA basin mapping to the "na" receipts.
    #   Note there are potentially two basin names for each coal plant.
    state_region_map = pd.read_csv(
        os.path.join(data_dir, 'coal_state_to_basin.csv')
    )
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
        state_region_map[['state', 'basin1', 'basin2']],
        left_on='coalmine_state',
        right_on='state',
        how='left'
    )
    eia_fuel_receipts_na.drop(columns=['state'], inplace=True)

    # Map the EIA basins to their NETL basin names
    #   Note: all netl_basin names are NaN, since no eia_coal_supply_region.
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
        eia_netl_basin,
        how='left',
        left_on='eia_coal_supply_region',
        right_on='eia_basin'
    )
    eia_fuel_receipts_na.drop(columns=['eia_basin'], inplace=True)

    # Match NETL basins to EIA basin 1
    minimerge = pd.merge(
        left=eia_fuel_receipts_na,
        right=eia_netl_basin,
        left_on='basin1',
        right_on='eia_basin',
        how='left'
    )
    minimerge.drop(
        columns=[
            "mine_state_abv",
            "county_fips_code",
            "coal_supply_region",
            "basin1",
            "basin2",
            "netl_basin_x",
            "eia_basin"],
        inplace=True
    )
    minimerge.rename(columns={"netl_basin_y": "netl_basin"}, inplace=True)

    # Add the "na" receipts to the "good" receipts.
    #   Note that most unmatched basins are "Imports".
    eia_fuel_receipts_good = pd.concat(
        [eia_fuel_receipts_good, minimerge],
        ignore_index=True,
        sort=False
    )

    # Overwrite select NETL basins based on energy sources and supply regions.
    gulf_lignite = (
        (eia_fuel_receipts_good['energy_source']=='LIG') &
        (eia_fuel_receipts_good['eia_coal_supply_region']=='Interior')
    )
    eia_fuel_receipts_good.loc[gulf_lignite, ['netl_basin']] = 'Gulf Lignite'

    lignite = ((eia_fuel_receipts_good['energy_source']=='LIG') &
               (eia_fuel_receipts_good['eia_coal_supply_region']=='Western'))
    eia_fuel_receipts_good.loc[lignite, ['netl_basin']] = 'Lignite'

    # In 2022, coalmine_type is only column with NaNs (65).
    eia_fuel_receipts_good.dropna(
        subset=['netl_basin', 'energy_source', 'coalmine_type'],
        inplace=True
    )

    # Generate the basin-coal type-mine type code
    eia_fuel_receipts_good['coal_source_code'] = eia_fuel_receipts_good.apply(
        _coal_code,
        axis=1
    )

    # Calculate heat input (quantity in tons; average heat content in millions
    # of Btus per unit quantity = millions of Btus)
    eia_fuel_receipts_good['heat_input'] = eia_fuel_receipts_good[
        'quantity'] * eia_fuel_receipts_good['average_heat_content']

    eia_fuel_receipts_good.drop_duplicates(inplace=True)

    # Map to NETL coal types --- these should match the coal type found in
    # the coal source code.
    eia_fuel_receipts_good["coal_type"] = eia_fuel_receipts_good[
        "energy_source"].map(coal_type_codes)

    final_df = eia_fuel_receipts_good.groupby(
        ['plant_id', 'coal_type', 'coal_source_code'],
    )[['quantity', 'heat_input']].sum()

    final_df["mass_fraction"] = final_df[["quantity"]].div(
        final_df[["quantity"]].groupby(level=[0,1]).transform('sum')
    )
    final_df["energy_fraction"] = final_df[["heat_input"]].div(
        final_df[["heat_input"]].groupby(level=[0,1]).transform('sum')
    )
    final_df["mass_fraction_total"] = final_df[["quantity"]].div(
        final_df[["quantity"]].groupby(level=[0]).transform('sum')
    )
    final_df["energy_fraction_total"] = final_df[["heat_input"]].div(
        final_df[["heat_input"]].groupby(level=[0]).transform('sum')
    )

    # Read EIA 923 generation and fuel data.
    #   reported_fuel_type_code (str): fuel code (e.g., 'SUB' or 'LIG')
    #   electric_fuel_consumption_quantity: short tons of coal for elec. gen.
    #   elec_fuel_consumption_mmbtu: millions of Btus of fuel consumption
    eia_923_gen_fuel = eia923_generation_and_fuel(year)
    eia_923_gen_fuel = eia_923_gen_fuel[[
        "plant_id",
        "reported_fuel_type_code",
        "electric_fuel_consumption_quantity",
        "elec_fuel_consumption_mmbtu"
    ]].copy()
    eia_923_gen_fuel["coal_type"] = eia_923_gen_fuel[
        "reported_fuel_type_code"].map(coal_type_codes)
    eia_923_gen_fuel["plant_id"] = eia_923_gen_fuel["plant_id"].astype(int)
    # Effectively filters out NaNs in coal type
    # NOTE: quantity is in short tons
    eia_923_grouped = eia_923_gen_fuel.groupby(
        by=["plant_id", "coal_type"])[[
            "electric_fuel_consumption_quantity",
            "elec_fuel_consumption_mmbtu"
    ]].sum()

    final_df.reset_index(inplace=True)
    eia_923_grouped.reset_index(inplace=True)

    final_df = final_df.merge(
        eia_923_grouped,
        on=["plant_id","coal_type"],
        how="left"
    )
    rc_coal = eia_923_grouped.loc[
        eia_923_grouped["coal_type"]=="RC", :].set_index("plant_id")
    final_df["rc_coal_quantity"] = final_df["plant_id"].map(
        rc_coal["electric_fuel_consumption_quantity"]
    )
    final_df["rc_coal_mmbtu"] = final_df["plant_id"].map(
        rc_coal["elec_fuel_consumption_mmbtu"]
    )
    final_df["new_quantity"] = final_df[
        "mass_fraction"] * final_df["electric_fuel_consumption_quantity"]
    final_df["new_heat_input"] = final_df[
        "energy_fraction"] * final_df["elec_fuel_consumption_mmbtu"]
    final_df.loc[
        ~final_df["rc_coal_quantity"].isna(),
        "new_quantity"
    ] = final_df["new_quantity"] + (
        final_df["mass_fraction_total"] * final_df["rc_coal_quantity"]
    )
    final_df.loc[
        ~final_df["rc_coal_mmbtu"].isna(),
        "new_heat_input"
    ] = final_df["new_heat_input"] + (
        final_df["energy_fraction"] * final_df["rc_coal_mmbtu"]
    )
    # WARNING: slice
    final_df = final_df[[
        "plant_id",
        "coal_source_code",
        "new_quantity",
        "new_heat_input",
    ]]
    final_df = final_df.rename(
        columns = {
            "new_quantity": "quantity",
            "new_heat_input": "heat_input"}
    )

    return final_df


def get_2023_coal_transport_lci(coal_xlsx="Transportation-Inventories.xlsx"):
    """Return the 2023 coal model transportation air emission LCI.

    The source Excel workbook is available online here:
    https://www.netl.doe.gov/energy-analysis/details?id=27ea1ba4-6ea9-4ee5-8b32-d7fce7f4e1e0

    Parameters
    ----------
    coal_xlsx : str, optional
        File path to the Excel workbook for coal transportation inventories,
        by default "Transportation-Inventories.xlsx"

    Returns
    -------
    pandas.DataFrame
        A data frame of air emissions associated with coal mining and processing transportation modes (e.g., 'Conveyor Belt', 'Truck', 'Barge', 'Ocean Vessel', and 'Railroad') in units of emission per
        kg*km of coal transport. Columns include:

        - 'Name', flow name
        - 'Compartment', the compartment path (e.g., emission/air)
        - 'Unit', the flow units (e.g., kg)
        - 'Mode', the transportation mode (e.g., 'Truck' or 'Railroad')
        - 'FlowUUID', the universally unique ID for the flow (FEDEFL mapped)
        - 'FlowAmount', the emission in units per kg*km
    """
    if coal_xlsx is None or coal_xlsx == "" or not os.path.isfile(coal_xlsx):
        # NETL VUE URL for transportation inventory Excel workbook.
        trans_url = (
            "https://www.netl.doe.gov/projects/VueConnection"
            "/download.aspx?id=403d0bc0-4752-4225-9bc5-5a402ebad020"
            "&filename=Transportation+Inventories.xlsx"
        )

        # The data store for NETL transportation inventories workbook.
        data_folder = os.path.join(paths.local_path, 'netl')
        if not os.path.isdir(data_folder):
            logging.info("Creating local data folder, %s" % data_folder)
            os.mkdir(data_folder)
        data_path = os.path.join(data_folder, "Transportation-Inventories.xlsx")

        # Check to see data file already exists
        if os.path.isfile(data_path):
            logging.info("Reading existing Excel file")
            coal_xlsx = data_path
        else:
            _is_good = download(trans_url, data_path)
            if _is_good:
                logging.info("Downloaded NETL transportation file")
                coal_xlsx = data_path
            else:
                raise OSError(
                    "Failed to acquire NETL transportation inventory!")

    # There is a workbook for each transportation mode.
    sheets = ['Conveyor Belt', 'Truck', 'Barge', 'Ocean Vessel', 'Train']
    num_sheets = len(sheets)
    logging.info("Generating 2023 coal transportation inventory data")
    for i in range(num_sheets):
        sheet = sheets[i]

        # Reads only the output flows (emissions) for a given transport mode.
        trans_df = pd.read_excel(
            coal_xlsx,
            sheet_name=sheet,
            skiprows=2,
            usecols="H:J,L"
        )
        # Fix column names;
        # note that ".1" is appended to each column name (duplicates in sheet)
        col_names = [x.replace(".1", "") for x in trans_df.columns]
        trans_df.columns = col_names

        # Make compartment path (remove 'elementary flows')
        trans_df["Compartment"] = trans_df["Category"].str.replace(
            "Elementary Flows/", "", regex=False)

        if i == 0:
            t_emissions = _process_2023_coal_transport_lci(trans_df, sheet)
        else:
            temp_df = _process_2023_coal_transport_lci(trans_df, sheet)
            t_emissions = pd.concat([t_emissions, temp_df], ignore_index=True)

    # Match modes to ``_transport_code``; fix Train
    t_emissions['coal_source_code'] = t_emissions['coal_source_code'].map({
        'Conveyor Belt': 'Belt',
        'Truck': 'Truck',
        'Barge': 'Barge',
        'Ocean Vessel': 'Ocean Vessel',
        'Train': 'Railroad',
    })

    return t_emissions


def get_2023_ave_coal_transport(trans_df, input_df):
    """Create a U.S. average facility coal transportation inventory.

    Parameters
    ----------
    trans_df : pandas.DataFrame
        Coal transportation LCI.
    input_df : pandas.DataFrame
        Coal inventory.

    Returns
    -------
    pandas.DataFrame
        U.S. average coal facility transportation LCI.

    Examples
    --------
    >>> coal_input_eia = generate_upstream_coal_map(2020)
    >>> coal_transport_df = read_coal_transportation()
    >>> get_2023_ave_coal_transport(coal_transport_df, coal_input_eia)
    """
    # Get coal transportation and average kg*km transport by mode and merge.
    trans_lci = get_2023_coal_transport_lci()
    us_ave_trans = _make_ave_transport(trans_df, input_df)
    trans_lci = trans_lci.merge(us_ave_trans, how='left', on='coal_source_code')
    # Multiply flow amount (units/kg*km) by total transportation (kg*km)
    trans_lci['FlowAmount'] *= trans_lci['kgkm']
    # Fix column names and set to a common variable
    trans_lci = trans_lci.rename(columns={'Name': 'FlowName'})
    # Clean up step (NaNs are mostly Belt transport)
    trans_lci = trans_lci.dropna()
    trans_lci = trans_lci.drop(columns=['kg', 'km', 'kgkm'])
    trans_lci = trans_lci.reset_index(drop=True)
    # Add missing transportation metadata columns
    trans_lci['Source'] = 'Transportation'
    trans_lci['ElementaryFlowPrimeContext'] = 'emission'
    trans_lci['input'] = False
    trans_lci["FlowType"] = "ELEMENTARY_FLOW"

    return trans_lci


def get_coal_transportation():
    """Create the coal transport data frame in kilograms of coal by kilometers
    of distance transported for each facility by transportation type
    (e.g. 'Barge' or 'Truck').

    Returns
    -------
    pandas.DataFrame
        A three-column data frame of 'plant_id', 'coal_source_code'
        (i.e., tranportation type like 'Truck' or 'Barge'), and 'quantity'
        (i.e., transportation of kilograms of coal by kilometers of distance).

        The 2020 version has five types of transportation (i.e., 'Barge', 'Lake
        Vessel', 'Ocean Vessel', 'Railroad', and 'Truck).

        The 2023 version has five types of transportation (i.e., 'Barge',
        'Belt', 'Ocean Vessel', 'Railroad', and 'Truck').

    Raises
    ------
    ValueError
        If the global parameter year is not correctly assigned.

    Notes
    -----
    Method depends on the global parameter, `COAL_TRANSPORT_LCI_VINTAGE`.
    For 2020, the 2016 baseline's ABB data file is referenced (i.e.,
    '2016_Coal_Trans_By_Plant_ABB_Data.csv').
    For 2023, the 2023 coal baseline data file is referenced
    (i.e., 'coal_transport_dist.csv' in the coal/2023 folder of data).
    """
    # IN PROGRESS
    if COAL_TRANSPORT_LCI_VINTAGE == 2020:
        # The 2016 transportation data by facility.
        logging.info("Using 2016 coal baseline transportation distance data.")
        coal_transportation = pd.read_csv(
            os.path.join(data_dir, '2016_Coal_Trans_By_Plant_ABB_Data.csv')
        )
        # Make rows facility IDs with Transport column (modes) and
        # value (ton*mi)
        coal_transportation = coal_transportation.melt(
            'Plant Government ID',
            var_name='Transport'
        )
        # NOTE: the 2016 transportation functional unit is ton*miles;
        # convert ton*mi to kg*km
        coal_transportation["value"] = (
            coal_transportation["value"]
            * pq.convert(1, "ton", "kg")
            * pq.convert(1, "mi", "km")
        )
        # Rename transport columns
        coal_transportation = coal_transportation.rename(columns={
            'Plant Government ID': 'plant_id',
            'Transport': 'coal_source_code',
            'value': 'quantity',
        })
        # Correct coal_transportation codes
        coal_transportation['coal_source_code'] = coal_transportation.apply(
            _transport_code, axis=1)
    elif COAL_TRANSPORT_LCI_VINTAGE == 2023:
        logging.info("Using 2023 coal model transportation distance data")
        coal_transportation = _make_2023_coal_transport_data(
            model_specs.eia_gen_year)

        # NOTE: the 2016 baseline uses 'Railroad' in place of 'Train'
        coal_transportation = coal_transportation.rename(
            columns={'Train': 'Railroad'}
        )

        # The data frame needs melted to match the 2016 data frame, which has
        # three columns: plant_id, coal_source_code (i.e., transportation type),
        # and quantity (i.e., the kg*km values).
        coal_transportation = coal_transportation.melt(
            id_vars=("plant_id",),
            value_vars=('Belt', 'Truck', 'Barge', 'Ocean Vessel', 'Railroad')
        )

        # To allow facilities receiving coal from more than one region/basin,
        # group by facility and sum by transportation type.
        coal_transportation = coal_transportation.groupby(by=['plant_id', 'variable']).agg({'value': 'sum'}).reset_index(drop=False)

        # Rename to match the 2016 data frame
        coal_transportation = coal_transportation.rename(
            columns={'variable': 'coal_source_code', 'value': 'quantity'}
        )
    else:
        raise ValueError(
            "The coal transport year, %d, "
            "is unknown!" % COAL_TRANSPORT_LCI_VINTAGE)

    return coal_transportation


def read_coal_mining():
    """Read coal mining (extraction and processing) life cycle inventory.

    Depends on the global coal mining LCI vintage year.
    The flow amounts (associated with Results column) are based on the
    functional unit of 1 kg of coal processed at mine.

    Returns
    -------
    pandas.DataFrame
        A coal mining life-cycle inventory.
        Columns include (but are not limited to):

        -   'Results', the flow amount column in Units per kg of coal
        -   'Coal Code', basin-coal type-mine type (e.g., CA-B-S)
        -   'FlowUUID', universally unique identifier for flow
        -   'Compartment', emission compartment paths (e.g. emission/air)
        -   'FlowName', flow name
        -   'FlowType', elementary, product, or waste flow
        -   'Unit', flow unit (e.g., kg)
        -   'input', for resources (true) or emissions (false)
        -   'ElementaryFlowPrimeContext', emission, resource , or technosphere

    Raises
    ------
    ValueError
        If the global parameter does not match coal mining vintages available.
    """
    if COAL_MINING_LCI_VINTAGE == 2023:
        logging.info("Reading 2023 coal mining inventory")
        # The 2023 coal mining CSV file has the correct headings and formats.
        cm_df = pd.read_csv(
            os.path.join(data_dir, 'coal', '2023', 'coal_mining_lci.csv')
        )
        cm_df = fix_coal_mining_lci(cm_df)
    elif COAL_MINING_LCI_VINTAGE == 2020:
        logging.info("Reading 2020 coal mining inventory")
        # The 2020 coal mining LCI needs some help and a results column.
        cm_df = pd.read_csv(
            os.path.join(data_dir, 'coal', '2020', 'coal_mining_lci.csv')
        )
        cm_df = cm_df.drop(columns=["flow.@type"])
        cm_df = cm_df.rename(
            columns={
                "flow.categoryPath": "Compartment",
                "flow.name": "FlowName",
                "flow.refUnit": "Unit",
                "flow.flowType": "FlowType",
                "Scenario": "Coal Code",
                "flow.@id": "FlowUUID",
                "p50": "Results",        # NOTE: Choose your results column.
            }                            # Monte-Carlo columns (p05 to p97.5)
        )                                # or Mean.
        cm_df["Compartment"] = cm_df["Compartment"].apply(literal_eval)
        cm_df["Compartment"] = cm_df["Compartment"].str.join("/")
        cm_df["Compartment"] = cm_df["Compartment"].str.replace(
            "Elementary Flows/", "", regex=False)
    else:
        raise ValueError("Coal mining LCI vintage must be 2020 or 2023.")

    # HOTFIX data type incompatibility [2024-01-09; TWD]
    cm_df["ElementaryFlowPrimeContext"] = ""
    cm_df.loc[
        cm_df["Compartment"].str.contains("emission/"),
        "ElementaryFlowPrimeContext"] = "emission"
    cm_df.loc[
        cm_df["Compartment"].str.contains("resource/"),
        "ElementaryFlowPrimeContext"] = "resource"
    cm_df.loc[
        (cm_df["FlowType"].str.contains("PRODUCT_FLOW"))
        | (cm_df["FlowType"].str.contains("WASTE_FLOW")),
        "ElementaryFlowPrimeContext"] = "technosphere"
    cm_df.reset_index(drop=True, inplace=True)

    return cm_df


def read_coal_transportation():
    """Return the coal transportation mode LCI.

    Currently utilizes the 2016 plant-level transportation distances,
    and combined with transportation mode LCI, which depends on global
    parameter, coal transportation vintage.

    Returns
    -------
    pandas.DataFrame
        Transportation LCI. Columns include:

        - 'plant_id', EIA facility ID (based on 2016 transport data)
        - 'coal_source_code', coal source code (e.g., Barge)
        - 'quantity', facility-level coal transport quantity (kg*km)
        - 'FlowName', emission name (e.g., Methane)
        - 'FlowAmount', emission amount scaled by coal quantity (float)
        - 'Compartment', emission compartment (e.g., emission/air)
        - 'FlowUUID', emission universally unique ID
        - 'Unit', emission unit (e.g., kg)
        - 'Source', Transportation
        - 'ElementaryFlowPrimeContext', emission
        - 'FlowType', ELEMENTARY_FLOW
        - 'input', whether flow is resource (true) or emission (false)

    """
    # Get the appropriate coal transportation distance data:
    coal_transportation = get_coal_transportation()

    # FORK IN THE ROAD
    if COAL_TRANSPORT_LCI_VINTAGE == 2023:
        logging.info("Reading 2023 coal transport LCI")
        coal_inventory_transportation = get_2023_coal_transport_lci()
        merged_transport_coal = coal_transportation.merge(
            coal_inventory_transportation,
            on=['coal_source_code'],
            how='left'
        )

        # Group because some plants get the same flow from several basins
        #   Columns in: 57255; Columns out: 56908.
        merged_transport_coal = merged_transport_coal.groupby(
            by=[
                'plant_id',
                'coal_source_code',
                'Compartment',
                'Name',
                'FlowUUID',
                'Unit']
        )[['quantity', 'FlowAmount']].sum()
        merged_transport_coal = merged_transport_coal.reset_index()

        # Multiply transportation emission value (kg/kg*km) by total
        # transportation (kg*km)
        merged_transport_coal['FlowAmount'] *= merged_transport_coal['quantity']

        # Fix column names and set to a common variable
        #   should have the same columns as 2020 data.
        merged_transport_coal = merged_transport_coal.rename(columns={
            'Name': 'FlowName'})
        transport_coal = merged_transport_coal
    elif COAL_TRANSPORT_LCI_VINTAGE == 2020:
        # Read coal transportation emissions inventory (units = kg/kg*km);
        # these are air emissions by transportation mode.
        coal_inventory_transportation = pd.read_excel(
            os.path.join(data_dir, 'Coal_model_transportation_inventory.xlsx'),
            sheet_name='transportation'
        )

        # Correct coal source codes
        coal_inventory_transportation = coal_inventory_transportation.rename(
            columns={'Modes': 'coal_source_code'})
        coal_inventory_transportation['coal_source_code'] = coal_inventory_transportation.apply(
            _transport_code, axis=1)

        # Add transportation data to emissions inventory
        # NOTE coal transportation does not include 'Belt' transport.
        merged_transport_coal = coal_transportation.merge(
            coal_inventory_transportation,
            on=['coal_source_code'],
            how='left'
        )

        # Multiply transportation emission value (kg/kg*km) by total
        # transportation (kg*km)
        column_air_emission=[
            x for x in coal_inventory_transportation.columns[1:]
            if "Unnamed" not in x]
        merged_transport_coal[column_air_emission] = (
            merged_transport_coal[column_air_emission].multiply(
                merged_transport_coal['quantity'],
                axis="index",
            )
        )

        # Groupby the plant ID since some plants have multiple row entries
        # (receive coal from multiple basins)
        merged_transport_coal = merged_transport_coal.groupby(
            ['plant_id','coal_source_code'])[['quantity'] + column_air_emission].sum()
        merged_transport_coal = merged_transport_coal.reset_index()

        # Melting the database on Plant ID; turns each emission and amount
        # into their own row
        melted_database_transport = merged_transport_coal.melt(
            id_vars=['plant_id','coal_source_code','quantity'],
            var_name='FlowName',
            value_name='FlowAmount'
        )

        # Add missing compartment (they are all air emissions).
        # We'll add the units in the next step.
        melted_database_transport['Compartment'] = 'emission/air'

        # Get flow mapping for 41 air emissions
        coal_transportation_flowmapping = pd.read_excel(
            os.path.join(data_dir, 'Coal_model_transportation_inventory.xlsx'),
            sheet_name='flowmapping'
        )

        melted_database_transport = melted_database_transport.merge(
            coal_transportation_flowmapping,
            left_on=["FlowName", "Compartment"],
            right_on=["flowname", "compartment"],
            how="left"
        )
        melted_database_transport.drop(
            columns=["FlowName", "Compartment", "flowname", "compartment"],
            errors="ignore",
            inplace=True
        )
        melted_database_transport.rename(
            columns={
                "TargetFlowUUID": "FlowUUID",
                "TargetFlowContext": "Compartment",
                "TargetUnit": "Unit",
                "TargetFlowName": "FlowName"},
            inplace=True
        )
        # Set to common variable; should have the same columns as 2023 data.
        transport_coal = melted_database_transport

    # Adding to new columns for the compartment (water) and
    # The source of the emissions (mining).
    transport_coal['Source'] = 'Transportation'
    transport_coal["ElementaryFlowPrimeContext"] = "emission"
    transport_coal["FlowType"] = "ELEMENTARY_FLOW"
    transport_coal["input"] = False

    return transport_coal


def generate_upstream_coal(year):
    """Generate the annual coal mining and transportation emissions (in kg)
    for each plant in EIA923.

    Proxy processes are used to gap fill certain missing coal mining scenarios.
    These include:

    -   U.S. Average for imports (of same coal mine type and coal type)
    -   U.S. Average of B-U and S-U for missing U (e.g., WNW-S-U, WNW-L-U)
    -   Surface & underground LCI for production (for same basin and coal type)
    -   PRB-S-S for PRB-B-S

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use

    Returns
    ----------
    pandas.DataFrame
        Includes the following columns:

        -   'plant_id' (int), Plant identifier
        -   'stage_code' (str), LCA stage code (e.g., 'Truck', 'L-L-S')
        -   'quantity' (float),
        -   'FlowName',
        -   'FlowAmount' (float),
        -   'Compartment',
        -   'input' (bool),
        -   'stage',
        -   'FlowUUID',
        -   'ElementaryFlowPrimeContext',
        -   'Unit',
        -   'FlowType',
        -   'FuelCategory',
        -   'Year' (int),
        -   'Source',

    Notes
    -----
    Relies on data files:

        -   2016_Coal_Trans_By_Plant_ABB_Data.csv: contains transportation
            units by transportation mode for each coal facility.
        -   coal_mining_lci.csv: Emission results for each coal scenario
            in the coal model.
        -   Coal_model_transportation_inventory.xlsx

            -   'transportation': Five modes by 41 emission flows
            -   'flowmapping': Map between flow name and target flow name,
                context (e.g., emission/air), UUID, and unit.
    """
    # Read the facility-level coal consumption by source code from EIA
    coal_input_eia = generate_upstream_coal_map(year)

    # Read coal mining LCI (units = kg/kg)
    coal_mining_inventory = read_coal_mining()

    # Look up the coal scenarios for each dataset.
    coal_input_eia_scens = list(coal_input_eia["coal_source_code"].unique())
    coal_inventory_scens = list(coal_mining_inventory["Coal Code"].unique())

    # Find missing scenarios in the LCI.
    missing_scens = [
        x for x in coal_input_eia_scens if x not in coal_inventory_scens]
    logging.info("Addressing %d missing coal scenarios" % len(missing_scens))

    # Fill in each missing scenario with the existing data using weighted
    # averages of current production. Most of these are from processing
    # plants, so the average will be between the underground and surface
    # plants in the same region mining the same type of coal. For imports
    # and surface and underground mines, this will be the weighted average
    # of all of the same type of coal production in the US.

    # Combine facility coal consumption with LCI for all known scenarios.
    #   for 2023 coal mining LCI, 14 existing scenarios are found
    existing_scens_merge = coal_input_eia.loc[
        ~coal_input_eia["coal_source_code"].isin(missing_scens),
        :
    ].merge(
        coal_mining_inventory,
        left_on=["coal_source_code"],
        right_on=["Coal Code"],
        how="left"
    )

    # Define a weighted mean method that uses coal consumption as the weight.
    def wtd_mean(pdser, total_db):
        try:
            wts = total_db.loc[pdser.index, "quantity"]
            result = np.average(pdser, weights=wts)
        except:
            result = float("nan")

        return result

    wm = lambda x: wtd_mean(x, existing_scens_merge)

    # Get just the flow info from coal mining LCI.
    #   drop scenario and results columns;
    #   creates a list of unique flows
    inventory_flow_info = coal_mining_inventory[[
        "FlowUUID",
        "Compartment",
        "FlowType",
        "FlowName",
        "Unit",
        "input",
        "ElementaryFlowPrimeContext"
    ]].drop_duplicates("FlowUUID")

    # Define sub/bituminous underground and surface mine scenarios
    _u_mines = ['-'.join(x) for x in [('B', 'U'), ('S', 'U')]]
    _s_mines = ['-'.join(x) for x in [('B', 'S'), ('S', 'S')]]

    # Initialize the list of flow data frames for missing scenarios
    missing_scens_df_list = []
    for scen in missing_scens:
        # Initialize the coal scenarios to be used for averaging.
        coals_to_include = []

        # Pull the search strings from the scenario
        _basin, _coal, _mine = scen.split("-")
        _coal_mine = "-".join(scen.split("-")[1:])
        _reg_coal = "-".join(scen.split("-")[0:2])

        if scen == "PRB-B-S":
            # Proxy subbituminous for bituminous surface mining
            coals_to_include = [
                x for x in coal_inventory_scens if x == 'PRB-S-S']
        elif _coal == 'W':
            # Waste coals are not currently supported.
            coals_to_include = ['MISSING', ]
        elif _basin == "IMP":
            # The imports, find all matching coal and mine scenarios
            coals_to_include = [
                x for x in coal_inventory_scens if _coal_mine in x]
        elif _mine == "P":
            # Processing scenarios, find all matching region and coal types
            coals_to_include = [
                x for x in coal_inventory_scens if _reg_coal in x]
        elif _mine in ['U', 'S'] and _coal in ['S', 'B']:
            # Use U.S. average for non-waste coal surface and underground mines
            if _mine == 'U':
                for _cm in _u_mines:
                    coals_to_include += [
                        x for x in coal_inventory_scens if _cm in x]
            elif _mine == 'S':
                for _cm in _s_mines:
                    coals_to_include += [
                        x for x in coal_inventory_scens if _cm in x]
            else:
                coals_to_include = [
                    x for x in coal_inventory_scens if _coal_mine in x]

        total_scens = len(coals_to_include)
        if total_scens == 1 and coals_to_include[0] == 'MISSING':
            logging.info("Skipping waste scenario for %s" % scen)
        else:
            logging.info(
                "Found %d proxy scenarios for %s" % (total_scens, scen))

        if total_scens == 0:
            logging.warning(
                "Failed to find proxy process for coal scenario, %s" % scen)
            coals_to_include = ["MISSING"]

        # Get all matching inventories;
        #   Note, this is empty for "MISSING".
        target_inventory_df = existing_scens_merge.loc[
            existing_scens_merge["coal_source_code"].isin(coals_to_include)]

        # Condense the target inventory to just IDs and amounts using
        # the weighted average method.
        scen_inventory_df = target_inventory_df.groupby(
            by="FlowUUID",
            as_index=False).agg({"Results": wm})

        # Skip empty data frames (e.g., waste coal)
        if len(scen_inventory_df) > 0:
            # Define the coal code and match weighted flows back to their info.
            scen_inventory_df["Coal Code"] = scen
            scen_inventory_df = scen_inventory_df.merge(
                inventory_flow_info,
                on=["FlowUUID"],
                how="left"
            )
            missing_scens_df_list.append(scen_inventory_df)

    # Concatenate all inventories for the proxy-filled scenarios.
    missing_scens_df = pd.concat(missing_scens_df_list).reset_index(drop=True)

    # Create an LCI for the proxy-filled scenarios.
    #   Note, there are NaN rows for waste scenarios.
    missing_scens_merge = coal_input_eia.loc[
        coal_input_eia["coal_source_code"].isin(missing_scens),
        :].merge(
            missing_scens_df,
            left_on=["coal_source_code"],
            right_on=["Coal Code"],
            how="left"
        )

    # Concatenate the proxy-filled scenario LCI with the existing LCI.
    #   Note, you still get NaN entries for waste scenarios.
    coal_mining_inventory_df = pd.concat(
        [existing_scens_merge, missing_scens_merge],
        sort=False
    ).reset_index(drop=True)

    # Calculate the flow amount by multiplying the result (kg per kg of coal)
    # by the quantity (short tons of coal consumed converted to kg).
    coal_mining_inventory_df["FlowAmount"] = (
        pq.convert(1, 'ton', 'kg')
        * coal_mining_inventory_df["Results"].multiply(
            coal_mining_inventory_df['quantity'],
            axis="index")
    )

    # Set the source metadata and limit the necessary columns
    #   Removes heat_input, Results, and Coal Code.
    coal_mining_inventory_df["Source"] = "Mining"
    coal_mining_inventory_df = coal_mining_inventory_df[[
        "plant_id",
        "coal_source_code",
        "quantity",
        "FlowName",
        "FlowAmount",
        "Compartment",
        "input",
        "Source",
        "FlowUUID",
        "ElementaryFlowPrimeContext",
        "Unit",
        "FlowType"
    ]].copy()

    # Read coal transportation data
    coal_transport_df = read_coal_transportation()

    # Quality check facilities in transportation against mining LCI.
    trans_plants = coal_transport_df['plant_id'].unique()
    invent_plants = coal_mining_inventory_df['plant_id'].unique()

    # Check for any inventory plants that don't have transportation LCI.
    # NOTE: this should not occur unless the LCI vintage years are mis-matched.
    missing_plants = [
        int(x) for x in invent_plants if x not in trans_plants]
    num_miss_plants = len(missing_plants)
    if num_miss_plants > 0:
        logging.info(
            "There are %d coal plants without transportation LCI" % (
                num_miss_plants))
        logging.info("Gap-filling with U.S. average transportation LCI.")

        # Create U.S. average inventory for missing plants
        us_ave = get_2023_ave_coal_transport(coal_transport_df, coal_input_eia)
        for plant_id in missing_plants:
            tmp_df = us_ave.copy()
            # Query coal input data for quantity.
            q = coal_input_eia.query("plant_id == %d" % plant_id)
            _quantity = q['quantity'].values[0]
            tmp_df['plant_id'] = plant_id
            tmp_df['quantity'] = _quantity
            coal_transport_df = pd.concat([coal_transport_df, tmp_df])

    # Add transport LCI to mining LCI, set fuel category to coal, and
    # rename columns.
    merged_coal_upstream = pd.concat(
        [coal_mining_inventory_df, coal_transport_df],
        sort=False).reset_index(drop=True)
    merged_coal_upstream['FuelCategory'] = 'COAL'
    merged_coal_upstream.rename(
        columns={
            'coal_source_code': 'stage_code',
            'Source': 'stage'},
        inplace=True
    )

    # Remove facilities with zero coal consumption
    zero_rows = merged_coal_upstream.loc[
        merged_coal_upstream["quantity"]==0, :].index
    merged_coal_upstream.drop(zero_rows, inplace=True)

    # Remove the waste coal scenarios
    merged_coal_upstream = merged_coal_upstream.dropna(subset='FlowName')

    # Sort and reset the data frame and add additional metadata.
    merged_coal_upstream.sort_values(
        ['plant_id','stage','stage_code','Compartment','FlowName'],
        inplace=True
    )
    merged_coal_upstream.reset_index(drop=True, inplace=True)
    merged_coal_upstream["Year"] = year
    merged_coal_upstream["Source"] = "netlcoaleiafuel"

    return merged_coal_upstream


##############################################################################
# MAIN
##############################################################################
if __name__=='__main__':
    from electricitylci.globals import output_dir
    import electricitylci.model_config as config
    config.model_specs = config.build_model_class()

    year=2020
    df = generate_upstream_coal(year)
    if os.path.isdir(output_dir):
        df.to_csv(output_dir+'/coal_emissions_{}.csv'.format(year))
