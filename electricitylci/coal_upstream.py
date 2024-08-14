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
from electricitylci.eia923_generation import eia923_download  # +model_specs
from electricitylci.eia923_generation import eia923_generation_and_fuel
import electricitylci.PhysicalQuantities as pq
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

Last updated:
    2024-08-02
"""


##############################################################################
# GLOBALS
##############################################################################
coal_type_codes = {
    'BIT': 'B',
    'LIG': 'L',
    'SUB': 'S',
    'WC': 'W',
    'RC' : 'RC',
}
mine_type_codes = {
    'Surface': 'S',
    'Underground': 'U',
    'Facility': 'F',
    'Processing': 'P',
}
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
transport_dict = {
    'Avg Barge Ton*Miles': 'Barge',
    'Avg Lake Vessel Ton*Miles': 'Lake Vessel',
    'Avg Ocean Vessel Ton*Miles': 'Ocean Vessel',
    'Avg Railroad Ton*Miles': 'Railroad',
    'Avg Truck Ton*Miles': 'Truck',
}


##############################################################################
# FUNCTIONS
##############################################################################
def _clean_columns(df):
   """Remove special characters and convert column names to snake case."""
   df.columns = (
       df.columns.str.lower().str.replace(
           '[^0-9a-zA-Z\-]+', ' ',
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


def _transport_code(row):
    """Generate a transport code based on coal source code."""
    transport_str = transport_dict[row['coal_source_code']]

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


def read_eia923_fuel_receipts(year):
    """Return data frame of EIA 923 coal receipts.

    Parameters
    ----------
    year : int
        The year associated with Form EIA 923.

    Returns
    -------
    pandas.DataFrame
        A data frame with monthly plant info. Columns include:

        - 'year' (int)
        - 'month' (int)
        - 'plant_id' (int)
        - 'plant_name' (str)
        - 'plant_state' (str)
        - 'energy_source' (str)
        - 'fuel_group' (str)
        - 'coalmine_type' (str)
        - 'coalmine_state' (str)
        - 'coalmine_county' (str)
        - 'coalmine_msha_id' (str)
        - 'quantity' (int)
        - 'average_heat_content' (float)
    """
    expected_923_folder = os.path.join(paths.local_path, 'f923_{}'.format(year))
    if not os.path.exists(expected_923_folder):
        logging.info('Downloading EIA-923 files')
        eia923_download(year=year, save_path=expected_923_folder)
        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match=['2_3_4_5'],
            return_name=True
        )
        eia_fuel_receipts_df = pd.read_excel(
            eia923_path,
            sheet_name='Page 5 Fuel Receipts and Costs',
            skiprows=4,
            usecols="A:E,H:M,P:Q")
        csv_fn = eia923_name.split('.')[0] + '_page_5_reduced.csv'
        csv_path = os.path.join(expected_923_folder, csv_fn)
        eia_fuel_receipts_df.to_csv(csv_path, index=False)
    else:
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        logging.info('Loading data from previously downloaded excel file')
        all_files = os.listdir(expected_923_folder)
        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        # NOTE: isn't this what `find_in_folder` does?
        csv_file = [
            f for f in all_files if '.csv' in f and '_page_5_reduced.csv' in f]
        if csv_file:
            csv_path = os.path.join(expected_923_folder, csv_file[0])
            eia_fuel_receipts_df=pd.read_csv(csv_path, low_memory=False)
        else:
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
            eia_fuel_receipts_df.to_csv(csv_path, index=False)
    eia_fuel_receipts_df = _clean_columns(eia_fuel_receipts_df)

    return eia_fuel_receipts_df


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

        - plant_id (int)
        - coal_source_code (str)
        - quantity (float)
        - heat_input (float)

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
    eia_fuel_receipts_df = read_eia923_fuel_receipts(year)
    expected_7a_folder = os.path.join(paths.local_path, 'f7a_{}'.format(year))
    if not os.path.exists(expected_7a_folder):
        eia_7a_download(year, expected_7a_folder)
        eia7a_path = find_file_in_folder(
            folder_path=expected_7a_folder,
            file_pattern_match=['coalpublic'],
            return_name=False)
    else:
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
    coal_criteria = eia_fuel_receipts_df['fuel_group']=='Coal'
    eia_fuel_receipts_df = eia_fuel_receipts_df.loc[coal_criteria, :]
    # Add coal supply region
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
    # Find where coal supply regions failed to match
    eia_fuel_receipts_na = eia_fuel_receipts_df.loc[
        eia_fuel_receipts_df["eia_coal_supply_region"].isnull(), :]
    eia_fuel_receipts_good = eia_fuel_receipts_df.loc[
        ~eia_fuel_receipts_df["eia_coal_supply_region"].isnull(), :]

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
    # Only 'Refuse Recovery' is unmatched.
    county_basin["mine_state_abv"] = county_basin[
        "mine_state"].str.lower().map(STATE_ABBREV).str.upper()
    county_basin["mine_county"] = county_basin["mine_county"].str.lower()

    fips_codes = pd.read_csv(os.path.join(data_dir, "fips_codes.csv"))
    fips_codes = _clean_columns(fips_codes)
    fips_codes["gu_name"] = fips_codes["gu_name"].str.lower()
    # Convert county FIPS code to string
    fips_codes["county_fips_code"] = fips_codes[
        "county_fips_code"].astype(str).str.replace(".0", "", regex=False)

    # Match state-county names
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
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
        right=county_basin[[
            "mine_state_abv", "county_fips_code", "coal_supply_region"]],
        left_on=["coalmine_state", "coalmine_county"],
        right_on=["mine_state_abv", "county_fips_code"],
        how="left"
    )
    eia_fuel_receipts_na["eia_coal_supply_region"] = eia_fuel_receipts_na[
        "coal_supply_region"]
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

    eia_netl_basin = pd.read_csv(
        os.path.join(data_dir, "eia_to_netl_basin.csv")
    )
    eia_fuel_receipts_good = eia_fuel_receipts_good.merge(
        eia_netl_basin,
        left_on='eia_coal_supply_region',
        right_on ="eia_basin",
        how="left").reset_index(drop=True)

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
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
        eia_netl_basin,
        how='left',
        left_on='eia_coal_supply_region',
        right_on='eia_basin'
    )
    eia_fuel_receipts_na.drop(columns=['eia_basin'], inplace=True)
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
    eia_fuel_receipts_good = pd.concat(
        [eia_fuel_receipts_good, minimerge],
        ignore_index=True,
        sort=False
    )

    gulf_lignite = (
        (eia_fuel_receipts_good['energy_source']=='LIG') &
        (eia_fuel_receipts_good['eia_coal_supply_region']=='Interior')
    )
    eia_fuel_receipts_good.loc[gulf_lignite, ['netl_basin']] = 'Gulf Lignite'

    lignite = ((eia_fuel_receipts_good['energy_source']=='LIG') &
               (eia_fuel_receipts_good['eia_coal_supply_region']=='Western'))
    eia_fuel_receipts_good.loc[lignite, ['netl_basin']] = 'Lignite'

    eia_fuel_receipts_good.dropna(
        subset=['netl_basin', 'energy_source', 'coalmine_type'],
        inplace=True
    )
    eia_fuel_receipts_good['coal_source_code'] = eia_fuel_receipts_good.apply(
        _coal_code,
        axis=1
    )
    eia_fuel_receipts_good['heat_input'] = eia_fuel_receipts_good[
        'quantity'] * eia_fuel_receipts_good['average_heat_content']
    eia_fuel_receipts_good.drop_duplicates(inplace=True)
    eia_fuel_receipts_good["coal_type"] = eia_fuel_receipts_good[
        "energy_source"].map(coal_type_codes)

    final_df = eia_fuel_receipts_good.groupby(
        ['plant_id', 'coal_type','coal_source_code'],
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

    eia_923_gen_fuel = eia923_generation_and_fuel(year)
    eia_923_gen_fuel = eia_923_gen_fuel[[
        "plant_id","reported_fuel_type_code",
        "electric_fuel_consumption_quantity",
        "elec_fuel_consumption_mmbtu"
    ]]
    eia_923_gen_fuel["coal_type"] = eia_923_gen_fuel[
        "reported_fuel_type_code"].map(coal_type_codes)
    eia_923_gen_fuel["plant_id"] = eia_923_gen_fuel["plant_id"].astype(int)

    eia_923_grouped = eia_923_gen_fuel.groupby(
        by=["plant_id","coal_type"])[[
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
        "new_quantity","new_heat_input"
    ]]
    final_df = final_df.rename(
        columns = {
            "new_quantity": "quantity",
            "new_heat_input": "heat_input"}
    )

    return final_df


def generate_upstream_coal(year):
    """
    Generate the annual coal mining and transportation emissions (in kg) for
    each plant in EIA923.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use

    Returns
    ----------
    pandas.DataFrame

    Notes
    -----
    Address the pandas FutureWarning:

        coal_upstream.py:374: FutureWarning: Setting an item of incompatible
        dtype is deprecated and will raise in a future error of pandas. Value
        'emission' has dtype incompatible with float64, please explicitly cast
        to a compatible dtype first.
        minimerge.drop(
    """
    # Read the coal input from eia
    coal_input_eia = generate_upstream_coal_map(year)
    # Read coal transportation and mining data
    coal_transportation = pd.read_csv(
        os.path.join(data_dir, '2016_Coal_Trans_By_Plant_ABB_Data.csv')
    )
    coal_mining_inventory = pd.read_csv(
        os.path.join(data_dir, 'coal_mining_lci.csv')
    )
    coal_mining_inventory.drop(
        columns=["@type", "flow.@type"],
        inplace=True,
        errors="ignore"
    )
    coal_mining_inventory.rename(
        columns={
            "flow.categoryPath": "Compartment",
            "flow.name": "FlowName",
            "flow.refUnit": "Unit",
            "flow.flowType": "FlowType",
            "Scenario": "Coal Code",
            "flow.@id": "FlowUUID",
        }, inplace=True
    )
    coal_mining_inventory["Compartment"] = coal_mining_inventory[
        "Compartment"].apply(literal_eval)
    coal_mining_inventory["Compartment"] = coal_mining_inventory[
        "Compartment"].str.join("/")
    coal_mining_inventory["Compartment"] = coal_mining_inventory[
        "Compartment"].str.replace("Elementary Flows/", "", regex=False)
    # HOTFIX data type incompatibility [2024-01-09; TWD]
    coal_mining_inventory["ElementaryFlowPrimeContext"] = ""
    coal_mining_inventory.loc[
        coal_mining_inventory["Compartment"].str.contains("emission/"),
        "ElementaryFlowPrimeContext"
    ] = "emission"
    coal_mining_inventory.loc[
        coal_mining_inventory["Compartment"].str.contains("resource/"),
        "ElementaryFlowPrimeContext"
    ] = "resource"
    coal_mining_inventory.loc[
        coal_mining_inventory["Compartment"].str.contains("Technosphere"),
        "ElementaryFlowPrimeContext"
    ] = "technosphere"
    coal_mining_inventory.reset_index(drop=True, inplace=True)
    # Reading coal inventory for transportation emissions due transportation
    # (units = kg/ton-mile)

    coal_inventory_transportation = pd.read_excel(
        os.path.join(data_dir, 'Coal_model_transportation_inventory.xlsx'),
        sheet_name='transportation'
    )
    coal_transportation_flowmapping = pd.read_excel(
        os.path.join(data_dir, 'Coal_model_transportation_inventory.xlsx'),
        sheet_name='flowmapping'
    )
    # Merge the coal input with the coal mining air emissions dataframe using
    # the coal code (basin-coal_type-mine_type) as the common entity
    coal_input_eia_scens = list(coal_input_eia["coal_source_code"].unique())
    coal_inventory_scens = list(coal_mining_inventory["Coal Code"].unique())
    missing_scens = [
        x for x in coal_input_eia_scens if x not in coal_inventory_scens]

    # We're going to fill in each missing scenario with the existing data
    # using weighted averages of current production. Most of these are from
    # processing plants so the average will be between the underground and
    # surface plants in the same region mining the same type of coal. For
    # imports, this will be the weighted average of all of the same type of
    # coal production in the US.
    existing_scens_merge = coal_input_eia.loc[
        ~coal_input_eia["coal_source_code"].isin(missing_scens),
        :].merge(
            coal_mining_inventory,
            left_on=["coal_source_code"],
            right_on=["Coal Code"],
            how="left"
        )
    groupby_cols=["FlowUUID"]

    # Define a weighted mean method
    def wtd_mean(pdser, total_db):
        try:
            wts = total_db.loc[pdser.index, "quantity"]
            result = np.average(pdser, weights=wts)
        except:
            result = float("nan")

        return result

    wm = lambda x: wtd_mean(x, existing_scens_merge)
    missing_scens_df_list = []
    inventory_flow_info = coal_mining_inventory[[
        "FlowUUID",
        "Compartment",
        "FlowType",
        "FlowName",
        "Unit",
        "input",
        "ElementaryFlowPrimeContext"
    ]].drop_duplicates("FlowUUID")
    for scen in missing_scens:
        coals_to_include = None
        if scen.split("-")[0] == "IMP":
            scen_key = "-".join(scen.split("-")[1:])
            coals_to_include = [
                x for x in coal_inventory_scens if scen_key in x]
        elif scen.split("-")[2] == "P":
            scen_key = "-".join(scen.split("-")[0:2])
            coals_to_include = [
                x for x in coal_inventory_scens if scen_key in x]
        if coals_to_include is not None:
            total_scens = len(coals_to_include)
        if total_scens==0 or coals_to_include is None:
            scen_key = scen.split("-")[0]
            coals_to_include = [
                x for x in coal_inventory_scens if scen_key in x]
        if coals_to_include is not None:
            total_scens = len(coals_to_include)
        if total_scens==0 or coals_to_include is None:
            coals_to_include = ["MISSING"]
        target_inventory_df = existing_scens_merge.loc[
            existing_scens_merge["coal_source_code"].isin(coals_to_include)]
        scen_inventory_df = target_inventory_df.groupby(
            by=groupby_cols,
            as_index=False).agg({"p50": wm})
        scen_inventory_df["Coal Code"] = scen
        scen_inventory_df=scen_inventory_df.merge(
            inventory_flow_info,
            on=["FlowUUID"],
            how="left"
        )
        missing_scens_df_list.append(scen_inventory_df)
    missing_scens_df = pd.concat(missing_scens_df_list).reset_index(drop=True)
    missing_scens_merge = coal_input_eia.loc[
        coal_input_eia["coal_source_code"].isin(missing_scens),
        :].merge(
            missing_scens_df,
            left_on=["coal_source_code"],
            right_on=["Coal Code"],
            how="left"
        )
    coal_mining_inventory_df = pd.concat(
        [existing_scens_merge, missing_scens_merge],
        sort=False).reset_index(drop=True)

    # Multiply coal mining emission factor by coal quantity;
    # convert to kg - coal input in tons (US)
    coal_mining_inventory_df["FlowAmount"] = (
        pq.convert(1, 'ton', 'kg')
        * coal_mining_inventory_df["p50"].multiply(
            coal_mining_inventory_df['quantity'],
            axis="index")
    )

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
    ]]

    # Repeat the same methods for emissions from transportation
    coal_transportation = coal_transportation.melt(
        'Plant Government ID',
        var_name = 'Transport'
    )
    coal_transportation["value"] = coal_transportation["value"] * pq.convert(
        1, "ton", "kg") * pq.convert(1, "mi", "km")
    merged_transport_coal = coal_transportation.merge(
        coal_inventory_transportation,
        left_on=['Transport'],
        right_on=['Modes'],
        how='left'
    )

    # multiply transportation emission factor (kg/kg-mi) by total transportation
    # (ton-miles)
    column_air_emission=[
        x for x in coal_inventory_transportation.columns[1:]
        if "Unnamed" not in x]
    merged_transport_coal[column_air_emission] = (
        merged_transport_coal[column_air_emission].multiply(
            merged_transport_coal['value'], axis="index"
        )
    )
    merged_transport_coal.rename(
        columns={'Plant Government ID': 'plant_id'},
        inplace=True
    )

    # Groupby the plant ID since some plants have multiple row entries
    # (receive coal from multiple basins)
    merged_transport_coal= merged_transport_coal.groupby(
        ['plant_id','Transport'])[['value'] + column_air_emission].sum()
    merged_transport_coal = merged_transport_coal.reset_index()

    # Keep the plant ID and emissions columns
    merged_transport_coal = (
        merged_transport_coal[
            ['plant_id','Transport','value'] + column_air_emission]
    )
    merged_transport_coal.rename(
        columns={
            'Transport': 'coal_source_code',
            'value': 'quantity'},
        inplace=True
    )
    # Melting the database on Plant ID
    melted_database_transport = merged_transport_coal.melt(
        id_vars=['plant_id','coal_source_code','quantity'],
        var_name='FlowName',
        value_name='FlowAmount'
    )
    melted_database_transport[
        'coal_source_code'] = melted_database_transport.apply(
            _transport_code, axis=1)
    # Adding to new columns for the compartment (water) and
    # The source of the emissions (mining).
    melted_database_transport['Compartment'] = 'emission/air'
    melted_database_transport['Source'] = 'Transportation'
    melted_database_transport["ElementaryFlowPrimeContext"]="emission"
    melted_database_transport["FlowType"]="ELEMENTARY_FLOW"
    melted_database_transport=melted_database_transport.merge(
        coal_transportation_flowmapping,
        left_on=["FlowName","Compartment"],
        right_on=["flowname","compartment"],
        how="left"
    )
    melted_database_transport.drop(
        columns=["FlowName","Compartment","flowname","compartment"],
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
    melted_database_transport["input"] = False
    merged_coal_upstream = pd.concat(
        [coal_mining_inventory_df, melted_database_transport],
        sort=False).reset_index(drop=True)
    merged_coal_upstream['FuelCategory'] = 'COAL'
    merged_coal_upstream.rename(
        columns={
            'coal_source_code':'stage_code',
            'Source':'stage'},
        inplace=True
    )
    zero_rows = merged_coal_upstream.loc[
        merged_coal_upstream["quantity"]==0, :].index
    merged_coal_upstream.drop(zero_rows, inplace=True)
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
