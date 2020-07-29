#!/usr/bin/python
# -*- coding: utf-8 -*-
if __name__=='__main__':
    import electricitylci.model_config as config
    config.model_specs = config.build_model_class()

import pandas as pd
from electricitylci.globals import data_dir, output_dir
from electricitylci.eia923_generation import eia923_download
import os
from os.path import join
from electricitylci.utils import find_file_in_folder
import requests
import electricitylci.PhysicalQuantities as pq
import numpy as np

coal_type_codes={'BIT': 'B',
                 'LIG': 'L',
                 'SUB': 'S',
                 'WC': 'W',
                 'RC' : 'RC'}
mine_type_codes={'Surface': 'S',
                 'Underground': 'U',
                 'Facility': 'F',
                 'Processing': 'P'}
basin_codes={'Central Appalachia': 'CA',
             'Central Interior': 'CI',
             'Gulf Lignite': 'GL',
             'Illinois Basin': 'IB',
             'Lignite': 'L',
             'Northern Appalachia': 'NA',
             'Powder River Basin': 'PRB',
             'Rocky Mountain': 'RM',
             'Southern Appalachia': 'SA',
             'West/Northwest': 'WNW',
             'Import': 'IMP'}

transport_dict={'Avg Barge Ton*Miles': 'Barge',
                'Avg Lake Vessel Ton*Miles': 'Lake Vessel',
                'Avg Ocean Vessel Ton*Miles': 'Ocean Vessel',
                'Avg Railroad Ton*Miles': 'Railroad',
                'Avg Truck Ton*Miles': 'Truck'}


def eia_7a_download(year, save_path):
    eia7a_base_url = 'http://www.eia.gov/coal/data/public/xls/'
    name = 'coalpublic{}.xls'.format(year)
    url = eia7a_base_url + name
    try:
        os.makedirs(save_path)
        print('Downloading EIA 7-A data...')
        eia_7a_file = requests.get(url)
        # eia_7a_file.retrieve(url,data_dir+'/'+name)
        open(save_path+'/'+name, 'wb').write(eia_7a_file.content)
        # download_unzip(url,data_dir)
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
            file_pattern_match=['2_3_4_5'],
            return_name=True
        )
        eia_fuel_receipts_df = pd.read_excel(
                eia923_path, sheet_name='Page 5 Fuel Receipts and Costs',
                skiprows=4, usecols="A:E,H:M,P:Q")
        csv_fn = eia923_name.split('.')[0] + '_page_5_reduced.csv'
        csv_path = join(expected_923_folder, csv_fn)
        eia_fuel_receipts_df.to_csv(csv_path, index=False)
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
        if csv_file:
            csv_path = os.path.join(expected_923_folder, csv_file[0])
            eia_fuel_receipts_df=pd.read_csv(csv_path, low_memory=False)
        else:
            eia923_path, eia923_name = find_file_in_folder(
                    folder_path=expected_923_folder,
                    file_pattern_match=['2_3_4_5'],
                    return_name=True)
            eia_fuel_receipts_df = pd.read_excel(
                eia923_path, sheet_name='Page 5 Fuel Receipts and Costs',
                skiprows=4, usecols="A:E,H:M,P:Q")
            csv_fn = eia923_name.split('.')[0] + '_page_5_reduced.csv'
            csv_path = join(expected_923_folder, csv_fn)
            eia_fuel_receipts_df.to_csv(csv_path, index=False)
    _clean_columns(eia_fuel_receipts_df)
    return eia_fuel_receipts_df


def _coal_code(row):
    # _coal_code_str = basin_codes[basin] + '-' + coal_type_codes[coal_type] + '-' + mine_type
    coal_code_str=(
            f'{basin_codes[row["netl_basin"]]}-'
            f'{coal_type_codes[row["energy_source"]]}-'
            f'{row["coalmine_type"]}'
    ).upper()
    return coal_code_str


def _transport_code(row):
    transport_str=transport_dict[row['coal_source_code']]
    return transport_str


def generate_upstream_coal_map(year):
    from electricitylci.globals import STATE_ABBREV
    from electricitylci.eia923_generation import eia923_generation_and_fuel
    eia_fuel_receipts_df=read_eia923_fuel_receipts(year)
    expected_7a_folder=join(data_dir, 'f7a_{}'.format(year))
    if not os.path.exists(expected_7a_folder):
        eia_7a_download(year, expected_7a_folder)
        eia7a_path, eia7a_name = find_file_in_folder(
            folder_path=expected_7a_folder,
            file_pattern_match=['coalpublic'],
            return_name=True)
    else:
        eia7a_path, eia7a_name = find_file_in_folder(
            folder_path=expected_7a_folder,
            file_pattern_match=['coalpublic'],
            return_name=True)
    eia7a_df = pd.read_excel(eia7a_path, sheet_name='Hist_Coal_Prod',
                             skiprows=3)
    _clean_columns(eia7a_df)
    coal_criteria = eia_fuel_receipts_df['fuel_group']=='Coal'
    eia_fuel_receipts_df = eia_fuel_receipts_df.loc[coal_criteria, :]
    eia_fuel_receipts_df = eia_fuel_receipts_df.merge(
            eia7a_df[['msha_id', 'coal_supply_region']], how='left',
            left_on='coalmine_msha_id', right_on='msha_id',
            )
    eia_fuel_receipts_df.drop(columns=['msha_id'], inplace=True)
    eia_fuel_receipts_df.rename(
            columns={'coal_supply_region': 'eia_coal_supply_region'},
            inplace=True)
    eia_fuel_receipts_na = eia_fuel_receipts_df.loc[eia_fuel_receipts_df["eia_coal_supply_region"].isnull(), :]
    eia_fuel_receipts_good = eia_fuel_receipts_df.loc[~eia_fuel_receipts_df["eia_coal_supply_region"].isnull(), :]
    county_basin=eia7a_df.groupby(
            by=["mine_state", "mine_county", "coal_supply_region"],
            as_index=False
            )["production_short_tons"].count()
    county_basin["mine_state"]=county_basin["mine_state"].str.replace(r" \(.*\)", "")
    county_basin["mine_state_abv"]=county_basin["mine_state"].str.lower().map(STATE_ABBREV).str.upper()
    county_basin["mine_county"]=county_basin["mine_county"].str.lower()
    fips_codes = pd.read_csv(f"{data_dir}/fips_codes.csv",)
    _clean_columns(fips_codes)
    fips_codes["gu_name"]=fips_codes["gu_name"].str.lower()
    fips_codes["county_fips_code"]=fips_codes["county_fips_code"].astype(str).str.replace(".0", "")
    county_basin=county_basin.merge(
            right=fips_codes[["state_abbreviation", "county_fips_code", "gu_name"]],
            left_on=["mine_state_abv", "mine_county"],
            right_on=["state_abbreviation", "gu_name"],
            how="left"
    )
    county_basin.drop_duplicates(subset=["mine_state_abv", "county_fips_code"], inplace=True)
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
            right=county_basin[["mine_state_abv", "county_fips_code", "coal_supply_region"]],
            left_on=["coalmine_state", "coalmine_county"],
            right_on=["mine_state_abv", "county_fips_code"],
            how="left"
            )
    eia_fuel_receipts_na["eia_coal_supply_region"]=eia_fuel_receipts_na["coal_supply_region"]
    eia_fuel_receipts_made_good=eia_fuel_receipts_na.loc[~eia_fuel_receipts_na["eia_coal_supply_region"].isnull(), :].reset_index(drop=True)
    eia_fuel_receipts_na=eia_fuel_receipts_na.loc[eia_fuel_receipts_na["eia_coal_supply_region"].isnull(), :].reset_index(drop=True)
    eia_fuel_receipts_made_good.drop(columns=["mine_state_abv", "county_fips_code", "coal_supply_region"], inplace=True)
    eia_fuel_receipts_good=pd.concat([eia_fuel_receipts_good, eia_fuel_receipts_made_good], ignore_index=True,sort=False)
    eia_netl_basin = pd.read_csv(data_dir+'/eia_to_netl_basin.csv')
    eia_fuel_receipts_good = eia_fuel_receipts_good.merge(
            eia_netl_basin,
            left_on='eia_coal_supply_region',
            right_on ="eia_basin",
            how="left").reset_index(drop=True)
    state_region_map = pd.read_csv(data_dir+'/coal_state_to_basin.csv')
    eia_fuel_receipts_na = eia_fuel_receipts_na.merge(
            state_region_map[['state', 'basin1', 'basin2']],
            left_on='coalmine_state', right_on='state', how='left')
    eia_fuel_receipts_na.drop(columns=['state'], inplace=True)
    eia_fuel_receipts_na=eia_fuel_receipts_na.merge(
            eia_netl_basin, how='left',
            left_on='eia_coal_supply_region', right_on='eia_basin')
    eia_fuel_receipts_na.drop(columns=['eia_basin'], inplace=True)
    netl_na = (eia_fuel_receipts_na['netl_basin'].isna())
    minimerge = pd.merge(left=eia_fuel_receipts_na,
                         right=eia_netl_basin,
                         left_on='basin1', right_on='eia_basin',
                         how='left')
    minimerge.drop(columns=["mine_state_abv", "county_fips_code", "coal_supply_region", "basin1", "basin2", "netl_basin_x", "eia_basin"], inplace=True)
    minimerge.rename(columns={"netl_basin_y": "netl_basin"}, inplace=True)
    eia_fuel_receipts_good=pd.concat([eia_fuel_receipts_good, minimerge], ignore_index=True,sort=False)

    gulf_lignite = (
            (eia_fuel_receipts_good['energy_source']=='LIG') &
            (eia_fuel_receipts_good['eia_coal_supply_region']=='Interior'))
    eia_fuel_receipts_good.loc[gulf_lignite, ['netl_basin']] = 'Gulf Lignite'
    lignite = ((eia_fuel_receipts_good['energy_source']=='LIG') &
               (eia_fuel_receipts_good['eia_coal_supply_region']=='Western'))
    eia_fuel_receipts_good.loc[lignite, ['netl_basin']]='Lignite'

#    eia_fuel_receipts_df.loc[netl_na,'netl_basin']=(
#            minimerge.loc[netl_na,'netl_basin_y'])
#    eia_fuel_receipts_df[['netl_basin','energy_source','coalmine_type']]
    eia_fuel_receipts_good.dropna(
        subset=['netl_basin', 'energy_source', 'coalmine_type'], inplace=True)
    eia_fuel_receipts_good['coal_source_code']=eia_fuel_receipts_good.apply(
            _coal_code, axis=1)
    eia_fuel_receipts_good['heat_input']=eia_fuel_receipts_good['quantity']*eia_fuel_receipts_good['average_heat_content']
    eia_fuel_receipts_good.drop_duplicates(inplace=True)
    eia_fuel_receipts_good["coal_type"]=eia_fuel_receipts_good["energy_source"].map(coal_type_codes)
    final_df=eia_fuel_receipts_good.groupby(
            ['plant_id', 'coal_type','coal_source_code'],
            )[['quantity', 'heat_input']].sum()
    final_df["mass_fraction"]=final_df[["quantity"]].div(final_df[["quantity"]].groupby(level=[0,1]).transform('sum'))
    final_df["energy_fraction"]=final_df[["heat_input"]].div(final_df[["heat_input"]].groupby(level=[0,1]).transform('sum'))
    final_df["mass_fraction_total"]=final_df[["quantity"]].div(final_df[["quantity"]].groupby(level=[0]).transform('sum'))
    final_df["energy_fraction_total"]=final_df[["heat_input"]].div(final_df[["heat_input"]].groupby(level=[0]).transform('sum'))
    eia_923_gen_fuel=eia923_generation_and_fuel(year)
    eia_923_gen_fuel=eia_923_gen_fuel[["plant_id","reported_fuel_type_code","electric_fuel_consumption_quantity","elec_fuel_consumption_mmbtu"]]
    eia_923_gen_fuel["coal_type"]=eia_923_gen_fuel["reported_fuel_type_code"].map(coal_type_codes)
    eia_923_gen_fuel["plant_id"]=eia_923_gen_fuel["plant_id"].astype(int)
    eia_923_grouped=eia_923_gen_fuel.groupby(by=["plant_id","coal_type"])[["electric_fuel_consumption_quantity","elec_fuel_consumption_mmbtu"]].sum()
    final_df.reset_index(inplace=True)
    eia_923_grouped.reset_index(inplace=True)
    final_df=final_df.merge(eia_923_grouped, on=["plant_id","coal_type"],how="left")
    rc_coal=eia_923_grouped.loc[eia_923_grouped["coal_type"]=="RC",:].set_index("plant_id")
    final_df["rc_coal_quantity"]=final_df["plant_id"].map(rc_coal["electric_fuel_consumption_quantity"])
    final_df["rc_coal_mmbtu"]=final_df["plant_id"].map(rc_coal["elec_fuel_consumption_mmbtu"])
    final_df["new_quantity"]=final_df["mass_fraction"]*final_df["electric_fuel_consumption_quantity"]
    final_df["new_heat_input"]=final_df["energy_fraction"]*final_df["elec_fuel_consumption_mmbtu"]
    final_df.loc[~final_df["rc_coal_quantity"].isna(),"new_quantity"]=final_df["new_quantity"]+final_df["mass_fraction_total"]*final_df["rc_coal_quantity"]
    final_df.loc[~final_df["rc_coal_mmbtu"].isna(),"new_heat_input"]=final_df["new_heat_input"]+final_df["energy_fraction"]*final_df["rc_coal_mmbtu"]
    final_df=final_df[["plant_id","coal_source_code","new_quantity","new_heat_input"]]
    final_df=final_df.rename(columns={
        "new_quantity":"quantity",
        "new_heat_input":"heat_input"
    })
    return final_df


def generate_upstream_coal(year):
    from ast import literal_eval
    """
    Generate the annual coal mining and transportation emissions (in kg) for
    each plant in EIA923.

    Parameters
    ----------
    year: int
        Year of EIA-923 fuel data to use

    Returns
    ----------
    dataframe
    """
#    coal_cleaning_percent = 1

    # Reading the coal input from eia
    coal_input_eia = generate_upstream_coal_map(year)
    # Reading coal transportation data
    coal_transportation = pd.read_csv(data_dir+
                                      '/2016_Coal_Trans_By_Plant_ABB_Data.csv')

    coal_mining_inventory = pd.read_csv(data_dir+'/coal_mining_lci.csv')
    coal_mining_inventory.drop(columns=["@type", "flow.@type"], inplace=True, errors="ignore")
    coal_mining_inventory.rename(columns={
            "flow.categoryPath": "Compartment",
            "flow.name": "FlowName",
            "flow.refUnit": "Unit",
            "flow.flowType": "FlowType",
            "Scenario": "Coal Code",
            "flow.@id": "FlowUUID",
            }, inplace=True)
#    coal_mining_inventory=coal_mining_inventory.loc[coal_mining_inventory["Per_Cleaned"]==coal_cleaning_percent,:]
#    coal_mining_inventory.drop(columns=["Per_Cleaned"],inplace=True)
    coal_mining_inventory["Compartment"]=coal_mining_inventory["Compartment"].apply(literal_eval)
    coal_mining_inventory["Compartment"]=coal_mining_inventory["Compartment"].str.join("/")
    coal_mining_inventory["Compartment"]=coal_mining_inventory["Compartment"].str.replace("Elementary Flows/", "")
    coal_mining_inventory["ElementaryFlowPrimeContext"]=float("nan")
    coal_mining_inventory.loc[coal_mining_inventory["Compartment"].str.contains("emission/"), "ElementaryFlowPrimeContext"]="emission"
    coal_mining_inventory.loc[coal_mining_inventory["Compartment"].str.contains("resource/"), "ElementaryFlowPrimeContext"]="resource"
    coal_mining_inventory.loc[coal_mining_inventory["Compartment"].str.contains("Technosphere"), "ElementaryFlowPrimeContext"]="technosphere"
    coal_mining_inventory.reset_index(drop=True, inplace=True)
#    air_list=[x for x in coal_mining_inventory.index if "emission/air" in coal_mining_inventory.loc[x,"FlowPath"]]
#    water_list=[x for x in coal_mining_inventory.index if "emission/water" in coal_mining_inventory.loc[x,"FlowPath"]]
#    input_list=[x for x in coal_mining_inventory.index if "resource/" in coal_mining_inventory.loc[x,"FlowPath"]]
#    technosphere_list=[x for x in coal_mining_inventory.index if "Technosphere Flows" in coal_mining_inventory.loc[x,"FlowPath"] or "Third party flows" in coal_mining_inventory.loc[x,"FlowPath"]]
#    waste_list=[x for x in coal_mining_inventory.index if "Deposited" in coal_mining_inventory.loc[x,"FlowPath"]]
#    coal_mining_inventory["Compartment"]=float("nan")
#    coal_mining_inventory.loc[technosphere_list,"Compartment"]="technosphere"
#    coal_mining_inventory=coal_mining_inventory.dropna(subset=["Compartment"]).reset_index(drop=True)
    # Reading coal inventory for transportation emissions due transportation
    # (units = kg/ton-mile)
    coal_inventory_transportation = pd.read_excel(
            data_dir+'/Coal_model_transportation_inventory.xlsx',
            sheet_name = 'transportation')
    coal_transportation_flowmapping=pd.read_excel(
            data_dir+'/Coal_model_transportation_inventory.xlsx',
            sheet_name = 'flowmapping')
    # Merge the coal input with the coal mining air emissions dataframe using
    # the coal code (basin-coal_type-mine_type) as the common entity
    coal_input_eia_scens=list(coal_input_eia["coal_source_code"].unique())
    coal_inventory_scens=list(coal_mining_inventory["Coal Code"].unique())
    missing_scens=[x for x in coal_input_eia_scens if x not in coal_inventory_scens]
    # We're going to fill in each missing sccenario with the existing data using
    # weighted averages of current production. Most of these are from processing plants
    # so the average will be between the underground and surface plants in the same region
    # mining the same type of coal. For imports, this will be the weighted average
    # of all of the same type of coal production in the US.
#    missing_scens=list(merged_input_eia_coal_a.loc[merged_input_eia_coal_a["FlowName"].isna(),"coal_source_code"].unique())
    existing_scens_merge=coal_input_eia.loc[~coal_input_eia["coal_source_code"].isin(missing_scens), :].merge(
            coal_mining_inventory,
            left_on=["coal_source_code"],
            right_on=["Coal Code"],
            how="left"
            )
    groupby_cols=["FlowUUID"]

    def wtd_mean(pdser, total_db):
        try:
            wts = total_db.loc[pdser.index, "quantity"]
            result = np.average(pdser, weights=wts)
        except:
#            module_logger.info(
#                f"Error calculating weighted mean for {pdser.name}-"
#                f"{total_db.loc[pdser.index[0],cols]}"
#            )
            result = float("nan")
        return result
    wm = lambda x: wtd_mean(x, existing_scens_merge)
    missing_scens_df_list=[]
    inventory_flow_info=coal_mining_inventory[["FlowUUID","Compartment","FlowType","FlowName","Unit","input","ElementaryFlowPrimeContext"]].drop_duplicates("FlowUUID")
    for scen in missing_scens:
        coals_to_include=None
        if scen.split("-")[0]=="IMP":
            scen_key="-".join(scen.split("-")[1:])
            coals_to_include = [x for x in coal_inventory_scens if scen_key in x]
        elif scen.split("-")[2]=="P":
            scen_key="-".join(scen.split("-")[0:2])
            coals_to_include = [x for x in coal_inventory_scens if scen_key in x]
        if coals_to_include is not None:
            total_scens=len(coals_to_include)
        if total_scens==0 or coals_to_include is None:
            scen_key=scen.split("-")[0]
            coals_to_include = [x for x in coal_inventory_scens if scen_key in x]
        if coals_to_include is not None:
            total_scens=len(coals_to_include)
        if total_scens==0 or coals_to_include is None:
            coals_to_include=["MISSING"]
        target_inventory_df=existing_scens_merge.loc[existing_scens_merge["coal_source_code"].isin(coals_to_include)]
        scen_inventory_df=target_inventory_df.groupby(by=groupby_cols, as_index=False).agg(
                {
                        "p50":wm
                        }
                )
        scen_inventory_df["Coal Code"]=scen
        scen_inventory_df=scen_inventory_df.merge(
                inventory_flow_info,
                on=["FlowUUID"],
                how="left"
                )
        missing_scens_df_list.append(scen_inventory_df)
    missing_scens_df=pd.concat(missing_scens_df_list).reset_index(drop=True)
    missing_scens_merge=coal_input_eia.loc[coal_input_eia["coal_source_code"].isin(missing_scens),:].merge(
            missing_scens_df,
            left_on=["coal_source_code"],
            right_on=["Coal Code"],
            how="left"
            )
    
    coal_mining_inventory_df = pd.concat([existing_scens_merge,missing_scens_merge],sort=False).reset_index(drop=True)
    
    
    # Multiply coal mining emission factor by coal quantity; 
    # convert to kg - coal input in tons (US)
    coal_mining_inventory_df["FlowAmount"] = (
            pq.convert(1,'ton','kg')*
            coal_mining_inventory_df["p50"].multiply(
                    coal_mining_inventory_df['quantity'],axis = "index")
            )
    
    coal_mining_inventory_df["Source"]="Mining"
    coal_mining_inventory_df=coal_mining_inventory_df[["plant_id","coal_source_code","quantity","FlowName","FlowAmount","Compartment","input","Source","FlowUUID","ElementaryFlowPrimeContext","Unit","FlowType"]]
    # Keep the plant ID and air emissions columns
#    merged_input_eia_coal_a = merged_input_eia_coal_a[
#            ['plant_id','coal_source_code','quantity'] + column_air_emission]
    
    # Groupby the plant ID since some plants have multiple row entries 
    # (receive coal from multiple basins)
#    merged_input_eia_coal_grouped = (
#            merged_input_eia_coal_a.groupby(
#                    by=['plant_id','coal_source_code'],
#                    as_index=False)[['quantity',"FlowAmount"]].sum())
#    merged_input_eia_coal_grouped = merged_input_eia_coal_a.reset_index(drop=True)
    
    # Melting the database on Plant ID
#    melted_database_air = merged_input_eia_coal_a.melt(
#            id_vars = ['plant_id','coal_source_code','quantity'], 
#            var_name = 'FlowName', 
#            value_name = 'FlowAmount')
    
    
    # Repeat the same methods for emissions from transportation 
    coal_transportation = coal_transportation.melt(
            'Plant Government ID',var_name = 'Transport')
    coal_transportation["value"]=coal_transportation["value"]*pq.convert(1,"ton","kg")*pq.convert(1,"mi","km")
    merged_transport_coal = coal_transportation.merge(
            coal_inventory_transportation, 
            left_on = ['Transport'], 
            right_on =['Modes'],
            how = 'left')
    
    # multiply transportation emission factor (kg/kg-mi) by total transportation
    # (ton-miles)
    column_air_emission=[x for x in coal_inventory_transportation.columns[1:] if "Unnamed" not in x]
    merged_transport_coal[column_air_emission] = (
            # pq.convert(1,'ton','kg')*
            merged_transport_coal[column_air_emission].multiply(
                    merged_transport_coal['value'],axis = "index"))
    
    merged_transport_coal.rename(columns={'Plant Government ID':'plant_id'},
                                 inplace=True)
    
    # Groupby the plant ID since some plants have multiple row entries 
    # (receive coal from multiple basins)
    merged_transport_coal= merged_transport_coal.groupby(
            ['plant_id','Transport'])[['value']+column_air_emission].sum()
    merged_transport_coal= merged_transport_coal.reset_index()
    
    # Keep the plant ID and emissions columns
    merged_transport_coal = (
            merged_transport_coal[
                    ['plant_id','Transport','value'] + column_air_emission])
    merged_transport_coal.rename(columns={'Transport':'coal_source_code',
                                          'value':'quantity'},
                                 inplace=True)
    # Melting the database on Plant ID
    melted_database_transport = merged_transport_coal.melt(
            id_vars = ['plant_id','coal_source_code','quantity'], 
            var_name = 'FlowName', 
            value_name = 'FlowAmount')
    melted_database_transport['coal_source_code']=melted_database_transport.apply(
            _transport_code,axis=1)
    # Adding to new columns for the compartment (water) and 
    # The source of the emissisons (mining). 
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
    melted_database_transport.drop(columns=["FlowName","Compartment","flowname","compartment"],errors="ignore",inplace=True)
    melted_database_transport.rename(
            columns={
                    "TargetFlowUUID":"FlowUUID",
                    "TargetFlowContext":"Compartment",
                    "TargetUnit":"Unit",
                    "TargetFlowName":"FlowName"
                    },
            inplace=True
            )
    melted_database_transport["input"]=False
    merged_coal_upstream = pd.concat([coal_mining_inventory_df, 
                                      melted_database_transport],sort=False).reset_index(drop=True)
    merged_coal_upstream['FuelCategory']='COAL'
    merged_coal_upstream.rename(columns={
            'coal_source_code':'stage_code',
            'Source':'stage'
            },inplace=True)
    zero_rows = merged_coal_upstream.loc[merged_coal_upstream["quantity"]==0,:].index
    merged_coal_upstream.drop(zero_rows,inplace=True)
    merged_coal_upstream.sort_values(
            ['plant_id','stage','stage_code','Compartment','FlowName'],
            inplace=True)
    merged_coal_upstream.reset_index(drop=True,inplace=True)
    merged_coal_upstream["Year"]=year
    merged_coal_upstream["Source"]="netl"
    return merged_coal_upstream

if __name__=='__main__':
    year=2016
    df = generate_upstream_coal(year)
    df.to_csv(output_dir+'/coal_emissions_{}.csv'.format(year))
