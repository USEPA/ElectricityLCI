#%%
# Import python modules

import pandas as pd
import numpy as np
import os
import urllib.request
from electricitylci.globals import output_dir, data_dir
#%%
# Set working directory, files downloaded from EIA will be saved to this location
#os.chdir = 'N:/eLCI/Transmission and Distribution'

#%%
# Define function to extract EIA state-wide electricity profiles and calculate state-wide transmission and distribution losses for the
# user-specified year

def eia_trans_dist_download_extract(year):

    """[This function (1) downloads EIA state-level electricity profiles for all 50 states in the U.S. 
    for a specified year to the working directory, and (2) calculates the transmission and distribution gross grid 
    loss for each state based on statewide 'estimated losses', 'total disposition', and 'direct use'. The final 
    output from this function is a [50x1] dimensional dataframe that contains transmission and distribution gross grid losses for each 
    U.S. state for the specified year. Additional information on the calculation for gross grid loss is provided
    on the EIA website and can be accessed via URL: https://www.eia.gov/tools/faqs/faq.php?id=105&t=3]
    
    Arguments:
        year {[str]} -- [Analysis year]
    """

    eia_trans_dist_loss = pd.DataFrame()
    
    state_abbrev = {
            'alabama': 'al',
            'alaska': 'ak',
            'arizona': 'az',
            'arkansas': 'ar',
            'california': 'ca',
            'colorado': 'co',
            'connecticut': 'ct',
            'delaware': 'de',
            'florida': 'fl',
            'georgia': 'ga',
            'hawaii': 'hi',
            'idaho': 'id',
            'illinois': 'il',
            'indiana': 'in',
            'iowa': 'ia',
            'kansas': 'ks',
            'kentucky': 'ky',
            'louisiana': 'la',
            'maine': 'me',
            'maryland': 'md',
            'massachusetts': 'ma',
            'michigan': 'mi',
            'minnesota': 'mn',
            'mississippi': 'ms',
            'missouri': 'mo',
            'montana': 'mt',
            'nebraska': 'ne',
            'nevada': 'nv',
            'newhampshire': 'nh',
            'newjersey': 'nj',
            'newmexico': 'nm',
            'newyork': 'ny',
            'northcarolina': 'nc',
            'northdakota': 'nd',
            'ohio': 'oh',
            'oklahoma': 'ok',
            'oregon': 'or',
            'pennsylvania': 'pa',
            'rhodeisland': 'ri',
            'southcarolina': 'sc',
            'southdakota': 'sd',
            'tennessee': 'tn',
            'texas': 'tx',
            'utah': 'ut',
            'vermont': 'vt',
            'virginia': 'va',
            'washington': 'wa',
            'westvirginia': 'wv',
            'wisconsin': 'wi',
            'wyoming': 'wy',
    }
    old_path = os.getcwd()
    if os.path.exists(f'{data_dir}/t_and_d_{year}'):
        os.chdir(f'{data_dir}/t_and_d_{year}')
    else:
        os.mkdir(f'{data_dir}/t_and_d_{year}')
        os.chdir(f'{data_dir}/t_and_d_{year}')
    state_df_list = list()
    for key in state_abbrev:
        filename = f'{state_abbrev[key]}.xlsx'
        if not os.path.exists(filename):
            url = (
                    'https://www.eia.gov/electricity/state/archive/' 
                    +  year 
                    + '/' 
                    + key 
                    + '/xls/' 
                    + filename
            )
            print(f'Downloading data for {state_abbrev[key]}')
            urllib.request.urlretrieve(url, filename)
        else:
            print(f'Using previously downloaded data for {state_abbrev[key]}')
        df = pd.read_excel(filename,
                    sheet_name='10. Source-Disposition',
                    header=3,
                    index_col=0)
        df.columns = df.columns.str.replace('Year\n', '')
        df = (df.loc['Estimated losses'] / (df.loc['Total disposition'] - df.loc['Direct use']))
        df = df.to_frame(name = state_abbrev[key])
        state_df_list.append(df)
    eia_trans_dist_loss = pd.concat(state_df_list,axis=1, sort = True)
    eia_trans_dist_loss.columns = eia_trans_dist_loss.columns.str.upper()
    eia_trans_dist_loss = eia_trans_dist_loss.transpose()
    eia_trans_dist_loss = eia_trans_dist_loss[[year]]
    eia_trans_dist_loss.columns = ['t_d_losses']
    os.chdir(old_path)
    return eia_trans_dist_loss

def generate_regional_grid_loss(final_database,year,subregion='all'):
    """This function generates transmission and distribution losses for the 
    provided generation data and given year, aggregated by subregion.
    
    Arguments:
        final_database: dataframe
            The database containing plant-level emissions.
        year: int 
            Analysis year for the transmission and distribution loss data. 
            Ideally this should match the year of your final_database.
    Returns:
        td_by_region: dataframe
            A dataframe of transmission and distribution loss rates as a
            fraction. This dataframe can be used to generate unit processes
            for transmission and distribution to match the regionally-
            aggregated emissions unit processes.
    """
    td_calc_columns=['State','NERC','FuelCategory', 'PrimaryFuel','NERC',
       'Balancing Authority Code','Electricity','Year','Subregion','FRS_ID',
       'eGRID_ID']
    plant_generation=final_database[td_calc_columns].drop_duplicates()
    td_rates = eia_trans_dist_download_extract(f'{year}')
    td_by_plant = pd.merge(
            left=plant_generation,
            right=td_rates,
            left_on='State',
            right_index=True,
            how='left'
    )
    td_by_plant.dropna(subset=['t_d_losses'],inplace=True)
    if subregion == 'all':
        aggregation_column = 'Subregion'
    elif subregion == 'NERC':
        aggregation_column = 'NERC'
    elif subregion == 'BA':
        aggregation_column = 'Balancing Authority Code'
    
    wm = lambda x: np.average(x, weights=td_by_plant.loc[x.index,'Electricity'])
    
    td_by_region = td_by_plant.groupby(aggregation_column, as_index=False).agg(
            {'t_d_losses':wm}
    )
    return td_by_region

if __name__ == '__main__':
    from electricitylci.egrid_filter import (
        #electricity_for_selected_egrid_facilities,
        egrid_facilities_to_include,
        emissions_and_waste_for_selected_egrid_facilities
    )
    from electricitylci.model_config import replace_egrid
    from electricitylci.generation import combine_gen_emissions_data
    year=2016
    from electricitylci.eia923_generation import build_generation_data

    if replace_egrid:
        generation_data = build_generation_data()
    else:
        generation_data = build_generation_data(
            egrid_facilities_to_include=egrid_facilities_to_include
        )
    final_database = combine_gen_emissions_data(
            generation_data,
            emissions_and_waste_for_selected_egrid_facilities,
            subregion='all'
    )
    trans_dist_grid_loss=generate_regional_grid_loss(final_database,year,'BA')
    trans_dist_grid_loss.to_csv(f'{output_dir}/trans_dist_loss_{year}.csv')