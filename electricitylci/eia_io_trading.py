import numpy as np
import os
import pandas as pd
#import eia
from datetime import datetime
import pytz
import json
from os.path import join
import zipfile
import requests
import logging


from electricitylci.globals import data_dir, output_dir
from electricitylci.bulk_eia_data import download_EBA, row_to_df, ba_exchange_to_df
#from electricitylci.bulk_eia_data import (
#    REGION_NAMES,
#    REGION_ACRONYMS
#    )
import electricitylci.eia923_generation as eia923
import electricitylci.eia860_facilities as eia860

from electricitylci.model_config import (
    use_primaryfuel_for_coal,
    fuel_name,
    replace_egrid,
    eia_gen_year,
    # region_column_name,
    model_specs
)
from electricitylci.process_dictionary_writer import *
"""
    Merge generation and emissions data. Add region designations using either
    eGRID or EIA-860. Same for primary fuel by plant (eGRID or 923). Calculate
    and merge in the total generation by region. Create the column "Subregion"
    to hold regional name info. Remove electricity flows. Rename flows and add
    UUIDs according to the federal flow list.

    Parameters
    ----------
    year : int
        Specified year to pull transaction data between balancing authorities
    subregion : str
        Description of a group of regions. Options include 'FERC' for all FERC
        market regions, 'BA' for all balancing authorities.

    Returns
    -------
    DataFrame with import region, export region, transaction amount, total
    imports for import region, and fraction of total

        Sample output:
            ferc_final_trade.head()
              import ferc region export ferc region         value         total  fraction
            0              CAISO              CAISO  2.662827e+08  3.225829e+08  0.825471
            1              CAISO             Canada  1.119572e+06  3.225829e+08  0.003471
            2              CAISO              ERCOT  0.000000e+00  3.225829e+08  0.000000
            3              CAISO             ISO-NE  0.000000e+00  3.225829e+08  0.000000
            4              CAISO               MISO  0.000000e+00  3.225829e+08  0.000000

"""

def ba_io_trading_model(year=None, subregion=None):

    if year is None:
        year = model_specs['NETL_IO_trading_year']
    if subregion is None:
        subregion = model_specs['regional_aggregation']
    if subregion not in ['BA', 'FERC']:
        raise ValueError(
            f'subregion or regional_aggregation must have a value of "BA" or "FERC" '
            f'when calculating trading with input-output, not {subregion}'
        )

    #Read in BAA file which contains the names and abbreviations
    df_BA = pd.read_excel(data_dir + '/BA_Codes_930.xlsx', sheet_name = 'US', header = 4)
    df_BA.rename(columns={'etag ID': 'BA_Acronym', 'Entity Name': 'BA_Name','NCR_ID#': 'NRC_ID', 'Region': 'Region'}, inplace=True)
    BA = pd.np.array(df_BA['BA_Acronym'])
    US_BA_acronyms = df_BA['BA_Acronym'].tolist()

    #Read in BAA file which contains the names and abbreviations
    #Original df_BAA does not include the Canadian balancing authorities
    #Import them here, then concatenate to make a single df_BAA_NA (North America)

    df_BA_CA = pd.read_excel(data_dir + '/BA_Codes_930.xlsx', sheet_name = 'Canada', header = 4)
    df_BA_CA.rename(columns={'etag ID': 'BA_Acronym', 'Entity Name': 'BA_Name','NCR_ID#': 'NRC_ID', 'Region': 'Region'}, inplace=True)
    df_BA_NA = pd.concat([df_BA, df_BA_CA])
    ferc_list = df_BA_NA['FERC_Region_Abbr'].unique().tolist()

    #Read in the bulk data

#    download_EBA()
    path = join(data_dir, 'bulk_data', 'EBA.zip')

    try:
        logging.info("Using existing bulk data download")
        z = zipfile.ZipFile(path, 'r')
        with z.open('EBA.txt') as f:
            raw_txt = f.readlines()
    except FileNotFoundError:
        logging.info("Downloading new bulk data")
        download_EBA()
        z = zipfile.ZipFile(path, 'r')
        with z.open('EBA.txt') as f:
            raw_txt = f.readlines()

    eia923_gen=eia923.build_generation_data(generation_years=[year])
    eia860_df=eia860.eia860_balancing_authority(year)
    eia860_df["Plant Id"]=eia860_df["Plant Id"].astype(int)
    
    eia_combined_df=eia923_gen.merge(eia860_df,
                                     left_on=["FacilityID"],
                                     right_on=["Plant Id"],
                                     how="left")
    eia_gen_ba=eia_combined_df.groupby(by=["Balancing Authority Code"],as_index=False)["Electricity"].sum()
    
    REGION_NAMES = [
        'California', 'Carolinas', 'Central',
        'Electric Reliability Council of Texas, Inc.', 'Florida',
        'Mid-Atlantic', 'Midwest', 'New England ISO',
        'New York Independent System Operator', 'Northwest', 'Southeast',
        'Southwest', 'Tennessee Valley Authority'
    ]

    REGION_ACRONYMS = [
        'TVA', 'MIDA', 'CAL', 'CAR', 'CENT', 'ERCO', 'FLA',
        'MIDW', 'ISNE', 'NYIS', 'NW', 'SE', 'SW',
    ]
    logging.info("Loading json")


#    TOTAL_INTERCHANGE_ROWS = [
#        json.loads(row) for row in raw_txt if b'EBA.TI.H' in row
#    ]

    NET_GEN_ROWS = [
        json.loads(row) for row in raw_txt if b'EBA.NG.H' in row
    ]

#    DEMAND_ROWS = [
#        json.loads(row) for row in raw_txt if b'EBA.D.H' in row
#    ]

    EXCHANGE_ROWS = [
        json.loads(row) for row in raw_txt if b'EBA.ID.H' in row
    ]

    BA_TO_BA_ROWS = [
        row for row in EXCHANGE_ROWS
        if row['series_id'].split('-')[0][4:] not in REGION_ACRONYMS
    ]
    logging.info("Pivoting")
    #Subset for specified eia_gen_year
    start_datetime = '{}-01-01 00:00:00+00:00'.format(year)
    end_datetime = '{}-12-31 23:00:00+00:00'.format(year)

    start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S%z')
    end_datetime = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S%z')

    #Net Generation Data Import

    df_net_gen = row_to_df(NET_GEN_ROWS, 'net_gen')
    df_net_gen = df_net_gen.pivot(index = 'datetime', columns = 'region', values = 'net_gen')
    ba_cols = US_BA_acronyms

    gen_cols = list(df_net_gen.columns.values)

    gen_cols_set = set(gen_cols)
    ba_ref_set = set(ba_cols)

    col_diff = list(ba_ref_set - gen_cols_set)
    col_diff.sort(key = str.upper)


    #Add in missing columns, then sort in alphabetical order
    for i in col_diff:
        df_net_gen[i] = 0

    #Keep only the columns that match the balancing authority names, there are several other columns included in the dataset
    #that represent states (e.g., TEX, NY, FL) and other areas (US48)

    df_net_gen = df_net_gen[ba_cols]

    #Resort columns so the headers are in alpha order
    df_net_gen = df_net_gen.sort_index(axis=1)
    df_net_gen = df_net_gen.fillna(value = 0)

    df_net_gen = df_net_gen.loc[start_datetime:end_datetime]


    #Sum values in each column
    df_net_gen_sum = df_net_gen.sum(axis = 0).to_frame()
    logging.info("Reading canadian import data")
    #Add Canadian import data to the net generation dataset, concatenate and put in alpha order
    df_CA_Imports_Gen = pd.read_csv(data_dir + '/CA_Imports_Gen.csv', index_col = 0)
    df_CA_Imports_Gen = df_CA_Imports_Gen[str(year)]



    df_net_gen_sum = pd.concat([df_net_gen_sum,df_CA_Imports_Gen]).sum(axis=1)
    df_net_gen_sum = df_net_gen_sum.to_frame()
    df_net_gen_sum = df_net_gen_sum.sort_index(axis=0)
    
    #Check the net generation of each Balancing Authority against EIA 923 data.
    #If the percent change of a given area is greater than the mean absolute difference
    #of all of the areas, it will be treated as an error and replaced with the 
    #value in EIA923.
    net_gen_check=df_net_gen_sum.merge(
            right=eia_gen_ba,
            left_index=True,
            right_on=["Balancing Authority Code"],
            how="left"
            ).reset_index()
    net_gen_check["diff"]=abs(net_gen_check["Electricity"]-net_gen_check[0])/net_gen_check[0]
    diff_mad=net_gen_check["diff"].mad()
    net_gen_swap=net_gen_check.loc[net_gen_check["diff"]>diff_mad,["Balancing Authority Code","Electricity"]].set_index("Balancing Authority Code")
    df_net_gen_sum.loc[net_gen_swap.index,[0]]=np.nan
    net_gen_swap.rename(columns={"Electricity":0},inplace=True)
    df_net_gen_sum=df_net_gen_sum.combine_first(net_gen_swap)
    #First work on the trading data from the 'df_trade_all_stack_2016' frame
    #This cell does the following:
    # 1. reformats the data to an annual basis
    # 2. formats the BA names in the corresponding columns
    # 3. evalutes the trade values from both BA perspectives
    #  (e.g. BA1 as exporter and importer in a transaction with BA2)
    # 4. evaluates the trading data for any results that don't make sense
    #       a. both BAs designate as importers (negative value)
    #       b. both BAs designate as exporters (postive value)
    #       c. one of the BAs in the transation reports a zero value and the other is nonzero
    # 5. calulate the percent difference in the transaction values reports by BAs
    # 6. final exchange value based on logic;
    #       a. if percent diff is less than 20%, take mean,
    #       b. if not use the value as reported by the exporting BAA
    #       c. designate each BA in the transaction either as the importer or exporter
    # Output is a pivot with index (rows) representing exporting BAs,
    #   columns representing importing BAs, and values for the traded amount

    #Group and resample trading data so that it is on an annual basis


    df_ba_trade = ba_exchange_to_df(BA_TO_BA_ROWS, data_type='ba_to_ba')
    df_ba_trade = df_ba_trade.set_index('datetime')
    df_ba_trade['transacting regions'] = df_ba_trade['from_region'] + '-' + df_ba_trade['to_region']


    #Keep only the columns that match the balancing authority names, there are several other columns included in the dataset
    #that represent states (e.g., TEX, NY, FL) and other areas (US48)
    filt1 = df_ba_trade['from_region'].isin(ba_cols)
    filt2 = df_ba_trade['to_region'].isin(ba_cols)
    filt = filt1 & filt2
    df_ba_trade = df_ba_trade[filt]

    #Subset for eia_gen_year, need to pivot first because of non-unique datetime index
    df_ba_trade_pivot = df_ba_trade.pivot(columns = 'transacting regions', values = 'ba_to_ba')

    df_ba_trade_pivot = df_ba_trade_pivot.loc[start_datetime:end_datetime]

    #Sum columns - represents the net transactced amount between the two BAs
    df_ba_trade_sum = df_ba_trade_pivot.sum(axis = 0).to_frame()
    df_ba_trade_sum = df_ba_trade_sum.reset_index()
    df_ba_trade_sum.columns = ['BAAs','Exchange']

    #Split BAA string into exporting and importing BAA columns
    df_ba_trade_sum['BAA1'], df_ba_trade_sum['BAA2'] = df_ba_trade_sum['BAAs'].str.split('-', 1).str
    df_ba_trade_sum = df_ba_trade_sum.rename(columns={'BAAs': 'Transacting BAAs'})

    #Create two perspectives - import and export to use for comparison in selection of the final exchange value between the BAAs
    df_trade_sum_1_2 = df_ba_trade_sum.groupby(['BAA1', 'BAA2','Transacting BAAs'], as_index=False)[['Exchange']].sum()
    df_trade_sum_2_1 = df_ba_trade_sum.groupby(['BAA2', 'BAA1', 'Transacting BAAs'], as_index=False)[['Exchange']].sum()
    df_trade_sum_1_2.columns = ['BAA1_1_2', 'BAA2_1_2','Transacting BAAs_1_2', 'Exchange_1_2']
    df_trade_sum_2_1.columns = ['BAA2_2_1', 'BAA1_2_1','Transacting BAAs_2_1', 'Exchange_2_1']

    #Combine two grouped tables for comparison for exchange values
    df_concat_trade = pd.concat([df_trade_sum_1_2,df_trade_sum_2_1], axis = 1)
    df_concat_trade['Exchange_1_2_abs'] = df_concat_trade['Exchange_1_2'].abs()
    df_concat_trade['Exchange_2_1_abs'] = df_concat_trade['Exchange_2_1'].abs()

    #Create new column to check if BAAs designate as either both exporters or both importers
    #or if one of the entities in the transaction reports a zero value
    #Drop combinations where any of these conditions are true, keep everything else
    df_concat_trade['Status_Check'] = np.where(((df_concat_trade['Exchange_1_2'] > 0) & (df_concat_trade['Exchange_2_1'] > 0)) \
                   |((df_concat_trade['Exchange_1_2'] < 0) & (df_concat_trade['Exchange_2_1'] < 0)) \
                   | ((df_concat_trade['Exchange_1_2'] == 0) | (df_concat_trade['Exchange_2_1'] == 0)), 'drop', 'keep')

    #Calculate the difference in exchange values
    df_concat_trade['Delta'] = df_concat_trade['Exchange_1_2_abs'] - df_concat_trade['Exchange_2_1_abs']

    #Calculate percent diff of exchange_abs values - this can be down two ways:
    #relative to 1_2 exchange or relative to 2_1 exchange - perform the calc both ways
    #and take the average
    df_concat_trade['Percent_Diff_Avg']= ((abs((df_concat_trade['Exchange_1_2_abs']/df_concat_trade['Exchange_2_1_abs'])-1)) \
        + (abs((df_concat_trade['Exchange_2_1_abs']/df_concat_trade['Exchange_1_2_abs'])-1)))/2

    #Mean exchange value
    df_concat_trade['Exchange_mean'] = df_concat_trade[['Exchange_1_2_abs', 'Exchange_2_1_abs']].mean(axis=1)

    #Percent diff equations creats NaN where both values are 0, fill with 0
    df_concat_trade['Percent_Diff_Avg'].fillna(0, inplace = True)

    #Final exchange value based on logic; if percent diff is less than 20%, take mean,
    #if not use the value as reported by the exporting BAA. First figure out which BAA is the exporter
    #by checking the value of the Exchance_1_2
    #If that value is positive, it indicates that BAA1 is exported to BAA2; if negative, use the
    #value from Exchange_2_1
    df_concat_trade['Final_Exchange'] = np.where((df_concat_trade['Percent_Diff_Avg'].abs() < 0.2),
                   df_concat_trade['Exchange_mean'],np.where((df_concat_trade['Exchange_1_2'] > 0),
                                  df_concat_trade['Exchange_1_2'],df_concat_trade['Exchange_2_1']))


    #Assign final designation of BAA as exporter or importer based on logical assignment
    df_concat_trade['Export_BAA'] = np.where((df_concat_trade['Exchange_1_2'] > 0), df_concat_trade['BAA1_1_2'],
                   np.where((df_concat_trade['Exchange_1_2'] < 0), df_concat_trade['BAA2_1_2'],''))

    df_concat_trade['Import_BAA'] = np.where((df_concat_trade['Exchange_1_2'] < 0), df_concat_trade['BAA1_1_2'],
                   np.where((df_concat_trade['Exchange_1_2'] > 0), df_concat_trade['BAA2_1_2'],''))

    df_concat_trade = df_concat_trade[df_concat_trade['Status_Check'] == 'keep']


    #Create the final trading matrix; first grab the necessary columns, rename the columns and then pivot
    df_concat_trade_subset = df_concat_trade[['Export_BAA', 'Import_BAA', 'Final_Exchange']]

    df_concat_trade_subset.columns = ['Exporting_BAA', 'Importing_BAA', 'Amount']

    df_trade_pivot = df_concat_trade_subset.pivot_table(index = 'Exporting_BAA', columns = 'Importing_BAA', values = 'Amount').fillna(0)


    # This cell continues formatting the df_trade
    # Find missing BAs - need to add them in so that we have a square matrix
    # Not all BAs are involved in transactions

    trade_cols = list(df_trade_pivot.columns.values)
    trade_rows = list(df_trade_pivot.index.values)

    trade_cols_set = set(trade_cols)
    trade_rows_set = set(trade_rows)
    trade_ba_ref_set = set(ba_cols)

    trade_col_diff = list(trade_ba_ref_set - trade_cols_set)
    trade_col_diff.sort(key = str.upper)

    trade_row_diff = list(trade_ba_ref_set - trade_rows_set)
    trade_row_diff.sort(key=str.upper)

    #Add in missing columns, then sort in alphabetical order
    for i in trade_col_diff:
        df_trade_pivot[i] = 0

    df_trade_pivot = df_trade_pivot.sort_index(axis=1)

    #Add in missing rows, then sort in alphabetical order
    for i in trade_row_diff:
        df_trade_pivot.loc[i,:] = 0

    df_trade_pivot = df_trade_pivot.sort_index(axis=0)

    # Add Canadian Imports to the trading matrix
    # CA imports are specified in an external file
    df_CA_Imports_Cols = pd.read_csv(data_dir + '/CA_Imports_Cols.csv', index_col = 0)

    df_CA_Imports_Rows = pd.read_csv(data_dir + '/CA_Imports_Rows.csv', index_col = 0)
    df_CA_Imports_Rows = df_CA_Imports_Rows[['us_ba', str(year)]]
    df_CA_Imports_Rows = df_CA_Imports_Rows.pivot(columns = 'us_ba', values = str(year))

    df_concat_trade_CA = pd.concat([df_trade_pivot, df_CA_Imports_Rows])
    df_concat_trade_CA = pd.concat([df_concat_trade_CA, df_CA_Imports_Cols], axis = 1)
    df_concat_trade_CA.fillna(0, inplace = True)
    df_trade_pivot = df_concat_trade_CA
    df_trade_pivot = df_trade_pivot.sort_index(axis=0)
    df_trade_pivot = df_trade_pivot.sort_index(axis=1)

    #Perform trading calculations as provided in Qu et al (2018) to
    #determine the composition of a BA consumption mix

    #Create total inflow vector x and then convert to a diagonal matrix x-hat
    logging.info("Inflow vector")
    x = []
    for i in range (len(df_net_gen_sum)):
        x.append(df_net_gen_sum.iloc[i] + df_trade_pivot.sum(axis = 0).iloc[i])

    x_np = np.array(x)

    #If values are zero, x_hat matrix will be singular, set BAAs with 0 to small value (1)
    df_x = pd.DataFrame(data = x_np, index = df_trade_pivot.index)
    df_x = df_x.rename(columns = {0:'inflow'})
    df_x.loc[df_x['inflow'] == 0] = 1

    x_np = df_x.values

    x_hat = np.diagflat(x_np)


    #Create consumption vector c and then convert to a digaonal matrix c-hat
    #Calculate c based on x and T
    logging.info("consumption vector")
    c = []

    for i in range(len(df_net_gen_sum)):
        c.append(x[i] - df_trade_pivot.sum(axis = 1).iloc[i])

    c_np = np.array(c)
    c_hat = np.diagflat(c_np)

    #Convert df_trade_pivot to matrix
    T = df_trade_pivot.values

    #Create matrix to split T into distinct interconnections - i.e., prevent trading between eastern and western interconnects
    #Connections between the western and eastern interconnects are through SWPP and WAUE
    logging.info("Matrix operations")
    interconnect = df_trade_pivot.copy()
    interconnect[:] = 1
    interconnect.loc['SWPP',['EPE', 'PNM', 'PSCO', 'WACM']] = 0
    interconnect.loc['WAUE',['WAUW', 'WACM']] = 0
    interconnect_mat = interconnect.values
    T_split = np.multiply(T, interconnect_mat)

    #Matrix trading math (see Qu et al. 2018 ES&T paper)
    x_hat_inv = np.linalg.inv(x_hat)

    B = np.matmul(T_split, x_hat_inv)

    I = np.identity(len(df_net_gen_sum))

    diff_I_B = I - B

    G = np.linalg.inv(diff_I_B)

    c_hat_x_hat_inv = np.matmul(c_hat, x_hat_inv)

    G_c = np.matmul(G, c_hat)
    H = np.matmul(G,c_hat, x_hat_inv)

    df_G = pd.DataFrame(G)
    df_B = pd.DataFrame(B)
    df_H = pd.DataFrame(H)

    #Convert H to pandas dataframe, populate index and columns

    df_final_trade_out = df_H
    df_final_trade_out.columns = df_net_gen_sum.index
    df_final_trade_out.index = df_net_gen_sum.index

    #Develop trading input for the eLCI code. Need to melt the dataframe to end up with a three column
    #dataframe:Repeat for both possible aggregation levels - BA and FERC market region

    #Establish a threshold of 0.00001 to be included in the final trading matrix
    #Lots of really small values as a result of the matrix calculate (e.g., 2.0e-15)

    df_final_trade_out_filt = df_final_trade_out.copy()
    col_list = df_final_trade_out.columns.tolist()

    for i in col_list:
        df_final_trade_out_filt[i] = np.where(df_final_trade_out[i].abs()/df_final_trade_out[i].sum() < 0.00001, 0, df_final_trade_out[i].abs())

    df_final_trade_out_filt = df_final_trade_out_filt.reset_index()
    df_final_trade_out_filt = df_final_trade_out_filt.rename(columns = {'index':'Source BAA'})

    df_final_trade_out_filt_melted = df_final_trade_out_filt.melt(id_vars = 'Source BAA' , value_vars=col_list)
    df_final_trade_out_filt_melted = df_final_trade_out_filt_melted.rename(columns = {'Source BAA':'export BAA', 'variable':'import BAA'})


    #Merge to bring in import region name matched with BAA
    df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted.merge(df_BA_NA, left_on = 'import BAA', right_on = 'BA_Acronym')
    df_final_trade_out_filt_melted_merge.rename(columns={'FERC_Region': 'import ferc region', 'FERC_Region_Abbr':'import ferc region abbr'}, inplace=True)
    df_final_trade_out_filt_melted_merge.drop(columns = ['BA_Acronym', 'BA_Name', 'NCR ID#', 'EIA_Region', 'EIA_Region_Abbr'], inplace = True)

    #Merge to bring in export region name matched with BAA
    df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted_merge.merge(df_BA_NA, left_on = 'export BAA', right_on = 'BA_Acronym')
    df_final_trade_out_filt_melted_merge.rename(columns={'FERC_Region': 'export ferc region', 'FERC_Region_Abbr':'export ferc region abbr'}, inplace=True)
    df_final_trade_out_filt_melted_merge.drop(columns = ['BA_Acronym', 'BA_Name', 'NCR ID#', 'EIA_Region', 'EIA_Region_Abbr'], inplace = True)

    BAA_import_grouped_tot = df_final_trade_out_filt_melted_merge.groupby(['import BAA'])['value'].sum().reset_index()
    ferc_import_grouped_tot = df_final_trade_out_filt_melted_merge.groupby(['import ferc region'])['value'].sum().reset_index()

    #Develop final df for BAA
    BAA_final_trade = df_final_trade_out_filt_melted_merge.copy()
    BAA_final_trade = BAA_final_trade.drop(columns = ['import ferc region', 'export ferc region', 'import ferc region abbr', 'export ferc region abbr'])
    BAA_final_trade = BAA_final_trade.merge(BAA_import_grouped_tot, left_on = 'import BAA', right_on = 'import BAA')
    BAA_final_trade = BAA_final_trade.rename(columns = {'value_x':'value','value_y':'total'})
    BAA_final_trade['fraction'] = BAA_final_trade['value']/BAA_final_trade['total']
    BAA_final_trade = BAA_final_trade.fillna(value = 0)
    BAA_final_trade = BAA_final_trade.drop(columns = ['value', 'total'])
    #Remove Canadian BAs in import list
    BAA_filt = BAA_final_trade['import BAA'].isin(US_BA_acronyms)
    BAA_final_trade = BAA_final_trade[BAA_filt]
    BAA_final_trade.to_csv(output_dir + '/BAA_final_trade_{}.csv'.format(year))

    #Develop final df for FERC Market Region
    ferc_final_trade = df_final_trade_out_filt_melted_merge.copy()
#    ferc_final_trade = ferc_final_trade.groupby(['import ferc region abbr', 'import ferc region', 'export ferc region','export ferc region abbr'])['value'].sum().reset_index()
    ferc_final_trade = ferc_final_trade.groupby(['import ferc region abbr', 'import ferc region', 'export BAA'])['value'].sum().reset_index()
    ferc_final_trade = ferc_final_trade.merge(ferc_import_grouped_tot, left_on = 'import ferc region', right_on = 'import ferc region')
    ferc_final_trade = ferc_final_trade.rename(columns = {'value_x':'value','value_y':'total'})
    ferc_final_trade['fraction'] = ferc_final_trade['value']/ferc_final_trade['total']
    ferc_final_trade = ferc_final_trade.fillna(value = 0)
    ferc_final_trade = ferc_final_trade.drop(columns = ['value', 'total'])
    #Remove Canadian entry in import list
    ferc_list.remove('CAN')
    ferc_filt = ferc_final_trade['import ferc region abbr'].isin(ferc_list)
    ferc_final_trade = ferc_final_trade[ferc_filt]
    ferc_final_trade.to_csv(output_dir + '/ferc_final_trade_{}.csv'.format(year))

    if subregion == 'BA':
        BAA_final_trade["export_name"]=BAA_final_trade["export BAA"].map(df_BA_NA[["BA_Acronym","BA_Name"]].set_index("BA_Acronym")["BA_Name"])
        BAA_final_trade["import_name"]=BAA_final_trade["import BAA"].map(df_BA_NA[["BA_Acronym","BA_Name"]].set_index("BA_Acronym")["BA_Name"])
        return BAA_final_trade
    elif subregion == 'FERC':
        ferc_final_trade["export_name"]=ferc_final_trade["export BAA"].map(df_BA_NA[["BA_Acronym","BA_Name"]].set_index("BA_Acronym")["BA_Name"])
        return ferc_final_trade


if __name__=='__main__':
    year=2016
    subregion = 'BA'
    df = ba_io_trading_model(year, subregion)









def olca_schema_consumption_mix(database, gen_dict, subregion="BA"):
    import numpy as np
    import pandas as pd

    from electricitylci.model_config import (
        use_primaryfuel_for_coal,
        fuel_name,
        replace_egrid,
        eia_gen_year,
        # region_column_name,
    )
    from electricitylci.generation import eia_facility_fuel_region
    from electricitylci.globals import data_dir, output_dir
    from electricitylci.process_dictionary_writer import (
        exchange_table_creation_ref,
        exchange,
        ref_exchange_creator,
        electricity_at_user_flow,
        electricity_at_grid_flow,
        process_table_creation_con_mix,
        exchange_table_creation_input_con_mix
    )
    import logging

###DELETE NEXT LINE
#    database = cons_mix_df
#    database = database.drop(columns = ['value', 'total'])
#    dist_dict = dist_mix_dict
###DELETE ABOVE

    consumption_mix_dict = {}
    if subregion == "FERC":
        aggregation_column = "import ferc region"
        region = list(pd.unique(database[aggregation_column]))
        export_column = 'export_name'

    elif subregion == "BA":
        aggregation_column = "import_name" #"import BAA"
        region = list(pd.unique(database[aggregation_column]))
        export_column = "export_name"#'export BAA'

    for reg in region:

        database_reg = database.loc[database[aggregation_column] == reg, :
            ]

        exchanges_list = []

        database_filt = database['fraction'] > 0
        database_reg = database_reg[database_filt]

        exchange(exchange_table_creation_ref_cons(database_reg), exchanges_list)


        for export_region in list(database_reg[export_column].unique()):

            database_f1 = database_reg[
                database_reg[export_column] == export_region
            ]
            if database_f1.empty != True:
                ra = exchange_table_creation_input_con_mix(
                    database_f1, export_region
                )
                ra["quantitativeReference"] = False
                ra['amount'] = database_reg.loc[database_reg[export_column] == export_region,'fraction'].values[0]
                matching_dict = None
                for gen in gen_dict:
                    if (
                        gen_dict[gen]["name"]
                        == 'Electricity; at grid; generation mix - ' + export_region
                    ):
                        matching_dict = gen_dict[export_region]
                        break
                if matching_dict is None:
                    logging.warning(
                        f"Trouble matching dictionary for {export_region} - {reg}"
                    )
                else:
                    ra["provider"] = {
                        "name": matching_dict["name"],
                        "@id": matching_dict["uuid"],
                        "category": matching_dict["category"].split("/"),
                    }
                exchange(ra, exchanges_list)
                   # Writing final file
        final = process_table_creation_con_mix(reg, exchanges_list)
        final["name"] = "Electricity; at grid; consumption mix - " + reg
        consumption_mix_dict[reg] = final

    return consumption_mix_dict
