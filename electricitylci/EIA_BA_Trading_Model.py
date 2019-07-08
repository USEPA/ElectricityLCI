
# coding: utf-8

# In[1]:
#required files:
#   1. 'BA_Codes_930.xlsx'
#   2. 'CA_Imports_Rows.csv'
#   3.'CA_Imports_Cols.csv'
#   4. 'CA_Imports_Gen.csv'

import numpy as np
import os
import pandas as pd
import eia
from datetime import datetime
import pytz
import json
from os.path import join
import zipfile
import requests


from electricitylci.globals import data_dir
from electricitylci.bulk_eia_data import download_EBA, row_to_df, ba_exchange_to_df
from electricitylci.bulk_eia_data import (
    REGION_NAMES,
    REGION_ACRONYMS
    )
from electricitylci.model_config import (
    use_primaryfuel_for_coal,
    fuel_name,
    replace_egrid,
    eia_gen_year,
    region_column_name,
)

#%%

#Read in BAA file which contains the names and abbreviations
df_BA = pd.read_excel(data_dir + '/BA_Codes_930.xlsx', sheetname = 'Table 1', header = 4)
df_BA.rename(columns={'etag ID': 'BA_Acronym', 'Entity Name': 'BA_Name','NCR_ID#': 'NRC_ID', 'Region': 'Region'}, inplace=True)
BA = pd.np.array(df_BA['BA_Acronym'])
BA_acronyms = df_BA['BA_Acronym'].tolist()


#Read in the bulk data

download_EBA()
path = join(data_dir, 'bulk_data', 'EBA.zip')

try:
    z = zipfile.ZipFile(path, 'r')
    with z.open('EBA.txt') as f:
        raw_txt = f.readlines()
except FileNotFoundError:
    download_EBA()
    z = zipfile.ZipFile(path, 'r')
    with z.open('EBA.txt') as f:
        raw_txt = f.readlines()


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

TOTAL_INTERCHANGE_ROWS = [
    json.loads(row) for row in raw_txt if b'EBA.TI.H' in row
]

NET_GEN_ROWS = [
    json.loads(row) for row in raw_txt if b'EBA.NG.H' in row
]

DEMAND_ROWS = [
    json.loads(row) for row in raw_txt if b'EBA.D.H' in row
]

EXCHANGE_ROWS = [
    json.loads(row) for row in raw_txt if b'EBA.ID.H' in row
]

BA_TO_BA_ROWS = [
    row for row in EXCHANGE_ROWS
    if row['series_id'].split('-')[0][4:] not in REGION_ACRONYMS
]

#%%
#Subset for specified eia_gen_year
start_datetime = '{}-01-01 00:00:00+00:00'.format(eia_gen_year)
end_datetime = '{}-12-31 23:00:00+00:00'.format(eia_gen_year)

start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S%z')
end_datetime = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S%z')

#%%
#Net Generation Data Import

df_net_gen = row_to_df(NET_GEN_ROWS, 'net_gen')
df_net_gen = df_net_gen.pivot(index = 'datetime', columns = 'region', values = 'net_gen')
ba_cols = BA_acronyms

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

#Add Canadian import data to the net generation dataset, concatenate and put in alpha order
df_CA_Imports_Gen = pd.read_csv(data_dir + '/CA_Imports_Gen.csv', index_col = 0)
df_net_gen_sum = pd.concat([df_net_gen_sum,df_CA_Imports_Gen]).sum(axis=1)
df_net_gen_sum = df_net_gen_sum.to_frame()
df_net_gen_sum = df_net_gen_sum.sort_index(axis=0)

#%%
df_ba_trade = ba_exchange_to_df(BA_TO_BA_ROWS, data_type='ba_to_ba')
df_ba_trade = df_ba_trade.set_index('datetime')
df_ba_trade['transacting regions'] = df_ba_trade['from_region'] + '-' + df_ba_trade['to_region']
interim = range(start_datetime,end_datetime)
df_ba_trade_sub = df_ba_trade.loc[start_datetime:end_datetime]




#%%

##%%
##Create series IDs for Total Interchange (TI), Net Generation (NG) and Demand (D)
##by taking BAA acronym and appending necessary text for the EIA API
##These are used in the next sections when the data calls are performed
#BAA_names_tot_int = []
#for i in range (len(BAA)):
#    BAA_names_tot_int.append('EBA.'+ BAA[i]+'-ALL.TI.H')
#
#BAA_names_net_gen = []
#for i in range (len(BAA)):      
#    BAA_names_net_gen.append('EBA.'+ BAA[i]+'-ALL.NG.H')
#    
#BAA_names_dem = []
#for i in range (len(BAA)):
#    BAA_names_dem.append('EBA.'+ BAA[i]+'-ALL.D.H')
    
##%%
##Use the EIA python call to pull an example dataset from the EIA API
##Use this example dataset to figure out how to format dates
#api_key = "d365fe67a9ec71960d69102951ae474f"
#api = eia.API(api_key)
#series_search = api.data_by_series(series= 'EBA.PJM-ALL.TI.H')
#df = pd.DataFrame(series_search)
#df.index
#
##Convert dataframe index of date strings to a list of date strings for processing 
#date_list = df.index.tolist()
#
##Remove the last three characters from the date string 2015 0701T05Z 01; 
##Datetime won't process two instances of days of the month, so I have to remove the '01'
#dates_trimmed = [x[:-3] for x in date_list]
#
##Use datetime.strptime to parse the sting into a datetime object
#dates_formatted = [datetime.strptime(date, '%Y %m%dT%HZ') for date in dates_trimmed]
#
##Convert datetimes to timestamp - raw data comes in as Zulu time
#dates_timestamp = [pd.Timestamp(date, tz = 'UTC') for date in dates_formatted]
#
##Convert UTC to US EST
#dates_USEST = [date.tz_convert('US/Eastern') for date in dates_timestamp]
#
#df_test = pd.DataFrame(index = dates_USEST)

#%%
#This section is used to query the EIA API for all necessary data - it saves the data in pickle files.
#It is not necessary to run this if importing the data directly from the pickle files

#Query EIA for Total Interchange Data
#Total interchange is the net for a balancing authority for a specified time period
#Negative total interchange implies importing, positive implies exporting
#This dataset does not provide detail on what entities a BA is interacting with
api_key = "d365fe67a9ec71960d69102951ae474f"
api = eia.API(api_key)

df_tot_int = pd.DataFrame(index = df.index)

BAA_error_list = []

for k in range (68):
    try:
        series_search = api.data_by_series(series= str(BAA_names_tot_int[k]))
        df_tot_int[BAA_names_tot_int[k]] = pd.DataFrame(series_search)
    
    #except: Exception
    except: 
        BAA_error_list.append(BAA_names_tot_int[k])

df_tot_int.index = df_test.index
print(BAA_error_list)

#This code works to rename the columns to only the BAA name and strip the other text 
df_tot_int.columns = df_tot_int.rename(columns=lambda x: x[(x.find('.')+1):x.find('-')], inplace = True)

#Subset df_new so that it only contains data for 2016
df_tot_int_2016 = df_tot_int.loc['2016-01-01 01:00:00-04:00':'2016-12-31 23:00:00-04:00',]

#Add a new column that is the sum of all columns. Theoretically this should be zero if all of the imports/exports balance.
df_tot_int_2016['US_sum'] = df_tot_int_2016.sum(axis=1)
df_tot_int_2016.to_pickle('data/2016_total_interchange.pkl')

#%%
#Query EIA for Net Generation Data
#Provides net generation for each BA by hour

api_key = "d365fe67a9ec71960d69102951ae474f"
api = eia.API(api_key)

df_net_gen = pd.DataFrame(index = df.index)

BAA_net_gen_error_list = []

for k in range (68):
    try:
        series_search = api.data_by_series(series= str(BAA_names_net_gen[k]))
        df_net_gen[BAA_names_net_gen[k]] = pd.DataFrame(series_search)
    
    #except: Exception
    except: 
        BAA_error_list.append(BAA_names_net_gen[k])

df_net_gen.index = df_test.index

print(BAA_net_gen_error_list)

#This code works to rename the columns to only the BAA name and strip the other text 
df_net_gen.columns = df_net_gen.rename(columns=lambda x: x[(x.find('.')+1):x.find('-')], inplace = True)

#Subset df_new so that it only contains data for 2016
df_net_gen_2016 = df_net_gen.loc['2016-01-01 01:00:00-04:00':'2016-12-31 23:00:00-04:00',]

#Add a new column that is the sum of all columns. Theoretically this should be zero if all of the imports/exports balance.
df_net_gen_2016['US_sum'] = df_net_gen_2016.sum(axis=1)
df_net_gen_2016.head()
df_net_gen_2016.to_pickle('data/2016_net_gen.pkl')

#%%
#Query EIA for Demand Data
#Provides demand for each BA by hour

api_key = "d365fe67a9ec71960d69102951ae474f"
api = eia.API(api_key)

df_dem = pd.DataFrame(index = df.index)

BAA_dem_error_list = []

for k in range (68):
    try:
        series_search = api.data_by_series(series= str(BAA_names_dem[k]))
        df_dem[BAA_names_dem[k]] = pd.DataFrame(series_search)
    
    #except: Exception
    except: 
        BAA_dem_error_list.append(BAA_names_dem[k])

df_dem.index = df_test.index

print(BAA_dem_error_list)

#This code works to rename the columns to only the BAA name and strip the other text 
df_dem.columns = df_dem.rename(columns=lambda x: x[(x.find('.')+1):x.find('-')], inplace = True)

#Subset df_new so that it only contains data for 2016
df_dem_2016 = df_dem.loc['2016-01-01 01:00:00-04:00':'2016-12-31 23:00:00-04:00',]

#Add a new column that is the sum of all columns. Theoretically this should be zero if all of the imports/exports balance.
df_dem_2016['US_sum'] = df_dem_2016.sum(axis=1)

df_dem_2016.to_pickle('data/2016_dem.pkl')

#%%
#This portion of code reads in a file that contains the IDs for each of the BAAs in the U.S. 
#and Canada and creates a list of every combination of every single BAA ID (e.g. MISO-PJM). 
#The names are formatted as necessary to match the format for the EIA API call. 
BAA_combo = []
for i in range(68):
    for j in range (68):
        BAA_combo.append('EBA.'+BAA[i] + '-'+ BAA[j]+'.ID.H')
BAA_combo_np = np.array(BAA_combo)
BAA_Names_DF = pd.DataFrame(BAA_combo_np, columns = ['Combo'])

#%%
#Use the EIA API to collect all of the actual interchange data 
#for each of the BAAs into a single dataframe.
#This code will only add BA combinations to the dataframe that actually transact, otherwise the 
#BA combination will raise an error
import eia
import pandas as pd
api_key = "d365fe67a9ec71960d69102951ae474f"
api = eia.API(api_key)
df_trade = pd.DataFrame(index = df.index)
BAA_trade_error_list = []

#4624 is the total possible combinations of all BAAs (68 by 68)       
for k in range (4624):
    try:
        series_search = api.data_by_series(series= str(BAA_combo[k]))
        df_trade[BAA_combo[k]] = pd.DataFrame(series_search)
    
    except: 
        BAA_trade_error_list.append(BAA_combo[k])

df_trade.index = df_test.index
df_trade.to_pickle('data/trade.pkl')

#%%
#Import trading data from EIA API
#Use the EIA API to collect all of the actual interchange data 
#for each of the BAAs into a single dataframe.
#Unlike the prior data call, this will fill in a zero value for BA
#combinatons that do not trade
#The result is a dataframe of all BA trading combinations (4624) for each
#hour of the day

import eia
import pandas as pd
api_key = "d365fe67a9ec71960d69102951ae474f"
api = eia.API(api_key)
df_trade_all = pd.DataFrame(index = df.index)
BAA_trade_error_list = []
### Retrieve Data By Series ID ###    
for k in range (4624):
    try:
        series_search = api.data_by_series(series= str(BAA_combo[k]))
        df_trade_all[BAA_combo[k]] = pd.DataFrame(series_search)
    
    except: 
        df_trade_all[BAA_combo[k]] = 0

df_trade_all.index = df_test.index


df_trade_stack = df_trade.stack().to_frame().reset_index()
df_trade_stack.columns = ['Datetime', 'BAAs','Exchange']
df_trade_stack['BAAs'].astype('str')

df_trade_stack.set_index('Datetime',inplace = True)


#Subset df_new so that it only contains data for 2016
df_trade_stack_2016 = df_trade_stack.loc['2016-01-01 01:00:00-04:00':'2016-12-31 23:00:00-04:00']

#Format columns with BA strings
df_trade_stack_2016['Exporting BAA'], df_trade_stack_2016['Importing BAA'] = df_trade_stack_2016['BAAs'].str.split('-', 1).str 
df_trade_stack_2016['Exporting BAA_1'] = df_trade_stack_2016['Exporting BAA'].str[4:] 
df_trade_stack_2016['Importing BAA_1'] = df_trade_stack_2016['Importing BAA'].str[0:-5] 
df_trade_stack_2016['Transacting BAAs'] = df_trade_stack_2016['Exporting BAA_1'] + '-' + df_trade_stack_2016['Importing BAA_1']

df_trade_stack_2016.groupby(['Exporting BAA_1', 'Importing BAA_1'])['Exchange'].sum()
df_trade_stack_2016.groupby(['Importing BAA_1', 'Exporting BAA_1'])['Exchange'].sum()

df_trade_all_stack = df_trade_all.stack().to_frame().reset_index()
df_trade_all_stack.columns = ['Datetime', 'BAAs','Exchange']
df_trade_all_stack['BAAs'].astype('str')

df_trade_all_stack.set_index('Datetime',inplace = True)

#Subset df_new so that it only contains data for 2016
df_trade_all_stack_2016 = df_trade_all_stack.loc['2016-01-01 01:00:00-04:00':'2016-12-31 23:00:00-04:00']

df_trade_all_stack_2016.to_pickle('data/trade_all_2016.pkl')

# In[8]:
#Read pickle files

df_trade_all_stack_2016 = pd.read_pickle(data_dir + '/trade_all_2016.pkl')
df_trade = pd.read_pickle(data_dir + '/trade.pkl')
df_dem_2016 = pd.read_pickle(data_dir + '/2016_dem.pkl')
df_net_gen_2016 = pd.read_pickle(data_dir + '/2016_net_gen.pkl')
df_tot_int_2016 = pd.read_pickle(data_dir + '/2016_total_interchange.pkl')










#%%
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
df_trade_all_stack_2016_resamp = df_trade_all_stack_2016.groupby('BAAs').resample('A').sum()
df_trade_all_stack_2016_resamp_stack = df_trade_all_stack_2016_resamp.stack().to_frame().reset_index()
df_trade_all_stack_2016_resamp_stack = df_trade_all_stack_2016_resamp_stack.set_index('Datetime')
del df_trade_all_stack_2016_resamp_stack['level_2']
df_trade_all_stack_2016_resamp_stack.columns = ['BAAs','Exchange']


#Split BAA string into exporting and importing BAA columns
df_trade_all_stack_2016_resamp_stack['BAA1'], df_trade_all_stack_2016_resamp_stack['BAA2'] = df_trade_all_stack_2016_resamp_stack['BAAs'].str.split('-', 1).str 
df_trade_all_stack_2016_resamp_stack['BAA1'] = df_trade_all_stack_2016_resamp_stack['BAA1'].str[4:] 
df_trade_all_stack_2016_resamp_stack['BAA2'] = df_trade_all_stack_2016_resamp_stack['BAA2'].str[0:-5] 
df_trade_all_stack_2016_resamp_stack['Transacting BAAs'] = df_trade_all_stack_2016_resamp_stack['BAA1'] + '-' + df_trade_all_stack_2016_resamp_stack['BAA2']


#Create two perspectives - import and export to use for comparison in selection of the final exchange value between the BAAs
df_trade_sum_1_2 = df_trade_all_stack_2016_resamp_stack.groupby(['BAA1', 'BAA2','Transacting BAAs'], as_index=False)[['Exchange']].sum()
df_trade_sum_2_1 = df_trade_all_stack_2016_resamp_stack.groupby(['BAA2', 'BAA1', 'Transacting BAAs'], as_index=False)[['Exchange']].sum()
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

df_concat_trade.to_csv('output/tradeout.csv')

#Create the final trading matrix; first grab the necessary columns, rename the columns and then pivot
df_concat_trade_subset = df_concat_trade[['Export_BAA', 'Import_BAA', 'Final_Exchange']]

df_concat_trade_subset.columns = ['Exporting_BAA', 'Importing_BAA', 'Amount']

df_trade_pivot = df_concat_trade_subset.pivot_table(index = 'Exporting_BAA', columns = 'Importing_BAA', values = 'Amount').fillna(0)

#%%
# This cell continues formatting the df_trade 
# Find missing BAs - need to add them in so that we have a square matrix
# Not all BAs are involved in transactions

cols = list(df_trade_pivot.columns.values)
rows = list(df_trade_pivot.index.values)

cols_set = set(cols)
rows_set = set(rows)
BAA_ref_set = set(BAA) 

col_diff = list(BAA_ref_set - cols_set)
col_diff.sort(key = str.upper)

row_diff = list(BAA_ref_set - rows_set)
row_diff.sort(key=str.upper)

#Add in missing columns, then sort in alphabetical order
for i in col_diff:
    df_trade_pivot[i] = 0

df_trade_pivot = df_trade_pivot.sort_index(axis=1)

#Add in missing rows, then sort in alphabetical order
for i in row_diff:
    df_trade_pivot.loc[i,:] = 0

df_trade_pivot = df_trade_pivot.sort_index(axis=0)

#%%
# Add Canadian Imports to the trading matrix
# CA imports are specified in an external file
df_CA_Imports_Rows = pd.read_csv('data/CA_Imports_Rows.csv', index_col = 0)
df_CA_Imports_Cols = pd.read_csv('data/CA_Imports_Cols.csv', index_col = 0)

df_concat_trade_CA = pd.concat([df_trade_pivot, df_CA_Imports_Rows])
df_concat_trade_CA = pd.concat([df_concat_trade_CA, df_CA_Imports_Cols], axis = 1)
df_concat_trade_CA.fillna(0, inplace = True)
df_trade_pivot = df_concat_trade_CA
df_trade_pivot = df_trade_pivot.sort_index(axis=0)
df_trade_pivot = df_trade_pivot.sort_index(axis=1)

#%%
#Define the values for BA generation for the trading math
#Add in missing BAAs to the net generation dataframe, assume zero values (no data available)



df_net_gen_2016 = df_net_gen_2016.drop('US_sum', axis = 1)

# Find missing BAs - need to add them in so we have the full set of 68
# Not all BAs are involved in transactions

cols = list(df_net_gen_2016.columns.values)

cols_set = set(cols)
BAA_ref_set = set(BAA) 

col_diff = list(BAA_ref_set - cols_set)
col_diff.sort(key = str.upper)

#Add in missing columns, then sort in alphabetical order
for i in col_diff:
    df_net_gen_2016[i] = 0

#Resort columns so the headers are in alpha order
df_net_gen_2016 = df_net_gen_2016.sort_index(axis=1)

#Sum values in each column
df_net_gen_2016_sum = df_net_gen_2016.sum(axis = 0).to_frame()

#Add Canadian import data to the net generation dataset, concatenate and put in alpha order
df_CA_Imports_Gen = pd.read_csv('data/CA_Imports_Gen.csv', index_col = 0)
df_net_gen_2016_sum = pd.concat([df_net_gen_2016_sum,df_CA_Imports_Gen]).sum(axis=1)
df_net_gen_2016_sum = df_net_gen_2016_sum.to_frame()
df_net_gen_2016_sum = df_net_gen_2016_sum.sort_index(axis=0)
#%%
#Perform trading calculations as provided in Qu et al (2018) to
#determine the composition of a BA consumption mix

#Create total inflow vector x and then convert to a diagonal matrix x-hat

x = []
for i in range (len(df_net_gen_2016_sum)):
    x.append(df_net_gen_2016_sum.iloc[i] + df_trade_pivot.sum(axis = 0).iloc[i])

x_np = np.array(x)

#If values are zero, x_hat matrix will be singular, set BAAs with 0 to small value (1)
df_x = pd.DataFrame(data = x_np, index = df_trade_pivot.index)
df_x = df_x.rename(columns = {0:'inflow'})
df_x.loc[df_x['inflow'] == 0] = 1

x_np = df_x.values
 
x_hat = np.diagflat(x_np)


#Create consumption vector c and then convert to a digaonal matrix c-hat
#Calculate c based on x and T 

c = []

for i in range(len(df_net_gen_2016_sum)):
    c.append(x[i] - df_trade_pivot.sum(axis = 1).iloc[i])
    
c_np = np.array(c)
c_hat = np.diagflat(c_np)

#Convert df_trade_pivot to matrix
T = df_trade_pivot.values

#Create matrix to split T into distinct interconnections - i.e., prevent trading between eastern and western interconnects
#Connections between the western and eastern interconnects are through SWPP and WAUE
interconnect = df_trade_pivot.copy()
interconnect[:] = 1
interconnect.loc['SWPP',['EPE', 'PNM', 'PSCO', 'WACM']] = 0
interconnect.loc['WAUE',['WAUW', 'WACM']] = 0
interconnect_mat = interconnect.values
T_split = np.multiply(T, interconnect_mat)

#Matrix trading math (see Qu et al. 2018 ES&T paper)
x_hat_inv = np.linalg.inv(x_hat)

B = np.matmul(T_split, x_hat_inv)

I = np.identity(len(df_net_gen_2016_sum))
np.savetxt("output/I.csv", I, delimiter=",")

diff_I_B = I - B

G = np.linalg.inv(diff_I_B)

c_hat_x_hat_inv = np.matmul(c_hat, x_hat_inv)
np.savetxt("output/c_hat_x_hat_inv_matrix.csv", c_hat_x_hat_inv, delimiter=",")

G_c = np.matmul(G, c_hat)
H = np.matmul(G,c_hat, x_hat_inv)

#%%
#Convert H to pandas dataframe, populate index and columns

df_final_trade_out = df_H
df_final_trade_out.columns = df_net_gen_2016_sum.index
df_final_trade_out.index = df_net_gen_2016_sum.index


#Create dataframe representing normalized consumption contribution from other BAs

col_list = df_final_trade_out.columns.tolist()
df_final_trade_out_norm = df_final_trade_out.copy()

for i in col_list:
    df_final_trade_out_norm[i] = df_final_trade_out_norm[i]/(df_final_trade_out_norm[i].sum())

df_final_trade_out_norm = df_final_trade_out_norm.fillna(value = 0)

df_final_trade_out_norm.to_csv('output/df_final_trade_out_norm.csv')

#Determine the consumption mix of technologies based on consumption mix
# and corresponding generation mix for each BA that contributes to another
# BA's consumption mix

df_final_trade_out = df_final_trade_out.reset_index()
df_final_trade_out = df_final_trade_out.rename(columns = {'index':'Source BAA'})
df_final_trade_out_melt = pd.melt(df_final_trade_out, id_vars='Source BAA', value_vars= df_final_trade_out.columns.values.tolist()[1:], var_name = 'Dest BAA', value_name = 'MWh_consumed')


#Import generation data from EIA860 and 923; merge with final trade data, calculate 'MWh by type' of generation
df_860_923 = pd.read_csv('data/EIA860_923 2016 Generation Data.csv')
df_merged = df_860_923.merge(df_final_trade_out_melt, left_on = 'BAA', right_on = 'Source BAA')
df_merged.rename(columns = {'BAA_x':'BAA'}, inplace=True)
df_merged['MWh_by_type'] = df_merged['MWh_consumed']*df_merged['Fuel Type Gen %']
df_merged.drop(['Unnamed: 0','BAA', 'Fuel Type Gen MWh'], axis =1, inplace= True)


#Groupby sum of MWh by type by BAA
df_merged_grouped_BAA_tot_cons = df_merged.groupby(['Dest BAA'])['MWh_by_type'].sum().reset_index()
df_merged_grouped_BAA_tot_cons.columns = ['Dest BAA','BAA_Tot_Cons']

#Calculate BAA fuel % by tehcnology
df_merged = df_merged.merge(df_merged_grouped_BAA_tot_cons, left_on = 'Dest BAA', right_on = 'Dest BAA')
df_merged['BAA_Fuel_%'] = df_merged['MWh_by_type']/df_merged['BAA_Tot_Cons']

#Groupby BAA and fuel type
df_final_BAA_cons_by_fuel = df_merged.groupby(['Dest BAA', 'Fuel Type'])['BAA_Fuel_%'].sum().reset_index()
df_final_BAA_cons_by_fuel.to_csv('output/Final_BAA_Cons_Mix.csv')

#Pivot data to desired format (BAA by technology share in consumption mix)
df_pivot_cons_mix = df_final_BAA_cons_by_fuel.pivot(index = 'Dest BAA', columns= 'Fuel Type', values= 'BAA_Fuel_%')
df_pivot_cons_mix = df_pivot_cons_mix.fillna(0)


#%%
#Aggregate to EIA regions

#Groupby sum of MWh by type by Region
df_merged_grouped_region_tot_cons = df_merged_grouped_BAA_tot_cons.merge(df_BAA, left_on = 'Dest BAA', right_on = 'BAA_Acronym')
df_merged_grouped_region_tot_cons = df_merged_grouped_region_tot_cons.groupby(['Region'])['BAA_Tot_Cons'].sum().reset_index()
df_merged_grouped_region_tot_cons = df_merged_grouped_region_tot_cons.rename(columns = {'BAA_Tot_Cons':'Region_Tot_Cons'})

#Calculate region fuel % by technology
df_merged_region = df_merged[['Fuel Type', 'Source BAA','Dest BAA', 'MWh_by_type', 'BAA_Tot_Cons']]
df_merged_region = df_merged_region.merge(df_BAA, left_on = 'Dest BAA', right_on = 'BAA_Acronym')
df_merged_region = df_merged_region.groupby(['Region', 'Fuel Type'])['MWh_by_type'].sum().reset_index()
df_merged_region = df_merged_region.merge(df_merged_grouped_region_tot_cons, left_on = 'Region', right_on = 'Region')
df_merged_region['Region_Fuel_%'] = df_merged_region['MWh_by_type']/df_merged_region['Region_Tot_Cons']


#Pivot data to desired format (BAA by technology share in consumption mix)
df_pivot_cons_mix_region = df_merged_region.pivot(index = 'Region', columns= 'Fuel Type', values= 'Region_Fuel_%')
df_pivot_cons_mix_region = df_pivot_cons_mix_region.fillna(0)



#Use BA melt table with Source and Destination BA and consumed MWh
#Merge with BA file to map BA names to region names
#Need to do this twice, once for Source BA, then for Dest BA
#Then groupby regions and pivot to get similar BA matrix
df_final_trade_out_melt_region = df_final_trade_out_melt.merge(df_BAA, left_on= 'Source BAA', right_on= 'BAA_Acronym')
df_final_trade_out_melt_region = df_final_trade_out_melt_region.drop(['BAA_Name', 'BAA_Acronym', 'NCR ID#', 'Source BAA', 'FERC Region'], axis = 1)
df_final_trade_out_melt_region = df_final_trade_out_melt_region.rename(columns={'Region': 'Source_Region'})


df_final_trade_out_melt_region = df_final_trade_out_melt_region.merge(df_BAA, left_on= 'Dest BAA', right_on= 'BAA_Acronym')
df_final_trade_out_melt_region = df_final_trade_out_melt_region.drop(['BAA_Name', 'BAA_Acronym', 'NCR ID#', 'Dest BAA', 'FERC Region'], axis = 1)
df_final_trade_out_melt_region = df_final_trade_out_melt_region.rename(columns={'Region': 'Dest_Region'})

df_final_trade_out_melt_region = df_final_trade_out_melt_region.groupby(['Source_Region', 'Dest_Region'])['MWh_consumed'].sum().reset_index()

df_final_trade_out_region_pivot = pd.pivot_table(df_final_trade_out_melt_region, values = 'MWh_consumed', index = 'Source_Region', columns = 'Dest_Region')


#%%
"""
Develop trading input for the eLCI code. Need to melt the dataframe to end up with a three column
dataframe:

import   export   fraction

Repeat for both possible aggregation levels - BA and FERC market region
"""
#Establish a threshold of 0.00001 to be included in the final trading matrix
#Lots of really small values as a result of the matrix calculate (e.g., 2.0e-15)


#Read in BAA file which contains the names and abbreviations
#Original df_BAA does not include the Canadian balancing authorities
#Import them here, then concatenate to make a single df_BAA_NA (North America)

df_BAA_CA = pd.read_excel('data/BA_Codes_930.xlsx', sheetname = 'Canada', header = 4)
df_BAA_CA.rename(columns={'etag ID': 'BAA_Acronym', 'Entity Name': 'BAA_Name','NCR_ID#': 'NRC_ID', 'Region': 'Region'}, inplace=True)
df_BAA_NA = pd.concat([df_BAA, df_BAA_CA])                  
                          

###
df_final_trade_out_filt = df_final_trade_out.copy()

for i in col_list:
    df_final_trade_out_filt[i] = np.where(df_final_trade_out[i].abs()/df_final_trade_out[i].sum() < 0.00001, 0, df_final_trade_out[i].abs())

df_final_trade_out_filt_melted = df_final_trade_out_filt.melt(id_vars = 'Source BAA' , value_vars=col_list)
df_final_trade_out_filt_melted = df_final_trade_out_filt_melted.rename(columns = {'Source BAA':'export BAA', 'variable':'import BAA'})


#Merge to bring in import region name matched with BAA
df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted.merge(df_BAA_NA, left_on = 'import BAA', right_on = 'BAA_Acronym')
df_final_trade_out_filt_melted_merge.rename(columns={'FERC Region': 'import ferc region'}, inplace=True)
df_final_trade_out_filt_melted_merge.drop(columns = ['BAA_Acronym', 'BAA_Name', 'NCR ID#', 'Region'], inplace = True)

#Merge to bring in export region name matched with BAA                                                     
df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted_merge.merge(df_BAA_NA, left_on = 'export BAA', right_on = 'BAA_Acronym')
df_final_trade_out_filt_melted_merge.rename(columns={'FERC Region': 'export ferc region'}, inplace=True)
df_final_trade_out_filt_melted_merge.drop(columns = ['BAA_Acronym', 'BAA_Name', 'NCR ID#', 'Region'], inplace = True)                                                   
                                                     
BAA_import_grouped_tot = df_final_trade_out_filt_melted_merge.groupby(['import BAA'])['value'].sum().reset_index()
ferc_import_grouped_tot = df_final_trade_out_filt_melted_merge.groupby(['import ferc region'])['value'].sum().reset_index()                                                     

#Develop final df for BAA
BAA_final_trade = df_final_trade_out_filt_melted_merge.copy()
BAA_final_trade = BAA_final_trade.drop(columns = ['import ferc region', 'export ferc region'])
BAA_final_trade = BAA_final_trade.merge(BAA_import_grouped_tot, left_on = 'import BAA', right_on = 'import BAA')
BAA_final_trade = BAA_final_trade.rename(columns = {'value_x':'value','value_y':'total'})
BAA_final_trade['fraction'] = BAA_final_trade['value']/BAA_final_trade['total']
BAA_final_trade.to_csv('output/BAA_final_trade_2016.csv')

#Develop final df for FERC Market Region
ferc_final_trade = df_final_trade_out_filt_melted_merge.groupby(['import ferc region','export ferc region'])['value'].sum().reset_index()
ferc_final_trade = ferc_final_trade.merge(ferc_import_grouped_tot, left_on = 'import ferc region', right_on = 'import ferc region')
ferc_final_trade = ferc_final_trade.rename(columns = {'value_x':'value','value_y':'total'})
ferc_final_trade['fraction'] = ferc_final_trade['value']/ferc_final_trade['total']
ferc_final_trade.to_csv('output/ferc_final_trade_2016.csv')


