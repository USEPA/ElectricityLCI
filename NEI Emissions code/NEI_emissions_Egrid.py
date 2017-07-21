
# coding: utf-8

# In[ ]:


"""
Created on Mon Jul 10 14:05:18 2017

@author: tghosh

PLEASE run every block of code separately as this not a continuous code but a set of Pandas statements that work BLOCK by BLOCK.
"""
#############################################################################################################################
#*****************************************************************************************************************************
#############################################################################################################################
#*****************************************************************************************************************************

import pandas as pd
import numpy as np

#Reading Files
nei = pd.read_csv("1121.csv", header=0,usecols=['eis facility id', 'site name', 'pollutant code', 'pollutant desc', 'total emissions','emissions uom'], dtype={'eis facility id':"str",'company name':"str",'pollutant code':"str",'pollutant desc':"str",'total emissions':"str",'emissions uom':"str"}, nrows= 1028059, error_bad_lines=False)
nei = nei.dropna(subset = ['pollutant code'])
nei = nei.dropna(subset = ['total emissions'])

#Reading the Bridge files
bridge = pd.read_csv("egridtoeis.csv", header=0,usecols=['EIS ID','PNAME', 'EGRID ID'], dtype={'EIS ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 2669, error_bad_lines=False)

#Changing from String to Float
nei['total emissions'] = pd.to_numeric(nei['total emissions'],errors = 'coerce')

#Unit COnversion
sLength = len(nei['total emissions'])
nei['total emissions1'] = pd.Series(np.random.randn(sLength), index=nei.index)
nei['total emissions1'][nei['emissions uom'] == 'LB'] = 0.453592*nei['total emissions']
nei['total emissions1'][nei['emissions uom'] == 'TON'] = 907.185.0*nei['total emissions']
nei['units'] = pd.Series(np.random.randn(sLength), index=nei.index)
nei['units'] = 'kg';


del nei['total emissions']
del nei['emissions uom']

#This part of the code essentially sums up all the similar emissions from every facility into one emissions
egeis = pd.merge(nei,bridge,left_on = ['eis facility id'], right_on = ['EIS ID'])
egeis['Total emissions'] = egeis.groupby(['eis facility id', 'pollutant code'])['total emissions1'].transform('sum')
del egeis['total emissions1']
egeis = egeis.drop_duplicates()


egeis.to_csv('eisemissions1.csv')

#############################################################################################################################
#*****************************************************************************************************************************
#############################################################################################################################
#*****************************************************************************************************************************

#THis part is exactly same as the first part except its for the second and third NEI file.

nei = pd.read_csv("1122.csv", header=0,usecols=['eis facility id', 'site name', 'pollutant code', 'pollutant desc', 'total emissions','emissions uom'], dtype={'eis facility id':"str",'company name':"str",'pollutant code':"str",'pollutant desc':"str",'total emissions':"str",'emissions uom':"str"}, nrows= 3971831, error_bad_lines=False)
nei = nei.dropna(subset = ['pollutant code'])
nei = nei.dropna(subset = ['total emissions'])


bridge = pd.read_csv("egridtoeis.csv", header=0,usecols=['EIS ID','PNAME', 'EGRID ID'], dtype={'EIS ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 2669, error_bad_lines=False)



nei['total emissions'] = pd.to_numeric(nei['total emissions'],errors = 'coerce')


sLength = len(nei['total emissions'])
nei['total emissions1'] = pd.Series(np.random.randn(sLength), index=nei.index)
nei['total emissions1'][nei['emissions uom'] == 'LB'] = 0.453592*nei['total emissions']
nei['total emissions1'][nei['emissions uom'] == 'TON'] = 907.185*nei['total emissions']
nei['units'] = pd.Series(np.random.randn(sLength), index=nei.index)
nei['units'] = 'kg';

del nei['total emissions']
del nei['emissions uom']

egeis = pd.merge(nei,bridge,left_on = ['eis facility id'], right_on = ['EIS ID'])


egeis['Total emissions'] = egeis.groupby(['eis facility id', 'pollutant code'])['total emissions1'].transform('sum')
del egeis['total emissions1']
egeis = egeis.drop_duplicates()


egeis.to_csv('eisemissions2.csv')

#############################################################################################################################
#*****************************************************************************************************************************


nei = pd.read_csv("1123.csv", header=0,usecols=['eis facility id', 'site name', 'pollutant code', 'pollutant desc', 'total emissions','emissions uom'], dtype={'eis facility id':"str",'company name':"str",'pollutant code':"str",'pollutant desc':"str",'total emissions':"str",'emissions uom':"str"}, nrows= 2546834, error_bad_lines=False)
nei = nei.dropna(subset = ['pollutant code'])
nei = nei.dropna(subset = ['total emissions'])

bridge = pd.read_csv("egridtoeis.csv", header=0,usecols=['EIS ID','PNAME', 'EGRID ID'], dtype={'EIS ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 2669, error_bad_lines=False)


nei['total emissions'] = pd.to_numeric(nei['total emissions'], errors = 'coerce')
sLength = len(nei['total emissions'])
nei['total emissions1'] = pd.Series(np.random.randn(sLength), index=nei.index)
nei['total emissions1'][nei['emissions uom'] == 'LB'] = 0.453592*nei['total emissions']
nei['total emissions1'][nei['emissions uom'] == 'TON'] = 907.185*nei['total emissions']
nei['units'] = pd.Series(np.random.randn(sLength), index=nei.index)
nei['units'] = 'kg';

del nei['total emissions']
del nei['emissions uom']

egeis = pd.merge(nei,bridge,left_on = ['eis facility id'], right_on = ['EIS ID'])


egeis['Total emissions'] = egeis.groupby(['eis facility id', 'pollutant code'])['total emissions1'].transform('sum')
del egeis['total emissions1']
egeis = egeis.drop_duplicates()

egeis.to_csv('eisemissions3.csv')

#############################################################################################################################
#*****************************************************************************************************************************
#############################################################################################################################
#*****************************************************************************************************************************

#For joining the three separate generated databases into one
nei1 = pd.read_csv("eisemissions1.csv", header=0,usecols=['eis facility id', 'site name', 'pollutant code', 'pollutant desc', 'Total emissions','units','EIS ID','PNAME', 'EGRID ID'], dtype={'eis facility id':"str",'company name':"str",'pollutant code':"str",'pollutant desc':"str",'Total emissions':"str",'units':"str", 'EIS ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 22132, error_bad_lines=False)
nei2 = pd.read_csv("eisemissions2.csv", header=0,usecols=['eis facility id', 'site name', 'pollutant code', 'pollutant desc', 'Total emissions','units','EIS ID','PNAME', 'EGRID ID'], dtype={'eis facility id':"str",'company name':"str",'pollutant code':"str",'pollutant desc':"str",'Total emissions':"str",'units':"str", 'EIS ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 51283, error_bad_lines=False)
nei3 = pd.read_csv("eisemissions3.csv", header=0,usecols=['eis facility id', 'site name', 'pollutant code', 'pollutant desc', 'Total emissions','units','EIS ID','PNAME', 'EGRID ID'], dtype={'eis facility id':"str",'company name':"str",'pollutant code':"str",'pollutant desc':"str",'Total emissions':"str",'units':"str", 'EIS ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 25820, error_bad_lines=False)
frames = [nei1, nei2, nei3]
nei = pd.concat(frames)

#Changing the Database Structure similar to Egrid
nei['Total emissions'] = pd.to_numeric(nei['Total emissions'],errors = 'coerce')
final_nei = nei.pivot_table('Total emissions', ['eis facility id', 'site name','units','PNAME', 'EGRID ID'],'pollutant desc')
final_nei.reset_index( drop=False, inplace=True)


#final_nei.to_csv('NEIEMISSIONS.CSV')

