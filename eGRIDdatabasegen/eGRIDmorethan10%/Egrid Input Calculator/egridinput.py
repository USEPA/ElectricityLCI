# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 13:01:11 2017

@author: tghosh
"""


import pandas as pd
import numpy as np

egrid = pd.read_csv("egrid.csv", header=0, error_bad_lines=False)
egrid = egrid.rename(columns={"DOE/EIA ORIS plant or facility code": "EGRID ID"})
#Remvoing all rows with no Net generation given. 
egrid = egrid.dropna(subset = [' Plant annual net generation (MWh) '])
sLength = len(egrid['EGRID ID'])
egrid['mmbtu/mwh'] = pd.Series(np.random.randn(sLength), index=egrid.index)
#Converting to Numeric Type
egrid[' Plant annual net generation (MWh) '] = pd.to_numeric(egrid[' Plant annual net generation (MWh) '],errors = 'coerce')
egrid['Plant total annual heat input (MMBtu)'] = pd.to_numeric(egrid['Plant total annual heat input (MMBtu)'],errors = 'coerce')

#Dividing the anuual net heat input with the net generation 
egrid[['mmbtu/mwh']] = egrid[['Plant total annual heat input (MMBtu)']].div(egrid[' Plant annual net generation (MWh) '],axis = 0)



del egrid['EGRID ID']
del egrid['Plant total annual heat input (MMBtu)']
del egrid[' Plant annual net generation (MWh) ']
del egrid[' Plant name ']


#print(egrid.columns.tolist())


#Aggregating the heat inputs/MWH by fuel sources and eGRID Subregions. 
chk1 = egrid.groupby([' eGRID subregion acronym ',' Plant primary fuel ']).mean()
chk1.reset_index( drop=False, inplace=True)

chk2 = egrid.groupby([' eGRID subregion acronym ',' Plant primary coal/oil/gas/ other fossil fuel category ']).mean()
chk2.reset_index( drop=False, inplace=True)


egrid['Solar type'] = pd.Series(np.random.randn(sLength), index=egrid.index)
egrid['Geo type'] = pd.Series(np.random.randn(sLength), index=egrid.index)
egrid['Solar type'] = None
egrid['Geo type'] = None
egrid['Solar type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'solar') & (egrid[' Prime Mover '].str.strip().str.lower() == 'pv')] = "SOLAR PV"
egrid['Solar type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'solar') & (egrid[' Prime Mover '].str.strip().str.lower() != 'pv')] = "SOLAR THERMAL"
egrid['Geo type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'geothermal') & (egrid[' Prime Mover '].str.strip().str.lower() == 'bt')] = "GEOTHERMAL BT"
egrid['Geo type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'geothermal') & (egrid[' Prime Mover '].str.strip().str.lower() != 'bt')] = "GEOTHERMAL FT"

chk3 = egrid.groupby([' eGRID subregion acronym ','Solar type']).mean()
chk3.reset_index( drop=False, inplace=True)

chk4 = egrid.groupby([' eGRID subregion acronym ','Geo type']).mean()
chk4.reset_index( drop=False, inplace=True)



chk1.to_csv('AggregatedFuelinput1.csv')
chk2.to_csv('AggregatedFuelinput2.csv')
chk3.to_csv('AggregatedFuelinput3.csv')
chk4.to_csv('AggregatedFuelinput4.csv')



#egrid1 = egrid[[' eGRID subregion acronym ',' Plant primary fuel ',' Plant primary coal/oil/gas/ other fossil fuel category ','Solar type','Geo type',' Prime Mover ']]



#egrid1 = egrid1.drop_duplicates()


#egrid1.to_csv('prime_moverlist.csv')
