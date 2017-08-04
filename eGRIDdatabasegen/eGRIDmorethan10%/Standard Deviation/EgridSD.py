# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 15:15:17 2017

@author: tghosh
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 12:09:09 2017

@author: tghosh
"""


import pandas as pd
import numpy as np

#Reading the Egrid files for matching

egrid = pd.read_csv("egrid.csv", header=0, error_bad_lines=False)

#Renaming the Columns
egrid = egrid.rename(columns={"DOE/EIA ORIS plant or facility code": "EGRID ID"})
egrid = egrid.rename(columns={"Plant annual NOx total output emission rate (kg/MWh)" : "nox"})
egrid = egrid.rename(columns={"Plant annual SO2 total output emission rate (kg/MWh)" : "so2"})
egrid = egrid.rename(columns={"Plant annual CO2 total output emission rate (kg/MWh)" : "co2"})
egrid = egrid.rename(columns={"Plant annual CH4 total output emission rate (kg/MWh)" : "ch4"})
egrid = egrid.rename(columns={"Plant annual N2O total output emission rate (kg/MWh)" : "n2o"})


#Converting to Numeric Data Type

egrid['nox'] = pd.to_numeric(egrid['nox'],errors = 'coerce')
egrid['so2'] = pd.to_numeric(egrid['so2'],errors = 'coerce')
egrid['co2'] = pd.to_numeric(egrid['co2'],errors = 'coerce')
egrid['ch4'] = pd.to_numeric(egrid['ch4'],errors = 'coerce')
egrid['n2o'] = pd.to_numeric(egrid['n2o'],errors = 'coerce')

egrid = egrid[egrid[' eGRID subregion acronym '] != ""]
egrid = egrid[egrid[' Plant primary fuel '] != ""]



#Trying to Just compile EGRID  for emissions based on Regions  and fuel sources first
chk1 = egrid[[' eGRID subregion acronym ', ' Plant primary fuel ', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ',' Plant primary fuel ']).std()
chk2 = egrid[[' eGRID subregion acronym ', ' Plant primary coal/oil/gas/ other fossil fuel category ', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ',' Plant primary coal/oil/gas/ other fossil fuel category ']).std()
chk1.reset_index( drop=False, inplace=True)
chk2.reset_index( drop=False, inplace=True)

#Trying to Just compile EGRID  for emissions based on Regions fuel sources first
#Writing to FIles. 
chk1.to_csv('StandardDev_LCI1.csv')
chk2.to_csv('StandardDev_LCI2.csv')


#This part is specifically for Solar and Geothermal Fuel sources and Prime Mover types. 

sLength = len(egrid['EGRID ID'])
egrid['Solar type'] = pd.Series(np.random.randn(sLength), index=egrid.index)
egrid['Geo type'] = pd.Series(np.random.randn(sLength), index=egrid.index)
egrid['Solar type'] = None
egrid['Geo type'] = None
egrid['Solar type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'solar') & (egrid[' Prime Mover '].str.strip().str.lower() == 'pv')] = "SOLAR PV"
egrid['Solar type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'solar') & (egrid[' Prime Mover '].str.strip().str.lower() != 'pv')] = "SOLAR THERMAL"
egrid['Geo type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'geothermal') & (egrid[' Prime Mover '].str.strip().str.lower() == 'bt')] = "GEOTHERMAL BT"
egrid['Geo type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'geothermal') & (egrid[' Prime Mover '].str.strip().str.lower() != 'bt')] = "GEOTHERMAL FT"

chk3 = egrid[[' eGRID subregion acronym ', 'Solar type', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ','Solar type']).std()
chk4 = egrid[[' eGRID subregion acronym ', 'Geo type', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ','Geo type']).std()
chk3.reset_index( drop=False, inplace=True)
chk4.reset_index( drop=False, inplace=True)
#Writing to files.
chk3.to_csv('StandardDev_LCI3.csv')
chk4.to_csv('StandardDev_LCI4.csv')
