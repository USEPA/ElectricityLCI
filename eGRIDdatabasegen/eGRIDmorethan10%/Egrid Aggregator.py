# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 12:09:09 2017

@author: tghosh
"""


import pandas as pd
import numpy as np

#Reading egrid files for matching

egrid = pd.read_csv("egrid.csv", header=0, error_bad_lines=False)


egrid = egrid.rename(columns={"DOE/EIA ORIS plant or facility code": "EGRID ID"})
egrid = egrid.rename(columns={"Plant annual NOx total output emission rate (kg/MWh)" : "nox"})
egrid = egrid.rename(columns={"Plant annual SO2 total output emission rate (kg/MWh)" : "so2"})
egrid = egrid.rename(columns={"Plant annual CO2 total output emission rate (kg/MWh)" : "co2"})
egrid = egrid.rename(columns={"Plant annual CH4 total output emission rate (kg/MWh)" : "ch4"})
egrid = egrid.rename(columns={"Plant annual N2O total output emission rate (kg/MWh)" : "n2o"})

#print(egrid.columns.tolist())
egrid['nox'] = pd.to_numeric(egrid['nox'],errors = 'coerce')
egrid['so2'] = pd.to_numeric(egrid['so2'],errors = 'coerce')
egrid['co2'] = pd.to_numeric(egrid['co2'],errors = 'coerce')
egrid['ch4'] = pd.to_numeric(egrid['ch4'],errors = 'coerce')
egrid['n2o'] = pd.to_numeric(egrid['n2o'],errors = 'coerce')

egrid = egrid[egrid[' eGRID subregion acronym '] != ""]
egrid = egrid[egrid[' Plant primary fuel '] != ""]


#Trying to Just compile EGRID  for emissions based on Regions fuel sources first
chk1 = egrid[[' eGRID subregion acronym ', ' Plant primary fuel ', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ',' Plant primary fuel ']).mean()
chk2 = egrid[[' eGRID subregion acronym ', ' Plant primary coal/oil/gas/ other fossil fuel category ', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ',' Plant primary coal/oil/gas/ other fossil fuel category ']).mean()
chk1.reset_index( drop=False, inplace=True)
chk2.reset_index( drop=False, inplace=True)

#Trying to Just compile EGRID  for emissions based on Regions fuel sources first

chk1.to_csv('Aggregated_LCI1.csv')
chk2.to_csv('Aggregated_LCI2.csv')



sLength = len(egrid['EGRID ID'])
egrid['Solar type'] = pd.Series(np.random.randn(sLength), index=egrid.index)
egrid['Geo type'] = pd.Series(np.random.randn(sLength), index=egrid.index)
egrid['Solar type'] = None
egrid['Geo type'] = None
egrid['Solar type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'solar') & (egrid[' Prime Mover '].str.strip().str.lower() == 'pv')] = "SOLAR PV"
egrid['Solar type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'solar') & (egrid[' Prime Mover '].str.strip().str.lower() != 'pv')] = "SOLAR THERMAL"
egrid['Geo type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'geothermal') & (egrid[' Prime Mover '].str.strip().str.lower() == 'bt')] = "GEOTHERMAL BT"
egrid['Geo type'][(egrid[' Plant primary coal/oil/gas/ other fossil fuel category '].str.strip().str.lower() == 'geothermal') & (egrid[' Prime Mover '].str.strip().str.lower() != 'bt')] = "GEOTHERMAL FT"

chk3 = egrid[[' eGRID subregion acronym ', 'Solar type', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ','Solar type']).mean()
chk4 = egrid[[' eGRID subregion acronym ', 'Geo type', 'nox', 'so2', 'co2', 'ch4', 'n2o']].groupby([' eGRID subregion acronym ','Geo type']).mean()
chk3.reset_index( drop=False, inplace=True)
chk4.reset_index( drop=False, inplace=True)

chk3.to_csv('Aggregated_LCI3.csv')
chk4.to_csv('Aggregated_LCI4.csv')

#Worked Perfectly. It matched with the method that I developed earlier with the other python code where you had to change the
#Fuel source for every run. Just one thing that cant be done using this code is the emissions from SOlar PV and Geothermal.
#DUe to too much Prime mover information. Its best to do that with the old Python Code
#The values over here match perfectly with the results obtained earlier. Hence we can use either of these. Take Solar and GEO from old files. 

#Lessthan10% avg file is from old Python Code. 
