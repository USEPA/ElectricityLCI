# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 18:08:36 2017

@author: tghosh
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 15:42:03 2017

@author: tghosh
"""

import pandas as pd
import numpy as np

#Reading the TRI main File. We need to repeat all these operations for a variety of emissionsin TRI. TO reduce complication I will be using Separate Runs for Each.THe list will be provided
# Please find and replace the names of the different emissions in the code and run the code to get different emissions. 
#LIST OF columns being compiled to be presented in different sheets - FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE, FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE, 

tri = pd.read_csv("TRI.csv", header=0,usecols=['TRIFID', 'FACILITY NAME', 'CAS NUMBER', 'CHEMICAL NAME','UNIT OF MEASURE','FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE'], nrows= 82444, error_bad_lines=False)

#Dropping Unecessary items
tri = tri.dropna(subset = ['TRIFID'])
tri = tri.dropna(subset = ['FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE'])

#Read Egrid bridge
bridge = pd.read_csv("egridtotri.csv", header=0,usecols=['TRI ID','PNAME','EGRID ID'], dtype={'TRI ID':"str",'PNAME':"str",'EGRID ID':"str"}, nrows= 1225, error_bad_lines=False)







#Delete should be before dropduplicate

del tri['UNIT OF MEASURE']
tri = tri.drop_duplicates()



#MErging with the BRIDGE and then restructuring to make it look like Egrid and saving a copy of the database
egtri = pd.merge(bridge,tri,left_on = ['TRI ID'], right_on = ['TRIFID'])
egtri1 = egtri.drop_duplicates(subset=['EGRID ID','CHEMICAL NAME'], keep = 'first')

final_tri = egtri1.pivot(index = 'EGRID ID', columns = 'CHEMICAL NAME', values = 'FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE')
final_tri.reset_index( drop=False, inplace=True)
#final_tri.to_csv('egtri2.csv')


#tri = final_tri
#del final_tri

#tri.to_csv('FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE.csv')
egrid = pd.read_csv("eGRIDPLNT2014.csv", header=0,usecols=['EGRID ID','Plant name','Generation'], nrows= 8504, error_bad_lines=False)
final_tri['EGRID ID'] = pd.to_numeric(final_tri['EGRID ID'],errors = 'coerce')
tri4 = pd.merge(egrid,final_tri,left_on = ['EGRID ID'], right_on = ['EGRID ID'])


tri4.to_excel('FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE.xlsx')
