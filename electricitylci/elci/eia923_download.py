# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 22:35:43 2018

@author: TGhosh
"""



import zipfile
import io
import os
import requests


def eia_download_extract(year):

        data_dir = os.path.dirname(os.path.realpath(__file__))+"\\eia923\\"
        url_eia923 =  'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_'+year+'.zip'
        
        request = requests.get(url_eia923)
        file = zipfile.ZipFile(io.BytesIO(request.content))
        
        file.extractall(path = data_dir)
        
        os.chdir(data_dir)      
        
        db = pd.read_excel('EIA923_Schedules_2_3_4_5_M_12_'+year+'_Final_Revision.xlsx',sheet_name = 'Page 1 Generation and Fuel Data')
        
        db.to_csv('eia_923_'+year+'.csv')
        
        
        
#eia_download_extract('2015')
#eia_download_extract('2016')