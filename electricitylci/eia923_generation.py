import pandas as pd
import zipfile
import io
import os
import requests
from electricitylci.globals import output_dir
from electricitylci.globals import data_dir

def eia_download_extract(odd_year):
        
        
        #This part has been commented out because it needs to run just once. 
        '''
        url_eia923 =  'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_'+odd_year+'.zip'
        
        request = requests.get(url_eia923)
        file = zipfile.ZipFile(io.BytesIO(request.content))
        
        file.extractall(path = data_dir)
        
            
        
        eia = pd.read_excel(data_dir+'EIA923_Schedules_2_3_4_5_M_12_'+odd_year+'_Final_Revision.xlsx',sheet_name = 'Page 1 Generation and Fuel Data')
        #db[['Total Fuel Consumption\r\nMMBtu', 'Net Generation\r\n(Megawatthours)']] = db[['Total Fuel Consumption\r\nMMBtu', 'Net Generation\r\n(Megawatthours)']].apply(pd.to_numeric,errors = 'coerce')
        
        eia = eia.drop([0])
        eia = eia.drop([1])
        eia = eia.drop([2])
        eia = eia.drop([3])
        eia.columns = eia.iloc[0]
        eia = eia.drop([4])       
        eia.to_csv(data_dir+'EIA923_Schedules_2_3_4_5_M_12_'+odd_year+'_Final_Revision.csv')
        '''
        #Replace this read in with a check to see if EIA-923 has been processed
        eia = pd.read_csv(data_dir+'EIA923_Schedules_2_3_4_5_M_12_'+odd_year+'_Final_Revision.csv')
        

        EIA_923 = eia[['Plant Id', 'Plant Name', 'Plant State', 'Total Fuel Consumption\r\nMMBtu', 'Net Generation\r\n(Megawatthours)','YEAR']]
        EIA_923[['Total Fuel Consumption\r\nMMBtu', 'Net Generation\r\n(Megawatthours)']] = EIA_923[['Total Fuel Consumption\r\nMMBtu', 'Net Generation\r\n(Megawatthours)']].apply(pd.to_numeric,errors = 'coerce')
        EIA_923[['Plant Id','YEAR']]= EIA_923[['Plant Id','YEAR']].astype(str)
        # Grouping similar facilities together.
        
        EIA_923_generation_data = EIA_923.groupby(['Plant Id', 'Plant Name','Plant State','YEAR'], as_index=False)['Total Fuel Consumption\r\nMMBtu', 'Net Generation\r\n(Megawatthours)'].sum()
        

        
        return EIA_923_generation_data
        
eia_download_extract('2015')