import pandas as pd
import zipfile
import io
import os
import requests
from electricitylci.globals import data_dir

def eia_download_extract(odd_year):
    odd_year = str(odd_year)
    schedule_name = 'EIA923_Schedules_2_3_4_5_'
    if odd_year == '2015':
        schedule_name = 'EIA923_Schedules_2_3_4_5_M_12_'
    stored_file_name = data_dir+schedule_name+odd_year+'_Final_Revision.csv'
    if not os.path.exists(stored_file_name):
        url_eia923 =  'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_'+odd_year+'.zip'
        print("Downloading EIA-923 files for " + odd_year)
        request = requests.get(url_eia923)
        file = zipfile.ZipFile(io.BytesIO(request.content))
        file.extractall(path = data_dir)
        print('Reading in Excel file ...')
        eia = pd.read_excel(data_dir+schedule_name+odd_year+'_Final_Revision.xlsx',sheet_name = 'Page 1 Generation and Fuel Data')
        eia = eia.drop([0])
        eia = eia.drop([1])
        eia = eia.drop([2])
        eia = eia.drop([3])
        eia.columns = eia.iloc[0]
        eia = eia.drop([4])
        #Rename some troublesome columns for 2015
        if odd_year == '2015':
            eia = eia.rename(columns={"Plant State":"State","Total Fuel Consumption\nMMBtu":"Total Fuel Consumption MMBtu","Net Generation\n(Megawatthours)":"Net Generation (Megawatthours)"})
        colstokeep = ['Plant Id', 'Plant Name', 'State', 'Total Fuel Consumption MMBtu', 'Net Generation (Megawatthours)', 'YEAR']
        eia = eia[colstokeep]
        eia.to_csv(stored_file_name)
    else:
        eia = pd.read_csv(stored_file_name)

    EIA_923 = eia
    EIA_923[['Total Fuel Consumption MMBtu', 'Net Generation (Megawatthours)']] = EIA_923[['Total Fuel Consumption MMBtu', 'Net Generation (Megawatthours)']].apply(pd.to_numeric,errors = 'coerce')
    EIA_923[['Plant Id','YEAR']]= EIA_923[['Plant Id','YEAR']].astype(str)
    # Grouping similar facilities together.
    EIA_923_generation_data = EIA_923.groupby(['Plant Id', 'Plant Name','State','YEAR'], as_index=False)['Total Fuel Consumption MMBtu', 'Net Generation (Megawatthours)'].sum()


    return EIA_923_generation_data
        



