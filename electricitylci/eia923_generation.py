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
        url_eia923 = 'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_'+odd_year+'.zip'
        print("Downloading EIA-923 files for " + odd_year)
        request = requests.get(url_eia923)
        file = zipfile.ZipFile(io.BytesIO(request.content))
        file.extractall(path=data_dir)
        print('Reading in Excel file ...')
        eia923_path = data_dir+schedule_name+odd_year+'_Final_Revision.xlsx'
        eia = pd.read_excel(eia923_path,
                            sheet_name='Page 1 Generation and Fuel Data',
                            header=5,
                            na_values=['.'],
                            dtype={'Plant Id': str,
                                   'YEAR': str})
        eia.columns = eia.columns.str.replace('\n', ' ')

        # Rename some troublesome columns for 2015
        if odd_year == '2015':
            eia = eia.rename(columns={"Plant State": "State"})
        colstokeep = [
            'Plant Id',
            'Plant Name',
            'State',
            'Total Fuel Consumption MMBtu',
            'Net Generation (Megawatthours)',
            'YEAR'
        ]
        eia = eia.loc[:, colstokeep]
        eia.to_csv(stored_file_name)
    else:
        eia = pd.read_csv(stored_file_name)

    EIA_923 = eia
    # Grouping similar facilities together.
    group_cols = ['Plant Id', 'Plant Name', 'State', 'YEAR']
    sum_cols = [
        'Total Fuel Consumption MMBtu',
        'Net Generation (Megawatthours)'
    ]
    EIA_923_generation_data = EIA_923.groupby(group_cols, as_index=False)[sum_cols].sum()

    return EIA_923_generation_data
