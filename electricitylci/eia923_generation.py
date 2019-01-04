import pandas as pd
import zipfile
import io
import os
from os.path import join
import requests
from electricitylci.globals import data_dir, EIA923_BASE_URL
from electricitylci.utils import download_unzip


def eia923_download(year, save_path):
    """
    Download and unzip one year of EIA 923 annual data to a subfolder
    of the data directory
    
    Parameters
    ----------
    year : int or str
        The year of data to download and save
    save_path : path or str
        A folder where the zip file contents should be extracted
    
    """
    current_url = EIA923_BASE_URL + 'xls/f923_{}.zip'.format(year)
    archive_url = EIA923_BASE_URL + 'archive/xls/f923_{}.zip'.format(year)

    # try to download using the most current year url format
    try:
        download_unzip(current_url, save_path)
    except ValueError:
        download_unzip(archive_url, save_path)


def load_eia923_excel(eia923_path):

    eia = pd.read_excel(eia923_path,
                        sheet_name='Page 1 Generation and Fuel Data',
                        header=5,
                        na_values=['.'],
                        dtype={'Plant Id': str,
                                'YEAR': str})
    # Get ride of line breaks. And apparently 2015 had 'Plant State'
    # instead of 'State'
    eia.columns = (eia.columns.str.replace('\n', ' ')
                              .str.replace('Plant State', 'State'))

    # colstokeep = [
    #     'Plant Id',
    #     'Plant Name',
    #     'State',
    #     'Reported Prime Mover',
    #     'Reported Fuel Type Code',
    #     'Total Fuel Consumption MMBtu',
    #     'Net Generation (Megawatthours)',
    #     'YEAR'
    # ]
    # eia = eia.loc[:, colstokeep]

    return eia


def eia923_download_extract(
    year,
    group_cols = [
        'Plant Id',
        'Plant Name',
        'State',
        'Reported Prime Mover',
        'Reported Fuel Type Code',
        'YEAR'
    ]
):
    """
    Download (if necessary) and extract a single year of generation/fuel
    consumption data from EIA-923. 
    
    Data are grouped by plant level
    
    Parameters
    ----------
    year : int or str
        Year of data to download/extract
    group_cols : list, optional
        The columns from EIA923 generation and fuel sheet to use when grouping
        generation and fuel consumption data.
    
    """
    expected_923_folder = join(data_dir, 'f923_{}'.format(year))

    if not os.path.exists(expected_923_folder):
        print('Downloading EIA-923 files')
        eia923_download(year=year, save_path=expected_923_folder)
        eia923_files = os.listdir(expected_923_folder)

        # would be more elegent with glob but this works to identify the
        # Schedule_2_3_4_5 file
        for f in eia923_files:
            if '2_3_4_5' in f:
                gen_file = f

        eia923_path = join(expected_923_folder, gen_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia923_excel(eia923_path)

        # Save as csv for easier access in future
        csv_fn = gen_file.split('.')[0] + '.csv'
        csv_path = join(expected_923_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_923_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [f for f in all_files
                    if '.csv' in f
                    and '{}_Final'.format(year) in f]

        # Read and return the existing csv file if it exists
        if csv_file:
            print('Loading data from csv file')
            fn = csv_file[0]
            csv_path = join(expected_923_folder, fn)
            eia = pd.read_csv(csv_path,
                              dtype={'Plant Id': str,
                                     'YEAR': str})

        else:
            print('Loading data from previously downloaded excel file')
            # would be more elegent with glob but this works to identify the
            # Schedule_2_3_4_5 file
            for f in all_files:
                if '2_3_4_5' in f:
                    gen_file = f
            eia923_path = join(expected_923_folder, gen_file)
            eia = load_eia923_excel(eia923_path)

            csv_fn = gen_file.split('.')[0] + '.csv'
            csv_path = join(expected_923_folder, csv_fn)
            eia.to_csv(csv_path, index=False)

    # EIA_923 = eia
    # Grouping similar facilities together.
    # group_cols = ['Plant Id', 'Plant Name', 'State', 'YEAR']
    sum_cols = [
        'Total Fuel Consumption MMBtu',
        'Net Generation (Megawatthours)'
    ]
    EIA_923_generation_data = eia.groupby(group_cols,
                                          as_index=False)[sum_cols].sum()

    return EIA_923_generation_data
