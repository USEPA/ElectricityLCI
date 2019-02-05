"""
Download and import EIA860 data, including power plant information such as
plant code, location, balancing authority, and primary fuel type.

This is using most of the code from eia923_generation.py. It could be
combined and generalized in the future.

"""
import pandas as pd
import zipfile
import io
import os
from os.path import join
import requests
from electricitylci.globals import data_dir, EIA860_BASE_URL
from electricitylci.utils import download_unzip, find_file_in_folder


def eia860_download(year, save_path):
    """
    Download and unzip one year of EIA 860 annual data to a subfolder
    of the data directory
    
    Parameters
    ----------
    year : int or str
        The year of data to download and save
    save_path : path or str
        A folder where the zip file contents should be extracted
    
    """
    current_url = EIA860_BASE_URL + 'xls/eia860{}.zip'.format(year)
    archive_url = EIA860_BASE_URL + 'archive/xls/eia860{}.zip'.format(year)

    # try to download using the most current year url format
    try:
        download_unzip(current_url, save_path)
    except ValueError:
        download_unzip(archive_url, save_path)


def load_eia860_excel(eia860_path):

    eia = pd.read_excel(eia860_path,
                        header=1,
                        na_values=['.', ' '],
                        dtype={'Plant Code': str})
    # Get ride of line breaks. Rename Plant Code to Plant Id (match
    # the 923 column name)
    eia.columns = (eia.columns.str.replace('\n', ' ')
                              .str.replace('Plant Code', 'Plant Id')
                              .str.replace('Plant State', 'State'))

    return eia


def eia860_balancing_authority(year):

    expected_860_folder = join(data_dir, 'eia860{}'.format(year))

    if not os.path.exists(expected_860_folder):
        print('Downloading EIA-860 files')
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match='2___Plant',
            return_name=True
        )
        # eia860_files = os.listdir(expected_860_folder)

        # # would be more elegent with glob but this works to identify the
        # # Schedule_2_3_4_5 file
        # for f in eia860_files:
        #     if '2___Plant' in f:
        #         plant_file = f

        # eia860_path = join(expected_860_folder, plant_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia860_excel(eia860_path)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split('.')[0] + '.csv'
        csv_path = join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [f for f in all_files
                    if '.csv' in f
                    and 'Plant_Y{}'.format(year) in f]

        # Read and return the existing csv file if it exists
        if csv_file:
            print('Loading {} EIA-860 plant data from csv file'.format(year))
            fn = csv_file[0]
            csv_path = join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path,
                              dtype={'Plant Id': str})

        else:
            print('Loading data from previously downloaded excel file')
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match='2___Plant',
                return_name=True
            )
            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2___Plant' in f:
            #         plant_file = f
            # eia860_path = join(expected_860_folder, plant_file)
            eia = load_eia860_excel(eia860_path)

            csv_fn = eia860_name.split('.')[0] + '.csv'
            csv_path = join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    
    ba_cols = [
        'Plant Id',
        'State',
        'NERC Region',
        'Balancing Authority Code',
        'Balancing Authority Name',
    ]
    eia_plant_ba_match = eia.loc[:, ba_cols].drop_duplicates()

    return eia_plant_ba_match


def eia860_primary_capacity(year):

    pass
