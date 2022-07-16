"""Extract hourly real-time EIA data from the bulk-download zip file."""

import pandas as pd
import json
from os.path import join
import os
import zipfile
import requests
import logging
from electricitylci.globals import paths

logger = logging.getLogger("bulk_eia_data")


def download_EBA():
    """Add docstring."""
    url = 'http://api.eia.gov/bulk/EBA.zip'
    logging.info(f"Downloading eia bulk data from {url}...")
    r = requests.get(url)
    os.makedirs(join(paths.local_path, 'bulk_data'), exist_ok=True)
    output = open(join(paths.local_path, 'bulk_data', 'EBA.zip'), 'wb')
    output.write(r.content)
    output.close()
    logger.info(f"complete.")


path = join(paths.local_path, 'bulk_data', 'EBA.zip')

if __name__=="__main__":
    try:
        z = zipfile.ZipFile(path, 'r')
        with z.open('EBA.txt') as f:
            raw_txt = f.readlines()
    except FileNotFoundError:
        download_EBA()
        z = zipfile.ZipFile(path, 'r')
        with z.open('EBA.txt') as f:
            raw_txt = f.readlines()

# REGION_NAMES = [
#    'California', 'Carolinas', 'Central',
#    'Electric Reliability Council of Texas, Inc.', 'Florida',
#    'Mid-Atlantic', 'Midwest', 'New England ISO',
#    'New York Independent System Operator', 'Northwest', 'Southeast',
#    'Southwest', 'Tennessee Valley Authority'
# ]
#
# REGION_ACRONYMS = [
#    'TVA', 'MIDA', 'CAL', 'CAR', 'CENT', 'ERCO', 'FLA',
#    'MIDW', 'ISNE', 'NYIS', 'NW', 'SE', 'SW',
# ]
#
# TOTAL_INTERCHANGE_ROWS = [
#    json.loads(row) for row in raw_txt if b'EBA.TI.H' in row
# ]
#
# NET_GEN_ROWS = [
#    json.loads(row) for row in raw_txt if b'EBA.NG.H' in row
# ]
#
# DEMAND_ROWS = [
#    json.loads(row) for row in raw_txt if b'EBA.D.H' in row
# ]
#
# EXCHANGE_ROWS = [
#    json.loads(row) for row in raw_txt if b'EBA.ID.H' in row
# ]
#
# BA_TO_BA_ROWS = [
#    row for row in EXCHANGE_ROWS
#    if row['series_id'].split('-')[0][4:] not in REGION_ACRONYMS
# ]


def row_to_df(rows, data_type):
    """
    Turn rows of a single type from the bulk data text file into a dataframe
    with the region, datetime, and data as columns
    Parameters
    ----------
    rows : list
        rows from the EBA.txt file
    data_type : str
        name to use for the data column (e.g. demand or total_interchange)
    Returns
    -------
    dataframe
        Data for all regions in a single df with datatimes converted and UTC
    """
    tuple_list = []
    for row in rows:

        # "data" is of form:
        # [['20190214T04Z', -102],
        # ['20190214T03Z', -107],
        # ['20190214T02Z', -108],
        # ['20190214T01Z', -103]]
        try:
            datetime = pd.to_datetime([x[0] for x in row['data']],
                                      utc=True, format='%Y%m%dT%HZ')
        except ValueError:
            try:
                datetime = pd.to_datetime([x[0]+":00" for x in row['data']],
                                          format='%Y%m%dT%H%z')
            except ValueError:
                continue
        data = [x[1] for x in row['data']]
        region = row['series_id'].split('-')[0][4:]

#        df_data = {
#            'region': region,
#            'datetime': datetime,
#            data_type: data,
#        }
#        region_list=[region for x in datetime]
#        _df = pd.DataFrame(df_data)
#        tuple_list.append(_df)
        tuple_data=[x for x in zip([region]*len(datetime), list(datetime), data)]
        tuple_list.extend(tuple_data)
    df=pd.DataFrame(tuple_list, columns=["region", "datetime", data_type])
#    df = pd.concat(df_list).reset_index(drop=True)

    return df


def ba_exchange_to_df(rows, data_type='ba_to_ba'):
    """
    Turn rows of a single type from the bulk data text file into a dataframe
    with the region, datetime, and data as columns
    Parameters
    ----------
    rows : list
        rows from the EBA.txt file
    data_type : str
        name to use for the data column (e.g. demand or total_interchange)
    Returns
    -------
    dataframe
        Data for all regions in a single df with datatimes converted and UTC
    """
    tuple_list = []
    for row in rows:

        # "data" is of form:
        # [['20190214T04Z', -102],
        # ['20190214T03Z', -107],
        # ['20190214T02Z', -108],
        # ['20190214T01Z', -103]]
        try:
            datetime = pd.to_datetime([x[0] for x in row['data']],
                                      utc=True, format='%Y%m%dT%HZ')
        except ValueError:
            try:
                datetime = pd.to_datetime([x[0]+"00" for x in row['data']],
                                          format='%Y%m%dT%H%z')
            except ValueError:
                continue
        data = [x[1] for x in row['data']]
        from_region = row['series_id'].split('-')[0][4:]
        to_region = row['series_id'].split('-')[1][:-5]

#        df_data = {
#            'from_region': from_region,
#            'to_region': to_region,
#            'datetime': datetime,
#            data_type: data,
#        }
        tuple_data = [x for x in zip([from_region]*len(datetime), [to_region]*len(datetime), datetime, data)]
        tuple_list.extend(tuple_data)
#        _df = pd.DataFrame(df_data)
#        df_list.append(_df)

#    df = pd.concat(df_list).reset_index(drop=True)
    df=pd.DataFrame(tuple_list, columns=["from_region", "to_region", "datetime", data_type])
    return df
