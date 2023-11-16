#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# bulk_eia_data.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os
import zipfile

import pandas as pd
import requests

from electricitylci.globals import paths


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module provides a function, :func:`download_EBA` to
download the bulk EIA data needed to determine the electricity trading
between balancing authority areas or between Federal Energy Regulatory
Commission regions. This electricity trading ultimately decides the
consumption mix for a given region.

Additional methods are available within this module, but much of it is
duplicated in eia_io_trading.py.

Last updated: 2023-11-15
"""
__all__ = [
    "ba_exchange_to_df",
    "download_EBA",
    "row_to_df",
]


##############################################################################
# FUNCTIONS
##############################################################################
def download_EBA():
    """Attempt to download a copy EIA's U.S. Electric System Operating Data
    from their API site (http://api.eia.gov/bulk/EBA.zip).
    """
    url = 'http://api.eia.gov/bulk/EBA.zip'
    logging.info(f"Downloading eia bulk data from {url}...")
    r = requests.get(url)
    os.makedirs(os.path.join(paths.local_path, 'bulk_data'), exist_ok=True)
    output = open(os.path.join(paths.local_path, 'bulk_data', 'EBA.zip'), 'wb')
    output.write(r.content)
    output.close()
    logging.info(f"complete.")


def row_to_df(rows, data_type):
    """Turn rows of a single type from the bulk data text file into a dataframe
    with the region, datetime, and data as columns.

    Parameters
    ----------
    rows : list
        rows from the EBA.txt file
    data_type : str
        name to use for the data column (e.g. demand or total_interchange)

    Returns
    -------
    pandas.DataFrame
        Data for all regions in a single df with datatimes converted and UTC.
    """
    tuple_list = []
    for row in rows:
        try:
            datetime = pd.to_datetime(
                [x[0] for x in row['data']],
                utc=True,
                format='%Y%m%dT%HZ'
            )
        except ValueError:
            try:
                datetime = pd.to_datetime(
                    [x[0]+":00" for x in row['data']],
                    format='%Y%m%dT%H%z'
                )
            except ValueError:
                continue
        data = [x[1] for x in row['data']]
        region = row['series_id'].split('-')[0][4:]
        tuple_data = [
            x for x in zip([region]*len(datetime), list(datetime), data)]
        tuple_list.extend(tuple_data)
    df = pd.DataFrame(tuple_list, columns=["region", "datetime", data_type])

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
    pandas.DataFrame
        Data for all regions in a single df with datatimes converted and UTC
    """
    tuple_list = []
    for row in rows:
        try:
            datetime = pd.to_datetime(
                [x[0] for x in row['data']],
                utc=True,
                format='%Y%m%dT%HZ'
            )
        except ValueError:
            try:
                datetime = pd.to_datetime(
                    [x[0]+"00" for x in row['data']],
                    format='%Y%m%dT%H%z'
                )
            except ValueError:
                continue
        data = [x[1] for x in row['data']]
        from_region = row['series_id'].split('-')[0][4:]
        to_region = row['series_id'].split('-')[1][:-5]
        tuple_data = [
            x for x in zip(
                [from_region]*len(datetime),
                [to_region]*len(datetime),
                datetime,
                data
            )
        ]
        tuple_list.extend(tuple_data)
    df = pd.DataFrame(
        tuple_list,
        columns=["from_region", "to_region", "datetime", data_type]
    )

    return df


##############################################################################
# MAIN
##############################################################################
if __name__=="__main__":
    path = os.path.join(paths.local_path, 'bulk_data', 'EBA.zip')
    try:
        z = zipfile.ZipFile(path, 'r')
        with z.open('EBA.txt') as f:
            raw_txt = f.readlines()
    except FileNotFoundError:
        download_EBA()
        z = zipfile.ZipFile(path, 'r')
        with z.open('EBA.txt') as f:
            raw_txt = f.readlines()
