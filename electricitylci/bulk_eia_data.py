#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# bulk_eia_data.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import datetime
import logging
import os
import tempfile
import zipfile

import pandas as pd
import requests

from electricitylci.globals import paths
from electricitylci.utils import read_json


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module provides a function, :func:`download_EBA` to
download the bulk EIA data needed to determine the electricity trading
between balancing authority areas or between Federal Energy Regulatory
Commission regions. This electricity trading ultimately decides the
consumption mix for a given region.

Last updated:
    2024-03-12
"""
__all__ = [
    "ba_exchange_to_df",
    "check_EBA_vintage",
    "download_EBA",
    "download_EBA_manifest",
    "read_local_manifest_last_update",
    "read_remote_manifest_last_update",
    "row_to_df",
]


##############################################################################
# GLOBALS
##############################################################################
EBA_URL = "http://api.eia.gov/bulk/EBA.zip"
'''str : The API URL for EIA bulk data.'''
MANIFEST_URL = "http://api.eia.gov/bulk/manifest.txt"
'''str : The API URL for EIA bulk data manifest.'''
VINTAGE_THRESH = 30
'''int : The threshold age (in days) to trigger new bulk data download.'''


##############################################################################
# FUNCTIONS
##############################################################################
def check_EBA_vintage():
    """Compare local manifest to EIA's API for last update date for
    US Electric System Operating Data (EBA) bulk data.

    If the vintage of the local copy is too old (i.e., greater than the
    VINTAGE_THRESH), then download a new copy of the bulk data.
    """
    logging.debug("Checking EBA bulk data vintage...")
    d = read_local_manifest_last_update()
    d = datetime.datetime.fromisoformat(d)

    a = read_remote_manifest_last_update()
    a = datetime.datetime.fromisoformat(a)

    if (a-d).days > VINTAGE_THRESH:
        download_EBA()


def download_EBA_manifest(out_file):
    """Download the EPA bulk data manifest for checking vintage.

    See `here <https://www.eia.gov/opendata/v1/bulkfiles.php>`_ for details.

    Parameters
    ----------
    out_file : str
        A file path to write the manifest text to.
        It should be a plain text file format (e.g., ends with .txt)
    """
    r = requests.get(MANIFEST_URL)
    if r.ok:
        if isinstance(out_file, str):
            output = open(out_file, 'wb')
        else:
            # See also, tempfile
            output = out_file
        try:
            output.write(r.content)
        except:
            logging.error("Failed to write manifest to file!")
        else:
            logging.debug("Wrote manifest to text.")
            output.close()
    else:
        logging.error("Failed to download EIA bulk data manifest!")


def download_EBA():
    """Download a copy EIA's U.S. Electric System Operating Data
    from their API site (http://api.eia.gov/bulk/EBA.zip).

    Now also includes the manifest.txt file.
    """
    logging.info(f"Downloading eia bulk data from {EBA_URL}...")
    r = requests.get(EBA_URL)
    os.makedirs(os.path.join(paths.local_path, 'bulk_data'), exist_ok=True)
    output = open(os.path.join(paths.local_path, 'bulk_data', 'EBA.zip'), 'wb')
    output.write(r.content)
    output.close()

    # HOTFIX: include manifest [2024-03-12; TWD]
    m_file = os.path.join(paths.local_path, 'bulk_data', 'manifest.txt')
    download_EBA_manifest(m_file)
    logging.info(f"complete.")


def read_local_manifest_last_update():
    """Read the manifest.txt file from local machine for the last updated
    time stamp for the EIA's US Electric System Operating Data (EBA), else
    return date string from January 2000.

    Returns
    -------
    str
        ISO formatted date string of the last_updated time for the EIA
        US Electric System Operating Data (EBA) bulk data.
    """
    m_file = os.path.join(paths.local_path, 'bulk_data', 'manifest.txt')
    d = read_json(m_file)
    d = d.get("dataset", {})
    d = d.get("EBA", {})
    # Add dummy time in case the manifest fails.
    d = d.get("last_updated", "2000-01-01T00:00:00-04:00")

    return d


def read_remote_manifest_last_update():
    """Return the last updated time stamp from EIA's US Electric System
    Operating Data (EBA) bulk data from their API, else return a date
    string from March 2023.

    Returns
    -------
    str
        ISO formatted date string of the last_updated time for the EIA
        US Electric System Operating Data (EBA) bulk data.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        m_file = os.path.join(temp_dir, "manifest.txt")
        download_EBA_manifest(m_file)
        d = read_json(m_file)
        d = d.get("dataset", {})
        d = d.get("EBA", {})
        # Add dummy time in case the manifest fails.
        d = d.get("last_updated", "2024-03-12T06:40:29-04:00")

    return d


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
