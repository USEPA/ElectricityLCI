#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# utils.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import io
import logging
import os
import re
import zipfile

import requests
import pandas as pd

from electricitylci.globals import data_dir


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """Small utility functions for use throughout the repository.

Last updated:
    2023-12-21
"""


##############################################################################
# FUNCTIONS
##############################################################################
def download_unzip(url, unzip_path):
    """
    Download and extract contents from a .zip file from a given url to a given
    path.

    Parameters
    ----------
    url : str
        Valid URL to download the zip file
    unzip_path : str or path object
        Destination to unzip the data
    """
    r = requests.get(url)
    content_type = r.headers["Content-Type"]
    if "zip" not in content_type and "-stream" not in content_type:
        logging.error(content_type)
        raise ValueError("URL does not point to valid zip file")

    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path=unzip_path)


def find_file_in_folder(folder_path, file_pattern_match, return_name=True):
    """Search a folder for files matching a pattern.

    Parameters
    ----------
    folder_path : str
        An existing directory path.
    file_pattern_match : list
        A list of keywords used to match a file name.
    return_name : bool, optional
        Whether to return the filename identified in addition to the full path,
        by default True

    Returns
    -------
    str or tuple
        The file path or, if `return_name` is true, a tuple of the file path
        and its basename.

    Examples
    --------
    >>> import os
    >>> from electricitylci.globals import paths
    >>> my_dir = os.path.join(paths.local_folder, 'f923_2016')
    >>> find_file_in_folder(my_dir, ['2_3_4_5', 'csv'])
    (
      '~/electricitylci/f923_2016/                                 \
       EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision_page_1.csv',
      'EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision_page_1.csv')
    """
    files = os.listdir(folder_path)

    # Would be more elegant with glob but this works to identify the
    # file in question.
    for f in files:
        # modified this so that we can search for multiple strings in the
        # file name - mostly to support different pages of csv files from 923.
        if all(a in f for a in file_pattern_match):
            file_name = f

    file_path = os.path.join(folder_path, file_name)

    if not return_name:
        return file_path
    else:
        return (file_path, file_name)


def create_ba_region_map(match_fn="BA code match.csv",
                         region_col="ferc_region"):
    """Generate a pandas series for mapping a region to balancing authority.

    Used in eia860_facilities.py

    Parameters
    ----------
    match_fn : str, optional
        The mapping data file, by default "BA code match.csv"
    region_col : str, optional
        The column name from the mapping file associated with the region to be
        mapped to balancing authority code. For default data file, valid
        options include, 'ferc_region,' 'eia_region,' and 'Balancing Authority
        Code,' the latter is trivial as it maps itself.
        Defaults to "ferc_region."

    Returns
    -------
    pandas.Series
        A series with indices associated with balancing authority codes and
        values from the requested region column (e.g., ferc_region).

    Examples
    --------
    >>> m = create_ba_region_map()
    >>> m.head()
    Balancing Authority Code
    AEC        SE
    AECI     MISO
    AVA        NW
    AZPS       SW
    BANC    CAISO
    Name: ferc_region, dtype: object
    """
    match_path = os.path.join(data_dir, match_fn)
    region_match = pd.read_csv(match_path, index_col=0)
    region_match["Balancing Authority Code"] = region_match.index
    try:
        map_series = region_match[region_col]
    except KeyError:
        if 'ferc' in region_col.lower():
            region_col = 'ferc_region'
        elif 'eia' in region_col.lower():
            region_col = 'eia_region'
        elif 'ba' in region_col.lower():
            region_col = "Balancing Authority Code"
        elif 'us' in region_col.lower():
            region_col = 'ferc_region'
        else:
            logging.warning(
                f"regional_col value is {region_col} - "
                "a mapping for this does not exist, "
                "using ferc_region instead")
            region_col = 'ferc_region'
        map_series = region_match[region_col]

    return map_series


def fill_default_provider_uuids(dict_to_fill, *args):
    """
    Fill UUIDs for default providers.

    For default providers in the specified dictionary (dict_to_fill) using any
    number of other dictionaries given in args to find the matching process and
    provide the UUID. This is to ensure all the required data for providers is
    available for openLCA.

    Parameters
    ----------
    dict_to_fill : dict
        A dictionary in the openLCA schema with processes that have
        input exchanges with default provider names provided but not
        UUIDs.
    *args: dictionary
        Any number of dictionaries to search for matching processes
        for the UUIDs

    Returns
    -------
    dict
        The dict_to_fill input with UUIDs filled in where matching
        processes were found.
    """
    found = False
    dict_list = list(args)
    list_of_dicts = [isinstance(x,dict) for x in dict_list]
    logging.info("Attempting to find UUIDs for default providers...")
    if all(list_of_dicts):
        for key in dict_to_fill.keys():
            for exch in dict_to_fill[key]['exchanges']:
                # BUG: no key, input; is this "is_input"?
                if exch['input'] is True and isinstance(exch['provider'],dict):
                    found = False
                    for src_dict in args:
                        for src_key in src_dict.keys():
                            if src_dict[src_key]["name"]==exch["provider"]["name"] and isinstance(src_dict[src_key]["uuid"],str):
                                exch["provider"]["@id"]=src_dict[src_key]["uuid"]
                                logging.debug(
                                    f"UUID for {exch['provider']} found")
                                found = True
                                break;
                        if found:
                            break;
                    if not found:
                        logging.info(f"UUID for {exch['provider']} not found")
    else:
        logging.warning(f"All arguments into function must be dictionaries")
    return dict_to_fill


def make_valid_version_num(foo):
    """
    Strip letters from a string to keep only digits and dots in order to make
    the version number valid in olca-schema processes.

    Notes
    -----
    See also: http://greendelta.github.io/olca-schema/html/Process.html

    Parameters
    ----------
    foo : str
        A string of the software version.

    Returns
    -------
    str
        Same string with only numbers and periods.
    """
    result = re.sub('[^0-9,.]', '', foo)
    return result


def set_dir(directory):
    """Ensure an output directory exists.

    Notes
    -----
    Generates a directory path, if it does not exist.

    Parameters
    ----------
    directory : str
        A path to a directory.

    Returns
    -------
    str
        The path to an existing directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def join_with_underscore(items):
    """A helper method to concatenate items together using an underscore.

    If the items are not strings, they are cast to strings.
    If the items are a dictionary, only the keys are concatenated.

    Parameters
    ----------
    items : list, tuple
        An iterable object with values to be concatenated.

    Returns
    -------
    str
        An underscore joined string of values.

    Examples
    --------
    >>> join_with_underscore(['a', 'b', 'c'])
    'a_b_c'
    >>> join_with_underscore({'a': 1, 'b': 2, 'c': 3})
    'a_b_c'
    >>> join_with_underscore([i for i in range(10)])
    '0_1_2_3_4_5_6_7_8_9'
    """
    type_cast_to_str = False
    for x in items:
        if not isinstance(x, str):
            type_cast_to_str = True
    if type_cast_to_str:
        items = [str(x) for x in items]
    return "_".join(items)
