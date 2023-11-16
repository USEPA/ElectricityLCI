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
__doc__ = """Small utility functions for use throughout the repository."""


##############################################################################
# GLOBALS
##############################################################################
module_logger = logging.getLogger("utils.py")


##############################################################################
# FUNCTIONS
##############################################################################
def download_unzip(url, unzip_path):
    """
    Download a zip file from url and extract contents to a given path.

    Parameters
    ----------
    url : str
        Valid url to download the zip file
    unzip_path : str or path object
        Destination to unzip the data

    """
    r = requests.get(url)
    content_type = r.headers["Content-Type"]
    if "zip" not in content_type and "-stream" not in content_type:
        module_logger.error(content_type)
        raise ValueError("URL does not point to valid zip file")

    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path=unzip_path)


def find_file_in_folder(folder_path, file_pattern_match, return_name=True):
    """Add docstring."""
    files = os.listdir(folder_path)

    # would be more elegent with glob but this works to identify the
    # file in question
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


def create_ba_region_map(
    match_fn="BA code match.csv", region_col="ferc_region"
):

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
            module_logger.warning(f"regional_col value is {region_col} - a mapping for this does not exist, using ferc_region instead")
#                'or "eia_region"')
            region_col = 'ferc_region'
#            raise (
#                ValueError,
#                f'regional_col value is {region_col}, but should match "ferc_region" '
#                'or "eia_region"'
#            )

        map_series = region_match[region_col]
    return map_series


def fill_default_provider_uuids(dict_to_fill, *args):
    """
    Fills in UUIDs.

    For default providers in the specified dictionary (dict_to_fill) using any
    number of other dictionaries given in args to find the matching process and
    provide the UUID. This is to ensure all the required data for providers is
    available for openLCA.

    Parameters
    ----------
    dict_to_fill : dictionary
        A dictionary in the openLCA schema with processes that have
        input exchanges with default provider names provided but not
        UUIDs
    *args: dictionary
        Any number of dictionaries to search for matching processes
        for the UUIDs

    Returns
    -------
    dictionary
        The dict_to_fill input with UUIDs filled in where matching
        processes were found.
    """
    found = False
    dict_list = list(args)
    list_of_dicts = [isinstance(x,dict) for x in dict_list]
    module_logger.info("Attempting to find UUIDs for default providers...")
    if all(list_of_dicts):
        for key in dict_to_fill.keys():
            for exch in dict_to_fill[key]['exchanges']:
                if exch['input'] is True and isinstance(exch['provider'],dict):
                    found=False
                    for src_dict in args:
                        for src_key in src_dict.keys():
                            if src_dict[src_key]["name"]==exch["provider"]["name"] and isinstance(src_dict[src_key]["uuid"],str):
                                exch["provider"]["@id"]=src_dict[src_key]["uuid"]
                                module_logger.debug(f"UUID for {exch['provider']} found")
                                found = True
                                break;
                        if found:
                            break;
                    if not found:
                        module_logger.info(f"UUID for {exch['provider']} not found")
    else:
        module_logger.warning(f"All arguments into function must be dictionaries")
    return dict_to_fill


def make_valid_version_num(foo):
    """
    Strip letters from a string to keep only digits and dots to make
    the version number valid in olca-schema processes

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
    if not os.path.exists(directory): os.makedirs(directory)
    return directory


def join_with_underscore(items):
    type_cast_to_str = False
    for x in items:
        if not isinstance(x, str):
            # raise TypeError("join_with_underscore()  inputs must be string")
            type_cast_to_str = True
    if type_cast_to_str:
        items = [str(x) for x in items]
    return "_".join(items)
