#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# utils.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import io
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import sys
import zipfile

import requests
import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.globals import output_dir


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """Small utility functions for use throughout the repository.

Last updated:
    2025-02-12

Changelog:
    -   [25.01.23]: Add logger utility methods
    -   [25.01.14]: Add StEWI inventories of interest method.
    -   [24.10.09]: Update find file in folder to not crash.
    -   [24.08.05]: Create new BA code getter w/ FERC mapping.
    -   TODO: update create_ba_region_map to link with new BA code getter
    -   TODO: write BA code w/ FERC mapping to file for offline use
    -   TODO: create a "wipe clean" method to remove all downloaded data
        within the electricitylci folder.
"""
__all__ = [
    "check_output_dir",
    "create_ba_region_map",
    "decode_str",
    "download",
    "download_unzip",
    "fill_default_provider_uuids",
    "find_file_in_folder",
    "get_logger",
    "get_stewi_invent_years",
    "join_with_underscore",
    "linear_search",
    "make_valid_version_num",
    "read_ba_codes",
    "read_eia_api",
    "read_json",
    "rollover_logger",
    "set_dir",
    "write_csv_to_output",
]


##############################################################################
# FUNCTIONS
##############################################################################
def _build_data_store(data_file_types=None, skip_dirs=[]):
    """Create a dictionary of files and folders for the data providers
    of ElectricityLCI, including stewi, stewicombo, facilitymatcher, and
    fedelemflowlist.

    Parameters
    ----------
    data_file_types : list, optional
        A list of data file type extensions (e.g., '.txt'), by default None
    skip_dirs : list, optional
        A list of directory names to skip (not paths), by default []

    Returns
    -------
    dict
        A dictionary of data stores for each data provider. Each key has
        a dictionary with three sub-keys: 'path', 'dirs', and 'files',
        where 'path' is the location of the data store, 'dirs' is a list of
        sub-folders within the data store (may be empty if not sub-dirs),
        and 'files' is a list of data files matching the given data file
        types (if None, then all files are matched).

    Examples
    --------
    >>> ds = _build_data_store()
    >>> for k in ds.keys():
    ...     print(k, ds[k]['path'])
    """
    ds = _init_data_store()

    for cur_elem in ds.keys():
        cur_path = ds[cur_elem]['path']
        for root, _, files in os.walk(cur_path):
            # This is the directory being searched for files and folders.
            r_dir = os.path.basename(root)

            # Don't look in any folders that are to be skipped.
            if r_dir in skip_dirs:
                continue
            else:
                # Only add sub-folders
                if root != cur_path:
                    ds[cur_elem]['dirs'].append(root)
                for f in files:
                    f_path = os.path.join(root, f)
                    if data_file_types is None:
                        # Add all files
                        ds[cur_elem]['files'].append(f_path)
                    else:
                        # Add only requested file types
                        f_ext = os.path.splitext(f)[1]
                        if f_ext in data_file_types:
                            ds[cur_elem]['files'].append(f_path)

    return ds


def _find_empty_dirs(filepath):
    """Search for empty sub-folders in the given file path.

    Parameters
    ----------
    filepath : str
        An existing directory path.

    Returns
    -------
    list
        A list of folder paths that are empty (contain no files or folders).
    """
    # Find empty directories
    empty_dirs = []
    for root, subdirs, files in os.walk(filepath):
        if len(subdirs) == 0 and len(files) == 0:
            empty_dirs.append(root)

    return empty_dirs


def _process_folders(filepath):
    """A helper method for deleting empty folders from a computer.

    Parameters
    ----------
    filepath : str
        A folder in which to remove empty sub-folders from.
    """
    empty_dirs = _find_empty_dirs(filepath)
    while len(empty_dirs) > 0:
        for my_dir in empty_dirs:
            os.rmdir(my_dir)
            logging.info("Deleted: %s" % my_dir)
        empty_dirs = _find_empty_dirs(filepath)


def _init_data_store():
    """Initialize an empty data store dictionary for data providers in
    ElectricityLCI.

    Returns
    -------
    dict
        An empty dictionary of data stores for each data provider.
        The dictionary has a key for each provider and the value of each
        key is a dictionary with three sub-keys: 'path', 'dirs', and 'files',
        where 'path' is the location of the data store, 'dirs' is an empty
        list, and 'files' is an empty list.
    """
    from electricitylci.globals import paths
    import stewi           # stewi.paths.local_path
    import stewicombo      # stewicombo.globals.path.local_path
    import fedelemflowlist # fedelemflowlist.globals.fedefl_path.local_path
    import facilitymatcher as fm  # fm.globals.paths.local_path

    data_store = {
        'electricitylci': {
            'path': '',
            'dirs': [],
            'files': [],
        },
        'stewi': {
            'path': '',
            'dirs': [],
            'files': [],
        },
        'stewicombo': {
            'path': '',
            'dirs': [],
            'files': [],
        },
        'facilitymatcher': {
            'path': '',
            'dirs': [],
            'files': [],
        },
        'fedelemflowlist': {
            'path': '',
            'dirs': [],
            'files': [],
        },
    }

    try:
        elci_path = str(paths.local_path)
    except:
        logging.warning("No eLCI data store folder!")
        del data_store["electricitylci"]
    else:
        data_store["electricitylci"]['path'] = elci_path

    try:
        stewi_path = str(stewi.paths.local_path)
    except:
        logging.warning("Failed to get stewi data store directory!")
        del data_store["stewi"]
    else:
        data_store["stewi"]['path'] = stewi_path

    try:
        combo_path = str(stewicombo.globals.paths.local_path)
    except:
        logging.warning("Failed to find stewicombo data store!")
        del data_store["stewicombo"]
    else:
        data_store['stewicombo']['path'] = combo_path

    try:
        fm_path = str(fm.globals.path.local_path)
    except:
        logging.warning("Failed to find facilitymatcher data store!")
        del data_store['facilitymatcher']
    else:
        data_store['facilitymatcher']['path'] = fm_path

    try:
        fedefl_path = str(fedelemflowlist.globals.fedefl_path.local_path)
    except:
        logging.warning("Failed to find fedelemflowlist data store directory!")
        del data_store["fedelemflowlist"]
    else:
        data_store["fedelemflowlist"]['path'] = fedefl_path

    return data_store


def _process_files(filelist, to_filter=False, filter_txt="n/a"):
    """Delete files from a file list that match a year filter.

    Parameters
    ----------
    filelist : list
        A list of data file paths.
    to_filter : bool, optional
        Whether to filter file paths by text, by default False
    filter_txt : str, optional
        The string to search if filtering, by default "n/a"

    Examples
    --------
    >>> my_list = ["file-2020.txt", "file-2021.txt", "file-2022.txt"]
    >>> _process_files(my_list, True, '2020')
    Deleted file-2020.txt
    """
    if not isinstance(filter_txt, str):
        filter_txt = str(filter_txt)

    for f in filelist:
        if not os.path.isfile(f):
            logging.warning("File, '%s', does not exist!" % f)
        else:
            msg = "Deleted: " + str(f)
            if to_filter and filter_txt in f:
                if not os.remove(f):
                    logging.info(msg)
            elif not to_filter:
                if not os.remove(f):
                    logging.info(msg)


def check_output_dir(out_dir):
    """Helper method to ensure a directory exists.

    If a given directory does not exist, this method attempts to create it.

    Parameters
    ----------
    out_dir : str
        A path to a directory.

    Returns
    -------
    bool
        Whether the directory exists.
    """
    if not os.path.isdir(out_dir):
        try:
            # Start with super mkdir
            os.makedirs(out_dir)
        except:
            logging.warning("Failed to create folder %s!" % out_dir)
            try:
                # Revert to simple mkdir
                os.mkdir(out_dir)
            except:
                logging.error("Could not create folder, %s" % out_dir)
            else:
                logging.info("Created %s" % out_dir)
        else:
            logging.info("Created %s" % out_dir)

    return os.path.isdir(out_dir)


def clean_data_store():
    """This method is designed to delete data files from the various
    data stores used in ElectricityLCI, skipping certain directories
    (e.g., hidden and archived), skipping certain file types (i.e.,
    not defined in the ``data_file_types`` list), and, optionally,
    filtered by data provider and year.

    Currently set to delete 2022 files/folders from electricitylci data store.

    For developers only.
    """
    ans = input(
        "Did you mean to clean your data store? If not, ctrl+c to escape now!")

    # NOTE
    # Define folders to skip over. For example, if you have EBA.zip files
    # from earlier time periods (e.g., you are trying to run ELCI_1), make
    # sure you keep a backup version of it in an archive or hidden folder to
    # avoid getting caught in this cleaner.
    skip_dirs = [
        'archive',
        'hidden',
        'netl',
        'output'
    ]

    # Target file extensions to clean
    data_file_types = [
        '.csv',
        '.json',
        '.parquet',
        '.pdf',
        '.xls',
        '.xlsx',
        '.zip',
    ]

    # Bools; to be modified by the user (or as method parameters)
    incl_elci = True
    incl_stewi = False
    incl_combo = False
    incl_fm = False
    incl_fedefl = False
    incl_year = True
    year = 2022

    ds = _build_data_store(data_file_types, skip_dirs)
    for k in ds.keys():
        if k == 'electricitylci' and incl_elci:
            _process_files(ds[k]['files'], incl_year, year)
            _process_folders(ds[k]['path'])
        elif k == 'stewi' and incl_stewi:
            _process_files(ds[k]['files'], incl_year, year)
            _process_folders(ds[k]['path'])
        elif k == 'stewicombo' and incl_combo:
            _process_files(ds[k]['files'], incl_year, year)
            _process_folders(ds[k]['path'])
        elif k == 'facilitymatcher' and incl_fm:
            _process_files(ds[k]['files'], incl_year, year)
            _process_folders(ds[k]['path'])
        elif k == 'fedelemflowlist' and incl_fedefl:
            _process_files(ds[k]['files'], incl_year, year)
            _process_folders(ds[k]['path'])


# TODO: Link this to read_ba_codes(); disconnected!
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


def decode_str(bstring):
    """Return a Python string.

    Decodes a byte string.

    Parameters
    ----------
    bstring : bytes
        An encoded byte string.

    Returns
    -------
    str
        A Python string.
    """
    if isinstance(bstring, bytes):
        try:
            bstring = bstring.decode("utf-8")
        except:
            bstring = ""
    elif isinstance(bstring, str):
        pass
    else:
        bstring = ""
    return bstring


def download(url, file_path):
    """Helper method to download a file from a URL.

    Parameters
    ----------
    url : str
        Universal resource locator
    file_path : str
        A local file path where the URL resource should be saved.

    Returns
    -------
    bool
        Whether the resource file was downloaded successfully.
    """
    is_success = True
    #adding 20s timeout to avoid long delays due to server issues.
    r = requests.get(url, timeout=20)
    if r.ok:
        with open(file_path, 'bw') as f:
            f.write(r.content)
    else:
        is_success = False

    return is_success


def download_unzip(url, unzip_path):
    """
    Download and extract contents from a .zip file from a given url to a given
    path.

    The output folder is created if it does not already exist.

    Parameters
    ----------
    url : str
        Valid URL to download the zip file
    unzip_path : str or path object
        Destination to unzip the data
    """
    #adding 20s timeout to avoid long delays due to server issues.
    r = requests.get(url, timeout=20)
    content_type = r.headers["Content-Type"]
    if "zip" not in content_type and "-stream" not in content_type:
        err_msg = (
            "Failed to find ZIP stream in HTTP content, '%s'; "
            "this is not a ZIP file!") % (content_type)
        logging.error(err_msg)
        raise ValueError("URL does not point to valid zip file")

    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path=unzip_path)


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
    file_path = None
    file_name = None

    for f in files:
        # modified this so that we can search for multiple strings in the
        # file name - mostly to support different pages of csv files from 923.
        if all(a in f for a in file_pattern_match):
            file_name = f

    if file_name:
        file_path = os.path.join(folder_path, file_name)

    if not return_name:
        return file_path
    else:
        return (file_path, file_name)


def get_logger(stream=True, rfh=True, str_lv='INFO', rfh_lv='DEBUG'):
    """A helper function for creating or retrieving a root logger with
    only one instance of stream and/or rotating file handler.

    Parameters
    ----------
    stream : bool, optional
        Whether to create a stream handler, by default True
    rfh : bool, optional
        Whether to create a rotating file handler, by default True
    str_lv : str, optional
        Stream handler logging level, by default 'INFO'
    rfh_lv : str, optional
        Rotating file handler logging level, by default 'DEBUG'

    Returns
    -------
    logging.Logger
        The root logger.

    Notes
    -----
    This could be expanded to allow the user to set the logging
    level of a specific handler (or just overwrite all levels).
    """
    # Create/retrieve the root logger
    log = logging.getLogger()
    log.setLevel("DEBUG")

    # Define log format
    rec_format = (
        "%(asctime)s.%(msecs)03d:%(levelname)s:%(module)s:%(funcName)s:"
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')

    # Check what handlers the root logger already has
    has_stream = False
    has_rfh = False
    for h in log.handlers:
        if h.name == 'elci_stream':
            has_stream = True
        elif h.name == 'elci_rfh':
            has_rfh = True

    # Create stream handler for info messages
    if stream and not has_stream:
        s_handler = logging.StreamHandler()
        s_handler.setLevel(str_lv)
        s_handler.setFormatter(formatter)
        s_handler.set_name('elci_stream')
        s_handler.stream = sys.stdout   # won't show pink in Jupyter notebook
        log.addHandler(s_handler)

    # Create file handler for debug messages
    if rfh and not has_rfh:
        log_filename = "elci.log"
        check_output_dir(output_dir)
        log_path = os.path.join(output_dir, log_filename)
        f_handler = RotatingFileHandler(
            log_path, backupCount=9, encoding='utf-8')
        f_handler.setLevel(rfh_lv)
        f_handler.setFormatter(formatter)
        f_handler.set_name('elci_rfh')
        log.addHandler(f_handler)

    # Clean-up step; all unnamed log handlers get elevated to critical.
    #  NOTE: alternatively, we could drop all unnamed loggers.
    num_handlers = len(log.handlers)
    for i in range(num_handlers):
        if not log.handlers[i].name:
            log.handlers[i].setLevel("CRITICAL")

    return log


def get_stewi_invent_years(year):
    """Helper function to return inventory years of interest from StEWI.
    See https://github.com/USEPA/standardizedinventories for inventory names
    and years, which are hard-coded here.

    Parameters
    ----------
    year : int
        An inventory vintage (e.g., 2020).

    Returns
    -------
        dict
            A dictionary of inventory codes and their most recent year of
            data available (less than or equal to the year provided).

    Notes
    -----
    This method answers the question, which inventories of interest should
    go into the modelconfig files (``inventories_of_interest`` attribute) for
    a given year.

    Depends on :func:`linear_search`.

    In eLCI, the inventories of interest configuration parameter cares about
    the following data providers,

    - eGRID
    - TRI
    - NEI
    - RCRAInfo

    Examples
    --------
    >>> get_stewi_invent_years(2019)
    """
    # A dictionary of StEWI inventories and their available vintages
    # NOTE: inventories not considered in eLCI are commented out.
    STEWI_DATA_VINTAGES = {
        # 'DMR': [x for x in range(2011, 2023, 1)],
        # 'GHGRP': [x for x in range(2011, 2023, 1)],
        'eGRID': [2014, 2016, 2018, 2019, 2020, 2021],
        'NEI': [2011, 2014, 2017, 2020],
        'RCRAInfo': [x for x in range(2011, 2023, 2)],
        'TRI': [x for x in range(2011, 2023, 1)],
    }

    r_dict = {}
    for key in STEWI_DATA_VINTAGES.keys():
        avail_years = STEWI_DATA_VINTAGES[key]
        y_idx = linear_search(avail_years, year)
        if y_idx != -1:
            r_dict[key] = STEWI_DATA_VINTAGES[key][y_idx]
    return r_dict


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


def linear_search(lst, target):
    """Backwards search for the value less than or equal to a given value.

    Parameters
    ----------
    lst : list
        A list of numerically sorted data (lowest to highest).
    target : int, float
        A target value (e.g., year).

    Returns
    -------
    int
        The index of the search list associated with the value equal to or
        less than the target, else -1 for a target out-of-range (i.e., smaller than the smallest entry in the list).

    Examples
    --------
    >>> NEI_YEARS = [2011, 2014, 2017, 2020]
    >>> linear_search(NEI_YEARS, 2020)
    3
    >>> linear_search(NEI_YEARS, 2019)
    2
    >>> linear_search(NEI_YEARS, 2018)
    2
    >>> linear_search(NEI_YEARS, 2010)
    -1
    """
    for i in range(len(lst) - 1, -1, -1):
        if lst[i] <= target:
            return i
    return -1


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


def read_ba_codes_old():
    """Create a data frame of balancing authority names and codes.

    Provides a common source for balancing authority names, as well as
    FERC an EIA region names.

    Returns
    -------
    pandas.DataFrame
        Index is set to 'BA_Acronym'. Columns include:

        - 'BA_Name' (str)
        - 'NCR ID#' (str)
        - 'EIA_Region' (str)
        - 'FERC_Region' (str)
        - 'EIA_Region_Abbr' (str)
        - 'FERC_Region_Abbr' (str)
    """
    #
    # See EIA 930 Reference Tables for updated list of BA codes
    # https://www.eia.gov/electricity/gridmonitor/about
    ba_codes = pd.concat([
        pd.read_excel(
            os.path.join(data_dir, "BA_Codes_930.xlsx"),
            header=4,
            sheet_name="US"
        ),
        pd.read_excel(
            os.path.join(data_dir, "BA_Codes_930.xlsx"),
            header=4,
            sheet_name="Canada"
        ),
    ])
    ba_codes.rename(
        columns={
            "etag ID": "BA_Acronym",
            "Entity Name": "BA_Name",
            "NCR_ID#": "NRC_ID",
            "Region": "Region",
        },
        inplace=True,
    )
    ba_codes.set_index("BA_Acronym", inplace=True)

    return ba_codes


def read_ba_codes():
    # IN PROGRESS
    #
    # Referenced in combinatory.py, eia_io_trading.py, import_impacts.py
    #
    # Columns for sheet, "BAs" (as of 2024), include:
    # - BA Code (str)
    # - BA Name (str)
    # - Time Zone (str): For example, "Eastern," "Central" or "Pacific"
    # - Region/Country Code (str): EIA region code
    # - Region/Country Name (str): EIA region name
    # - Generation Only BA (str): "Yes" or "No"
    # - Demand by BA Subregion (str): "Yes" or "No"
    # - U.S. BA (str): "Yes" or "No"
    # - Active BA (str): "Yes" or "No"
    # - Activation Date: (str/NA): mostly empty, a few years are available
    # - Retirement Date (str/NA): mostly empty, a few years are available
    eia_ref_url = "https://www.eia.gov/electricity/930-content/EIA930_Reference_Tables.xlsx"

    # BA-to-FERC mapping is based on an intermediate EIA-to-FERC map,
    # which was completed as a part of Electricity Grid Mix Explorer v4.2.
    EIA_to_FERC = {
        "California": "CAISO",
        "Carolinas": "Southeast",
        "Central": "SPP",
        "Electric Reliability Council of Texas, Inc.": "ERCOT",
        "Florida": "Southeast",
        "Mid-Atlantic": "PJM",
        "Midwest": "MISO",
        "New England ISO": "ISO-NE",
        "New York Independent System Operator": "NYISO",
        "Northwest": "Northwest",
        "Southeast": "Southeast",
        "Southwest": "Southwest",
        "Tennessee Valley Authority": "Southeast",
        # Issue #291 - Entries in EIA930 have changed - adding those here
        "Texas": "ERCOT",
        "New York": "NYISO",
        "New England": "ISO-NE",
        "Tennessee": "Southeast",
        # Add Canada and Mexico
        "Canada": "Canada",
        "Mexico": "Mexico",
    }
    FERC_ABBR = {
        "CAISO": "CAISO",
        "ERCOT": "ERCOT",
        "ISO-NE": "ISO-NE",
        "MISO": "MISO",
        "Northwest": "NW",
        "NYISO": "NYISO",
        "PJM": "PJM",
        "Southeast": "SE",
        "Southwest": "SW",
        "SPP": "SPP",
        # Add Canada and Mexico
        "Canada": "CAN",
        "Mexico": "MEX",
    }
    df = pd.read_excel(eia_ref_url)
    df = df.rename(columns={
        'BA Code': 'BA_Acronym',
        'BA Name': 'BA_Name',
        'Region/Country Code': 'EIA_Region_Abbr',
        'Region/Country Name': 'EIA_Region',
    })
    df['FERC_Region'] = df['EIA_Region'].map(EIA_to_FERC)
    df['FERC_Region_Abbr'] = df['FERC_Region'].map(FERC_ABBR)
    df = df.set_index("BA_Acronym")

    # TODO: save this as a CSV and read it if available.

    return df


def read_eia_api(url, url_try=0, max_tries=5):
    """Return a JSON data response from EIA's API.

    Parameters
    ----------
    url : str
        The URL in proper syntax.
    url_try : int
        Internal counter for URL retries; default is 0
    max_tries : int
        When to stop retrying; default is 5

    Returns:
    (dict, int)
        The JSON response and URL try count.
        The JSON dictionary includes keys:

        -   'response' (dict): with keys:

            -   'total' (int): count of records in 'data'
            -   'dateFormat' (str): For example, 'YYYY-MM-DD"T"HH24'
            -   'frequency' (str): For example, 'hourly'
            -   'description' (str): Data description
            -   'data' (list): Dictionaries with keys:

                -   'period'
                -   'fromba': for ID only
                -   'fromba-name': for ID only
                -   'toba': for ID only
                -   'toba-name': for ID only
                -   'respondent': for D and NG only
                -   'respondent-name': for D and NG only
                -   'type': for D and NG only
                -   'type-name': for D and NG only
                -   'value'
                -   'value-units'

        -   'request' (dict): Parameters sent to the API
        -   'apiVersion' (str): API version string (e.g., '2.1.7')
        -   'ExcelAddInVersion' (str): AddIn version string (e.g., '2.1.0')
    """
    r_dict = {}
    url_try += 1
    #adding 20s timeout to avoid long delays due to server issues.
    r = requests.get(url, timeout=20)
    r_status = r.status_code
    if r_status == 200:
        r_content = r.content
        try:
            r_dict = r.json()
        except:
            # If at first you, fail...
            r_content = decode_str(r_content)
            r_dict = json.loads(r_content)
    else:
        if url_try < max_tries:
            r_dict, url_try = read_eia_api(url, url_try, max_tries)
        else:
            logging.error("Requests failed!")

    return (r_dict, url_try)


def read_json(json_path):
    """Read a JSON-formatted file into a Python dictionary.

    Parameters
    ----------
    json_path : str
        A file path to a JSON-formatted file.

    Returns
    -------
    dict
        A dictionary-formatted version of the file.
        If any errors are encountered, or if the file does not exist,
        then an empty dictionary is returned.
    """
    r_dict = {}
    if isinstance(json_path, str) and os.path.isfile(json_path):
        try:
            with open(json_path, 'r') as f:
                r_dict = json.load(f)
        except:
            logging.error("Failed to read dictionary from %s" % json_path)
        else:
            logging.info("Read file to JSON")
    elif not os.path.isfile(json_path):
        logging.warning("Failed to find file, %s" % json_path)
    else:
        logging.warning(
            "Expected string file name, received %s" % type(json_path))

    return r_dict


def read_log_file(idx=1):
    """A helper function for analyzing ElectricityLCI's log files.

    Parameters
    ----------
    idx : int, optional
        The log file index (0--9), by default 1

    Returns
    -------
    pandas.DataFrame
        A data frame with rows equal to number of logging statements.
        If the log file is not found, an empty data frame is returned.
        Columns include the following:

        - 'date', timestamp
        - 'level', str, logging level (e.g., 'DEBUG')
        - 'module', str, module name (e.g., 'utils')
        - 'method', str, function name (e.g., 'download_unzip')
        - 'message', str, logging message
    """
    # The standard log file naming scheme
    search_file = "elci.log.%s" % idx
    search_path = os.path.join(output_dir, search_file)

    # See :func:`get_logger` for log file format
    #   (1)date:(2)log lv:(3)log name:(4)function:(5)message
    p = re.compile(
        "^(\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}:\\d{2}\\.+\\d{0,3}):"
        "([A-Z]+):(\\w+):(\\w+):(.*)$"
    )

    # Initialize lists for storing data
    date_lst = []
    level_lst = []
    name_lst = []
    func_lst = []
    msg_lst = []

    # Open the file, parse its contents
    if os.path.isfile(search_path):
        logging.info("Found log file, '%s'!" % search_file)

        with open(search_path, 'r') as f:
            for line in f:
                r = p.match(line)
                if r:
                    date_lst.append(r.group(1))
                    level_lst.append(r.group(2))
                    name_lst.append(r.group(3))
                    func_lst.append(r.group(4))
                    msg_lst.append(r.group(5))

    # Put data (or empty lists) in a data frame.
    df = pd.DataFrame({
        'date': date_lst,
        'level': level_lst,
        'module': name_lst,
        'method': func_lst,
        'message': msg_lst,
    })

    # Convert date strings to datetime objects
    try:
        # NOTE: The decimal seconds show up in my log files [250211;TWD]
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S.%f")
    except:
        logging.error(
            "Failed to parse datetime string format (e.g., '%s')" % (
                df.loc[0, 'date'])
        )
        pass

    return df


def rollover_logger(logger):
    """Helper method to rollover a named Rotating File Handler.

    Parameters
    ----------
    logger : logging.Logger
        A logger (e.g., root logger)
    """
    try:
        idx = [x.name for x in logger.handlers].index("elci_rfh")
    except ValueError:
        idx = -1

    # Rollover the rotating file handler (if found)
    if idx != -1:
        logger.handlers[idx].doRollover()


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


def write_csv_to_output(f_name, data):
    """Write data to CSV file in the outputs directory.

    Parameters
    ----------
    f_name : str
        A file name with '.csv' file extension.
    data : str, pandas.DataFrame
        A data object to be written to file.
        A data frame is written using `to_csv` without index.
        A string is written to a plain text file.

    Raises
    ------
    TypeError : If the data type is not recognized.
    """
    f_path = os.path.join(output_dir, f_name)
    if os.path.isfile(f_path):
        logging.warn("File exists! Overwriting %s" % f_path)
    if isinstance(data, pd.DataFrame):
        try:
            data.to_csv(f_path, index=False)
        except:
            logging.error("Failed to write '%s' to file" % f_path)
        else:
            logging.info("Wrote data to file, %s" % f_path)
    elif isinstance(data, str):
        try:
            with open(f_path, 'w') as f:
                f.write(data)
        except:
            logging.error("Failed to write '%s' to file" % f_path)
        else:
            logging.info("Wrote data to file, %s" % f_path)
    else:
        logging.error("Data type, %s, not recognized!" % type(data))
        raise TypeError("Data type, %s, not recognized!" % type(data))
