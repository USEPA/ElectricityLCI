#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# eia_io_trading.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import json
import logging
import os
import time
import zipfile

import numpy as np
import pandas as pd
import re

from electricitylci.globals import data_dir
from electricitylci.globals import paths
from electricitylci.globals import API_SLEEP
from electricitylci.bulk_eia_data import download_EBA
from electricitylci.bulk_eia_data import row_to_df
from electricitylci.bulk_eia_data import ba_exchange_to_df
from electricitylci.bulk_eia_data import check_EBA_vintage
from electricitylci.model_config import model_specs
import electricitylci.eia923_generation as eia923
import electricitylci.eia860_facilities as eia860
from electricitylci.utils import read_ba_codes
from electricitylci.utils import check_output_dir
from electricitylci.utils import download
from electricitylci.utils import read_eia_api
from electricitylci.utils import write_csv_to_output
from electricitylci.process_dictionary_writer import (
    exchange,
    process_table_creation_con_mix,
    exchange_table_creation_input_con_mix,
    exchange_table_creation_ref,
)


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses data from the EIA bulk data file to determine
how much electricity flows between either balancing authority areas (BAAs)
or Federal Energy Regulatory Commission (FERC) regions. These electricity
flows are then used to generate the consumption mix for a given region or
balancing authority area.

The default data provider for the bulk U.S. Electric System Operating Data
is the EIA API open data portal. Request a free API key here:

https://www.eia.gov/opendata/

The main trading model is based on the quasi-input-output model by
Qu et al. (2017) and Qu et al. (2018).

References:

-   Qu, S. et al. (2017). A Quasi-Input-Output model to improve the
    estimation of emission factors for purchased electricity from
    interconnected grids. Applied Energy, 200, 249-259.
    https://doi.org/10.1016/j.apenergy.2017.05.046
-   Qu, S. et al. (2018). Virtual CO2 Emission Flows in the Global
    Electricity Trade Network. Environmental Science & Technology,
    52(11), 6666-6675. https://doi.org/10.1021/acs.est.7b05191

Last updated:
    2024-09-25
"""
__all__ = [
    "ba_io_trading_model",
    "olca_schema_consumption_mix",
    "qio_model",
]


##############################################################################
# GLOBALS
##############################################################################
REGION_ACRONYMS = [
    'TVA', 'MIDA', 'CAL', 'CAR', 'CENT', 'ERCO', 'FLA',
    'MIDW', 'ISNE', 'NYIS', 'NW', 'SE', 'SW',
]
'''list : Region acronyms for BA-to-BA trade.'''


##############################################################################
# FUNCTIONS
##############################################################################
def _get_ca_imports(just_read=False):
    """Return net annual Canadian exports and net annual exports to US
    balancing authorities based on www.cer-rec.gc.ca electricity export
    sales workbook.

    Canadian province and US region to balancing authority mapping provided
    by NETL.

    Workbook source is https://www.cer-rec.gc.ca/en/index.html (last accessed
    March 13, 2024).

    Parameters
    ----------
    just_read : bool, optional
        Helper to read existing Excel workbook (skips download), by default False

    Returns
    -------
    tuple
        Length two.

        -   pandas.DataFrame: net annual exports by Canadian balancing
            authority (MWh)
        -   pandas.DataFrame: net annual exports by Canadian balancing
            authority to US balancing authority (MWh)

    Raises
    ------
    OSError
        If Excel workbook is unreachable or the output directory cannot be
        created to save the workbook.
    """
    # Source:
    # Data and analysis > Energy commodities > Electricity and renewables >
    # Electricity Trade Summary
    url = (
        "https://www.cer-rec.gc.ca/en"
        "/data-analysis/energy-commodities/electricity/statistics"
        "/electricity-trade-summary"
        "/electricity-trade-summary-resume-echanges-commerciaux"
        "-electricite.xlsx"
    )
    # Map taken from 'CA_ExportSalesSummaryReport.xlsx' developed by NETL.
    CA_BA_MAP = {
        "Alberta":  "AESO",
        "British Columbia": "BCHA",
        "Québec": "HQT",
        "Manitoba": "MHEB",
        "New Brunswick": "NBSO",
        "Nova Scotia": "NSPI",
        "Ontario": "IESO",
        "Saskatchewan": "SPC",
        "Newfoundland and Labrador": "NEWL",
    }
    US_BA_MAP = {
        "Alaska": "BPAT",
        "Arizona": "BPAT",
        "California": "BPAT",
        "Colorado": "BPAT",
        "Idaho": "BPAT",
        "Indiana": "MISO",
        "Kansas": "MISO",
        "Maine": "ISNE",
        "Massachusetts": "ISNE",
        "Michigan": "MISO",
        "Minn / N. Dakota": "MISO",
        "Minnesota": "MISO",
        "Mississippi": "MISO",
        "Montana": "BPAT",
        "Nebraska": "MISO",
        "Nevada": "BPAT",
        "New England-ISO": "ISNE",
        "New Jersey": "PJM",
        "New Mexico": "BPAT",
        "New York": "NYIS",
        "North Dakota": "MISO",
        "Ohio": "PJM",
        "Oregon": "BPAT",
        "Pennsylvania": "PJM",
        "Pennsylvania Jersey Maryland Power Pool": "PJM",
        "Texas": "MISO",
        "Utah": "BPAT",
        "Vermont": "ISNE",
        "Washington": "BPAT",
        "Wyoming": "BPAT",
        "New Hampshire": "ISNE",
        "Florida": "FPL",
        # HOTFIX: missing state maps [2024-03-14; TWD]
        # https://github.com/USEPA/ElectricityLCI/issues/236
        'Illinois': 'MISO',
        'Missouri': 'AECI',
        'South Dakota': 'SWPP',
        'Oklahoma': 'SWPP',
        'Maryland': 'PJM',
        'Arkansas': 'MISO',
    }

    file_name = os.path.basename(url)
    out_dir = os.path.join(paths.local_path, "cer_rec")
    if check_output_dir(out_dir):
        file_path = os.path.join(out_dir, file_name)
        # Allow user to by-pass the download if it exists.
        if just_read and os.path.isfile(file_path):
            pass
        else:
            download(url, file_path)

        if os.path.isfile(file_path):
            # Read into data frame, which includes six columns:
            # - 'Date' (int): year
            # - 'Volume (MW.h)' (float): exchange amount
            # - 'Province (Eng. / Ang.)' (str): Canadian province
            # - 'Destination (Eng. / Ang.)' (str): US destination
            # - 'Province (Fra.)' (str): Canadian province
            # - 'Destination (Fra.)' (str): US destination
            df = pd.read_excel(file_path, sheet_name='Fig. 2')
            df = df.rename(columns={
                'Volume (MW.h)': 'Volume',
                'Province (Eng. / Ang.)': 'Province',
                'Destination (Eng. / Ang.)': 'Destination',
            })

            # Map balancing authority codes to regions.
            # NOTE: In the Excel workbook from March 2024, there were two
            #       US provinces: Michigan and New York.
            df['ca_ba'] = df['Province'].map(CA_BA_MAP)
            # NOTE: 'Ontario' destination is unmatched, March 2024.
            df['us_ba'] = df['Destination'].map(US_BA_MAP)

            # Aggregate export volumes between CA and US by year
            agg_df = df.groupby(by=['ca_ba', 'us_ba', 'Date']).agg(
                {'Volume': 'sum'})
            agg_df = agg_df.reset_index()

            # Aggregate export volumes by CA by year
            agg_ca = df.groupby(by=['ca_ba', 'Date']).agg({'Volume': 'sum'})
            agg_ca = agg_ca.reset_index()

            logging.info("Read Canadian exports from https://www.cer-rec.gc.ca")
            return (agg_df, agg_ca)
        else:
            raise OSError("Failed to access file, %s" % file_path)
    else:
        raise OSError("Failed to create output directory, %s" % out_dir)


def _read_ba():
    """Generate the Balancing Authority data frame and acronym and FERC lists.

    Returns
    -------
    tuple
        A tuple of length three.

        - pandas.DataFrame : Balancing authority data
          Columns include the following.

            * 'BA_Acronym' (str) : short letter abbreviation
            * 'BA_Name' (str) : long name
            * 'NCR ID#' (str) : NRC identifier
            * 'EIA_Region' (str) : region name (includes Canada)
            * 'FERC_Region' (str) : FERC region (includes Canada)
            * 'EIA_Region_Abbr' (str) : EIA region abbreviation
            * 'FERC_Region_Abbr' (str) : FERC region abbreviation

        - list : U.S. Balancing Authority abbreviation codes
        - list : U.S. FERC region codes
    """
    ba_df = read_ba_codes()
    US_BA_acronyms = sorted(list(
        ba_df.query("EIA_Region != 'Canada'").index.values
    ))
    df_BA_NA = ba_df.reset_index()
    ferc_list = df_BA_NA['FERC_Region_Abbr'].unique().tolist()

    return df_BA_NA, US_BA_acronyms, ferc_list


def _check_api(key, owner, r_txt):
    """Helper function to check and request for API key.

    Parameters
    ----------
    key : str, Nonetype
        The key to be checked.
    owner : str
        The API owner (e.g., 'EIA' or 'EPA').
    r_txt : str
        Helper text for acquiring an API key (e.g., registration URL).

    Returns
    -------
    str
        API key as provided by the user.
    """
    if key is None or key == "":
        key = input("Enter %s API key: " % owner)
        key = key.strip()
        if key == "":
            logging.warning(
                "No API key given!"
                f"Sign up here: {r_txt}"
            )
    return key


def _check_json(d):
    """Check that EBA.zip JSON data has info.

    If a JSON entry is missing data, send a critical logging statement.
    The consequence of using this data is that consumption mix processes
    will not be created in the JSON-LD.
    See https://github.com/USEPA/ElectricityLCI/discussions/254.

    Parameters
    ----------
    d : dict
        JSON line read from EBA.zip
    """
    name = d.get('name', 'n/a')
    series = d.get('series_id', 'n/a')
    start = d.get('start', None)
    end = d.get('end', None)
    data = d.get('data', [])
    if start is None or end is None or len(data) == 0:
        logging.critical("No JSON data for %s, '%s'" % (series, name))


def _read_bulk(ba_cols):
    """Handle both ZIP and API data sources for bulk U.S. Electric System
    Operating Data managed by model_config.

    Parameters
    ----------
    ba_cols : list
        A list of balancing authority short codes.
        These are used for querying API demand and net generation data.

    Returns
    -------
    tuple
        A tuple of length three.
        Each item is a list.
        See :func:`_read_bulk_api` and :func:`read_bulk_zip` for details.
    """
    if model_specs.use_eia_bulk_zip:
        logging.info("Reading EIA bulk zip")
        return _read_bulk_zip()
    else:
        logging.info("Reading EIA API bulk data")
        return _read_bulk_api(ba_cols)


def _write_bulk_api(row_data, output_file):
    """Helper function to write out bulk row data to pseudo JSON file.

    Note that the output format does not comply with JSON strictly; rather,
    each row is a dictionary created by json.dumps(). This allows each row
    to be read using json.loads().

    Parameters
    ----------
    row_data : list
        A list of dictionaries to be written to file.
    output_file : str
        A file path for writing data. The parent directory's existence is
        checked using :func:`check_output_dir`.
    """
    output_dir = os.path.dirname(output_file)
    check_output_dir(output_dir)
    # Get away with using write_csv_to_output in utils.py for writing strings.
    d_txt = "\n".join([json.dumps(x) for x in row_data])
    write_csv_to_output(output_file, d_txt)


def _read_bulk_json(json_file):
    """Helper method to read JSON data written by :func:`write_bulk_api`

    Note that the plain text file is not strictly in JSON format; rather,
    each line is a dictionary produced using ``json.dumps``.

    Parameters
    ----------
    json_file : str
        File path to an existing data file.

    Returns
    -------
    list
        A list of dictionaries representing bulk (demand, net gen, interchange)
        data.
    """
    row_data = []
    if os.path.isfile(json_file):
        with open(json_file, 'r') as f:
            for line in f:
                d = json.loads(line)
                row_data.append(d)

    return row_data


def _read_bulk_api(ba_cols):
    """Read demand, net generation, and interchange data from EIA's API.

    Parameters
    ----------
    ba_cols : list
        A list of balancing authority short codes.
        Used for querying regions for demand and net generation.

    Returns
    -------
    tuple
        A tuple of length three.

        - list : rows associated with net generation.
        - list : rows associated with BA-to-BA interchange.
        - list : rows associated with demand.

    Notes
    -----
    For API registration, go to: https://www.eia.gov/opendata/.

    See https://github.com/USEPA/ElectricityLCI/discussions/254 for details.

    If you don't want to pass ba_cols, you can find all the respondents
    on the API by calling (adding ?api_key=YOUR-KEY at the end):
    https://api.eia.gov/v2/electricity/rto/daily-region-data/facet/respondent
    The response dictionary should have a key, 'facets' with 'id' and 'name'
    fields for each BA/region.
    """
    # Define the URLs for the two sub-domains and for hourly and daily data.
    _baseurl = "https://api.eia.gov/v2/"
    _sub_domain_h = "electricity/rto/region-data/data/"
    _sub_domain_d = "electricity/rto/daily-region-data/data/"
    _sub_domain2_h = "electricity/rto/interchange-data/data/"
    _sub_domain2_d = "electricity/rto/daily-interchange-data/data/"
    _freq = "daily" # or 'local-hourly' or 'daily'
    # NOTE: if using 'local-hourly' these times must be in timezone format!
    # NOTE: the API time filter is based on day (not hour)!
    _yr = model_specs.NETL_IO_trading_year
    _start = "%d-01-01" % _yr
    _end = "%d-12-31" % _yr

    # Correct URL based on frequency (daily vs hourly)
    _sub_domain = _sub_domain_h
    _sub_domain2 = _sub_domain2_h
    if _freq == 'daily':
        _sub_domain = _sub_domain_d
        _sub_domain2 = _sub_domain2_d

    # LOCAL DATA STORE MANAGEMENT
    data_store = os.path.join(paths.local_path, "bulk_data")
    d_rows_file = os.path.join(data_store, "eia_bulk_demand_%s.json" % _yr)
    ng_rows_file = os.path.join(data_store, "eia_bulk_netgen_%s.json" % _yr)
    id_rows_file = os.path.join(data_store, "eia_bulk_id_%s.json" % _yr)
    d_rows_exists = os.path.isfile(d_rows_file)
    ng_rows_exists = os.path.isfile(ng_rows_file)
    id_rows_exists = os.path.isfile(id_rows_file)

    # Initialize return lists
    DEMAND_ROWS = []
    NET_GEN_ROWS = []
    BA_TO_BA_ROWS = []

    # Initialize API strings
    new_api = "https://www.eia.gov/opendata/"
    api_key = model_specs.eia_api_key

    # TODO: consider having a configuration setting to force API; otherwise,
    # it is up to the end user to delete the JSON files from bulk_data to
    # prompt the API a second time (assuming success on the first run).

    # Get bulk demand
    if d_rows_exists:
        logging.info("Reading local %s" % os.path.basename(d_rows_file))
        DEMAND_ROWS= _read_bulk_json(d_rows_file)
    else:
        logging.info("Querying EIA API for bulk demand data")
        api_key = _check_api(api_key, 'EIA', new_api)
        DEMAND_ROWS, _ok = _read_dng_api(
            _baseurl, _sub_domain, api_key, _freq, _start, _end, ba_cols, 'D')
        if _ok:
            _write_bulk_api(DEMAND_ROWS, d_rows_file)

    # Get bulk net generation
    if ng_rows_exists:
        logging.info("Reading local %s" % os.path.basename(ng_rows_file))
        NET_GEN_ROWS = _read_bulk_json(ng_rows_file)
    else:
        logging.info("Querying EIA API for bulk net generation data")
        api_key = _check_api(api_key, 'EIA', new_api)
        NET_GEN_ROWS, _ok = _read_dng_api(
            _baseurl, _sub_domain, api_key, _freq, _start, _end, ba_cols, 'NG')
        if _ok:
            _write_bulk_api(NET_GEN_ROWS, ng_rows_file)

    # Get bulk interchange
    if id_rows_exists:
        logging.info("Reading local %s" % os.path.basename(id_rows_file))
        BA_TO_BA_ROWS = _read_bulk_json(id_rows_file)
    else:
        logging.info("Querying EIA API for bulk interchange data")
        api_key = _check_api(api_key, 'EIA', new_api)
        BA_TO_BA_ROWS = _read_id_api(
            _baseurl, _sub_domain2, api_key, _freq, _start, _end)
        if True:
            _write_bulk_api(BA_TO_BA_ROWS, id_rows_file)

    return (NET_GEN_ROWS, BA_TO_BA_ROWS, DEMAND_ROWS)


def _read_bulk_zip():
    """Read and parse EIA's U.S. Electric System Operating Data.

    Creates three lists of JSON-based dictionaries.
    Each dictionary contains metadata and a time series of data.
    Time series data appear to go back to around 2015.

    Returns
    -------
    tuple
        A tuple of length three.

        - list : rows associated with net generation.
        - list : rows associated with BA-to-BA trade.
        - list : rows associated with demand.
    """
    # Initialize return lists
    NET_GEN_ROWS = []
    BA_TO_BA_ROWS = []
    DEMAND_ROWS = []

    # Changing to regex matches to allow compatibility with past and present
    # bulk data. [2024-08-16; MJ]
    ngh_matches = "^EBA[\\S\\w\\d]+[^NG]\\.NG\\.H$"
    idh_matches = "^EBA.+\\.ID\\.H$"
    dh_matches = "^EBA.+\\.D\\.H$"

    # HOTFIX: Check file vintage [2024-03-12; TWD]
    path = os.path.join(paths.local_path, 'bulk_data', 'EBA.zip')
    if model_specs.bypass_bulk_vintage:
        logging.info("Skipping EBA vintage check")
    else:
        check_EBA_vintage()

    try:
        z = zipfile.ZipFile(path, 'r')
    except FileNotFoundError:
        logging.info("Downloading new bulk data")
        download_EBA()
        z = zipfile.ZipFile(path, 'r')
    else:
        logging.info("Using existing bulk data download")

    logging.info("Loading bulk data to json")
    with z.open('EBA.txt') as f:
        for line in f:
            # To improve compatibility with old/new EBA.zip
            f_json = json.loads(line)

            # All the entries should have a 'series_id' and an 'f' key.
            # 'H' for UTC hourly; 'HL' for local hourly; hard-coded to UTC.
            # See https://github.com/USEPA/ElectricityLCI/discussions/254.
            if 'series_id' in f_json.keys() and f_json.get('f', '') == 'H':
                series_id = f_json['series_id']

                # LEGACY NOTES --- The 2016 Baseline
                # All but one BA is reporting net generation in UTC
                # and local time. For that one BA (GRMA) only UTC time is
                # reported - so only pulling that for now.

                if re.search(ngh_matches, series_id) is not None:
                    # HOTFIX: add single instance of JSON line checker
                    # will throw about 82 warnings that data are not available.
                    # (e.g., August 19, 2024 EBA.zip)
                    _check_json(f_json)
                    NET_GEN_ROWS.append(f_json)

                # Similarly there are 5 interchanges that report interchange
                # in UTC but not in local time.
                elif re.search(idh_matches, series_id) is not None:
                    # Split on intersection, rstrip "EBA."
                    s_txt = f_json['series_id'].split('-')[0][4:]
                    if s_txt not in REGION_ACRONYMS:
                        BA_TO_BA_ROWS.append(f_json)

                # Keeping these here just in case
                elif re.search(dh_matches, series_id) is not None:
                    DEMAND_ROWS.append(f_json)

    logging.debug(f"Net gen rows: {len(NET_GEN_ROWS)}")
    logging.debug(f"BA to BA rows:{len(BA_TO_BA_ROWS)}")
    logging.debug(f"Demand rows:{len(DEMAND_ROWS)}")

    return (NET_GEN_ROWS, BA_TO_BA_ROWS, DEMAND_ROWS)


def _read_id_api(baseurl, sub_domain, api_key, freq, start, end):
    """Return list of interchanges at the given frequency and time period."""
    r_list = []
    d_dict = {}

    if freq not in ['daily', 'hourly', 'local-hourly']:
        raise ValueError(
            "Frequency must be 'daily' 'hourly' or 'local-hourly', "
            "not '%s'!" % freq)
    if api_key is None or api_key == '':
        raise ValueError(
            "Missing EIA API key! Register online "
            "https://www.eia.gov/opendata/"
        )

    # Provide starting values to get into the while loop.
    recs_captured = 0
    total_recs = 2
    offset = 0
    while recs_captured < (total_recs - 1):
        _url = (
            f"{baseurl}{sub_domain}?api_key={api_key}&out=json"
            f"&frequency={freq}"
            f"&start={start}"
            f"&end={end}"
            "&sort[0][column]=period"
            "&sort[0][direction]=asc"
            "&data[]=value"
            f"&offset={offset}"
            "&length=4999"
        )

        # Variable idx for series ID, and add a timezone for daily downloads.
        _idx = 'H'
        if freq == 'daily':
            _idx = 'D'
            _url += "&facets[timezone][]=Central"
        elif freq == 'local-hourly':
            _idx = 'HL'

        # Make request and sleep, so as to not be a hater.
        d_json, _ = read_eia_api(_url)
        time.sleep(API_SLEEP)

        # Check response
        d_resp = d_json.get('response', {})
        try:
            # The total number of records available, not necessarily how
            # many you get this call.
            total_recs = d_resp.get("total", 0)
            total_recs = int(total_recs)
        except:
            total_recs = 0

        # See how many records are in this response and update your counters
        # NOTE: this is different from 'total', which is all records.
        d_rec = len(d_resp.get('data', []))
        recs_captured += d_rec
        offset = recs_captured + 1
        logging.info("Retrieved %d entries out of %d ID records" % (
            recs_captured, total_recs))

        # Proceed if you have data.
        if d_rec > 0:
            for d in d_resp.get('data', []):
                # Recreate the data format of EBA.zip
                f_ba = d['fromba']
                t_ba = d['toba']

                # Employ the same filter used in read_bulk_zip
                if f_ba not in REGION_ACRONYMS:
                    series_id = "EBA.%s-%s.ID.%s" % (f_ba, t_ba, _idx)

                    # Use d_dict to store each unique BA-BA pairing and
                    # build-out the data list. It's done this way because
                    # we know the trade regions of interest, REGION_ACRONYMS,
                    # but we don't know who they're trading with.
                    # HOTFIX: for some reason, I cannot stop duplicate
                    # entries, so use dictionary for uniqueness!
                    if series_id in d_dict.keys():
                        d_dict[series_id]['data'][d['period']] = d['value']
                    else:
                        d_dict[series_id] = {
                            'series_id': series_id,
                            'data': {}
                        }
                        d_dict[series_id]['data'][d['period']] = d['value']

    # Take the data lists and series ids and make them a list of dicts.
    for k in d_dict.keys():
        d = d_dict[k]
        # Convert dictionary to list (will likely lose sorting)
        d['data'] = [[x, y] for x, y in d['data'].items()]
        r_list.append(d)

    return r_list


def _read_dng_api(baseurl, sub_domain, api_key, freq, start, end, ba_cols, m):
    """Return list of net gen or demand for given frequency and time period."""
    r_list = []
    if m not in ['D', 'NG']:
        raise ValueError("Metric must be either 'D' or 'NG', not '%s'!" % m)
    if freq not in ['daily', 'hourly', 'local-hourly']:
        raise ValueError(
            "Frequency must be 'daily' 'hourly' or 'local-hourly', "
            "not '%s'!" % freq)

    # For logging
    _metric = 'demand'
    if m == 'NG':
        _metric = 'net gen'

    # For demand and net gen, we only need U.S. BA areas:
    # Due to API response limits, request each BA individually.
    _is_okay = True
    for ba in ba_cols:
        _url = (
            f"{baseurl}{sub_domain}?api_key={api_key}&out=json"
            f"&frequency={freq}"
            f"&start={start}"
            f"&end={end}"
            f"&facets[respondent][]={ba}"
            f"&facets[type][]={m}"
            "&data[]=value"
        )

        # Variable idx for series ID, and add a timezone for daily downloads.
        _idx = 'H'
        if freq == 'daily':
            _idx = 'D'
            _url += "&facets[timezone][]=Central"
        elif freq == 'local-hourly':
            _idx = 'HL'

        # Make request and sleep, so as to not be a hater.
        d_json, url_tries = read_eia_api(_url, max_tries=5)
        time.sleep(API_SLEEP)

        # Check for max retries
        if url_tries == 5:
            _is_okay = False

        # Check response
        d_resp = d_json.get('response', {})
        if 'warnings' in d_resp.keys():
            _is_okay = False  # something didn't work right
            logging.warning(d_resp['warnings'])
        try:
            d_tot = d_resp.get("total", 0)
            d_tot = int(d_tot)
        except:
            d_tot = 0
        logging.info("Retrieved %d %s %s entries in %d request(s)" % (
            d_tot, ba, _metric, url_tries))

        # Proceed if there is data:
        if d_tot > 0:
            # Recreate the data format of EBA.zip
            d_dict = {}
            d_dict['series_id'] = "EBA.%s-ALL.%s.%s" % (ba, m, _idx)
            # HOTFIX: Can't get rid of duplicate entries in the daily API,
            # even with timezone setting! Use dictionary for uniqueness.
            d_dict['data'] = {}
            for d in d_resp.get('data', []):
                d_dict['data'][d['period']] = d['value']
            # Convert dictionary back to list of lists
            d_dict['data'] = [[x,y] for x,y in d_dict['data'].items()]
            r_list.append(d_dict)

    return r_list, _is_okay


def _read_ca_imports(year):
    """Return the Canadian import data frames based on Canadian electricity
    sales data (either from eLCI data hub or from source).

    For year 2005-2020, data are from provided CSV files:

    -   CA_Imports_Gen.csv
    -   CA_Imports_Rows.csv

    These CSV files were developed using Canadian electricity export sales.
    The methods are captured in :func:`get_ca_imports`, which includes years
    past 2020.

    Parameters
    ----------
    year : int
        The trading year (e.g., 2020).

    Returns
    -------
    tuple
        Length two.

        -   pandas.Series: Net annual Canadian export sales (MWh).
            Indices are balancing authority codes and the values are the
            trade amounts for the given trade year.
        -   pandas.DataFrame: Net annual Canadian exports to US balancing
            authorities (MWh)
    """
    # The Canadian annual export data from 2014-2020 is available w/ eLCI.
    # The content is broken across the following CSV files.
    data_file = os.path.join(data_dir, "CA_Imports_Gen.csv")
    rows_file = os.path.join(data_dir, "CA_Imports_Rows.csv")

    # Initialize return data frames
    df = None
    rf = None

    # Ensure year is integer and yr is the string version.
    year = int(year)
    yr = str(year)

    # Read net annual Canadian exports.
    if os.path.isfile(data_file):
        df = pd.read_csv(data_file, index_col=0)
        if yr in df.columns:
            # The CSV exists and the trading year is available.
            logging.info("Read Canadian net exports from %s" % data_file)
            df = df[yr].copy()
        else:
            df = None

    # Read net annual Canadian exports to their US BA destinations.
    if os.path.isfile(rows_file):
        rf = pd.read_csv(rows_file, index_col=0)
        if yr in rf.columns and 'us_ba' in rf.columns:
            # The CSV exists and the trading year is available.
            logging.info("Read Canadian-to-US exports from %s" % rows_file)
            rf = rf[['us_ba', yr]].copy()
            rf = rf.pivot(columns='us_ba', values=yr)
        else:
            rf = None

    # If, for any reason, the net exports or Canadian-US export data are not
    # available, then use the CER-rec data download.
    if df is None or rf is None:
        # Either the CSV is gone or the trading year is not available.
        #  ca_us (pandas.DataFrame): Canadian-to-US BA exchanges by year
        #  ca_df (pandas.DataFrame): Canadian BA net exchanges (MWh) by year
        ca_us, ca_df = _get_ca_imports()

        # Create series w/ ca_ba as the index and year as the name.
        ca_df = ca_df.query("Date == %d" % year).copy()
        df = ca_df['Volume'].copy()
        df.name = str(year)
        df.index = ca_df['ca_ba']

        # Create data frame of CA-US exports
        # NOTE: this data frame is not square.
        rf = ca_us.query("Date == %d" % year).copy()
        rf.index = rf['ca_ba'].copy()
        rf = rf.pivot(columns='us_ba', values='Volume')
        rf = rf.fillna(0)

    # NOTE: this could return an empty series if the year is not found!
    if len(df) == 0:
        logging.warning("Failed to find %d in Canadian export data!" % year)

    return (df, rf)


def _read_eia_gen(year):
    """Create data frame of EIA generation data for balancing authorities.

    Data are from EIA Forms 923 and 860.

    Parameters
    ----------
    year : int
        Data year (e.g., 2016)

    Returns
    -------
    tuple
        A tuple of length two.

        - pandas.DataFrame : EIA generation data with two columns:
          "Balancing Authority Code" and "Electricity".
        - list : EIA 860 balancing authority abbreviation codes
    """
    eia923_gen = eia923.build_generation_data(generation_years=[year])
    eia860_df = eia860.eia860_balancing_authority(year)
    eia860_df["Plant Id"] = eia860_df["Plant Id"].astype(int)
    eia860_ba_list = list(
        eia860_df["Balancing Authority Code"].dropna().unique())
    eia_combined_df = eia923_gen.merge(
        eia860_df,
        left_on=["FacilityID"],
        right_on=["Plant Id"],
        how="left"
    )
    eia_gen_ba = eia_combined_df.groupby(
        by=["Balancing Authority Code"],
        as_index=False
    )["Electricity"].sum()

    return (eia_gen_ba, eia860_ba_list)


def _make_ferc_trade(df, import_list):
    """Calculate the trades between balancing authority exporters and
    FERC region importers.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with exporting and importing balancing authority regions
        and their trade values along with their associated FERC region names
        and abbreviations.
    import_list : list
        A list of FERC region abbreviations used to filter the return data
        frame's importers.

    Returns
    -------
    pandas.DataFrame
        Fractions of the trade between exporting balancing authorities and
        importing FERC regions. Columns include:

        -   'import ferc region abbr' (str)
        -   'import ferc region' (str)
        -   'export BAA' (str)
        -   'fraction' (float)
    """
    # Calculate the total trade per import FERC region.
    ferc_import_grouped_tot = df.groupby(
        ['import ferc region'])['value'].sum().reset_index()

    ferc_trade = df.copy()
    ferc_trade = ferc_trade.groupby([
        'import ferc region abbr',
        'import ferc region',
        'export BAA'])['value'].sum().reset_index()
    ferc_trade = ferc_trade.merge(
        ferc_import_grouped_tot,
        left_on='import ferc region',
        right_on='import ferc region'
    )
    ferc_trade = ferc_trade.rename(
        columns={
            'value_x': 'value',
            'value_y':'total'}
    )
    ferc_trade['fraction'] = ferc_trade['value']/ferc_trade['total']
    ferc_trade = ferc_trade.fillna(value=0)
    ferc_trade = ferc_trade.drop(columns=['value', 'total'])

    # Remove Canadian entry in import list
    ferc_list = [x for x in import_list if x != 'CAN']

    ferc_filt = ferc_trade['import ferc region abbr'].isin(ferc_list)
    ferc_trade = ferc_trade[ferc_filt].copy()

    return ferc_trade


def _make_net_gen(year, ba_cols, ng_json_list):
    """Convert EIA bulk net generation data into time series data frame.

    Parameters
    ----------
    year : int
        Data year (e.g., 2016)
    ba_cols : list
        A list of balancing authority abbreviation codes.
    ng_json_list : list
        A list of JSON dictionaries for net generation from EIA's bulk data
        download.

    Returns
    -------
    pandas.DataFrame
        A data frame of net generation values with columns for each balancing
        authority code and a row index of hourly time stamps associated with
        the given year.

    Examples
    --------
    >>> df_BA_NA, ba_cols, ferc_list = _read_ba()
    >>> NET_GEN_ROWS, BA_TO_BA_ROWS, DEMAND_ROWS = _read_bulk()
    >>> df_net_gen = _make_net_gen(2016, ba_cols, NET_GEN_ROWS)
    >>> df_net_gen.head()
    region                       AEC  ...    WACM    WALC    WWA    YAD
    datetime                          ...
    2016-01-01 00:00:00+00:00  742.0  ...  5003.0   913.0   167.0  171.0
    2016-01-01 01:00:00+00:00  691.0  ...  5481.0  1136.0   184.0  169.0
    2016-01-01 02:00:00+00:00  630.0  ...  5531.0  1235.0   182.0  170.0
    2016-01-01 03:00:00+00:00  575.0  ...  5551.0  1081.0   165.0  170.0
    2016-01-01 04:00:00+00:00  586.0  ...  5394.0  1055.0   160.0  171.0
    """
    # Net Generation Data Import
    logging.info("Creating net generation data frame with datetime")
    df_net_gen = row_to_df(ng_json_list, 'net_gen')

    logging.info("Pivoting")
    df_net_gen = df_net_gen.pivot(
        index='datetime',
        columns='region',
        values='net_gen'
    )
    gen_cols = list(df_net_gen.columns.values)

    gen_cols_set = set(gen_cols)
    ba_ref_set = set(ba_cols)

    col_diff = list(ba_ref_set - gen_cols_set)
    col_diff.sort(key=str.upper)

    # Add in missing columns, then sort in alphabetical order
    logging.info("Cleaning net_gen data frame")
    for i in col_diff:
        df_net_gen[i] = 0

    # Keep only the columns that match the balancing authority names;
    # there are several other columns included in the dataset
    # that represent states (e.g., TEX, NY, FL) and other areas (US48)
    df_net_gen = df_net_gen[ba_cols]

    # Convert columns made of strings to numeric.
    cols_to_change = df_net_gen.columns[df_net_gen.dtypes.eq('object')]
    df_net_gen[cols_to_change] = df_net_gen[cols_to_change].apply(
        pd.to_numeric, errors="coerce")

    # Re-sort columns so the headers are in alpha order
    df_net_gen = df_net_gen.sort_index(axis=1)
    df_net_gen = df_net_gen.fillna(value=0)

    # Filter for the year of interest (NOTE: UTC dates)
    df_net_gen = df_net_gen.loc[df_net_gen.index.year==year]

    return df_net_gen


def _make_net_gen_sum(net_trade, eia_gen, ca_gen):
    """Combine net trades with annual CA exports and EIA net generation.

    Parameters
    ----------
    net_trade : pandas.DataFrame
        Hourly generation trade table for each US balancing authority, see :func:`_make_net_gen`.
    eia_gen : pandas.DataFrame
        Net annual generation for each US balancing authority.
    ca_gen : pandas.Series
        Net annual electricity exports from Canadian balancing authorities, see :func:`_read_ca_imports`.

    Returns
    -------
    pandas.DataFrame
        Net annual generation amounts for both US and Canada balancing
        authorities. Corrected using EIA 923 generation data. Assumes
        Canadian exports to US are equal to generation amounts.
    """
    # Sum values in each column
    # Creates a data frame with one column, rows are BA codes, values are
    # annual sums of net generation.
    df_net_gen_sum = net_trade.sum(axis=0).to_frame()

    # Add Canadian import data to the net generation dataset,
    # concatenate, convert to data frame, and put in alpha order
    logging.info("Combining US and Canadian net gen data")
    df_net_gen_sum = pd.concat([df_net_gen_sum, ca_gen]).sum(axis=1)
    df_net_gen_sum = df_net_gen_sum.to_frame()
    df_net_gen_sum = df_net_gen_sum.sort_index(axis=0)

    # Merge with EIA generation. Now, for each BA there are two columns:
    #    0: bulk net trade generation + Canadian export data
    #    'Electricity': EIA 923 generation data
    logging.info("Checking against EIA 923 generation data")
    net_gen_check = df_net_gen_sum.merge(
        right=eia_gen,
        left_index=True,
        right_on=["Balancing Authority Code"],
        how="left"
    ).reset_index()

    # HOTFIX: Zero-fill any mis-matches in the merge.
    net_gen_check = net_gen_check.fillna({0: 0, 'Electricity': 0})

    # Calculate percent difference between EIA generation and net trades.
    # HOTFIX: add a tiny bit to denom.; avoid zero division [2024-03-14; TWD]
    # Previously, diff_mad was calculated as inf and was useless.
    net_gen_check["diff"] = abs(
        net_gen_check["Electricity"] - net_gen_check[0]) / (
            1e-9 + net_gen_check[0]
        )
    # Calculated the mean absolute difference (MAD)
    # HOTFIX: no more mad() [2023-11-14; TWD]
    # https://github.com/pandas-dev/pandas/blob/2cb96529396d93b46abab7bbc73a208e708c642e/pandas/core/generic.py#L10817
    # NOTE: susceptible to outliers (e.g., GRIS BA)
    diff_mad = net_gen_check["diff"] - net_gen_check["diff"].mean()
    diff_mad = abs(diff_mad).mean()

    # Find the balancing authorities where the EIA generation is vastly
    # different from the net trades, using MAD as the cutoff criteria.
    net_gen_swap = net_gen_check.loc[
        net_gen_check["diff"] > diff_mad,
        ["Balancing Authority Code", "Electricity"]
    ].set_index("Balancing Authority Code")

    # Swap trades with EIA generation, when generation is vastly larger.
    df_net_gen_sum.loc[net_gen_swap.index, [0]] = np.nan
    net_gen_swap.rename(columns={"Electricity": 0}, inplace=True)
    df_net_gen_sum = df_net_gen_sum.combine_first(net_gen_swap)

    return df_net_gen_sum


def _make_square_pivot(df, names):
    """Make a pivot table square using the list of expected row/col names.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame meant to be a square matrix with the same names for
        rows (index) and columns.
    names : list
        List of row (index) and column names.

    Returns
    -------
    pandas.DataFrame
        The same data frame sent, but with extra rows and columns added
        to match the list of names provided.
    """
    # Find missing rows/cols - need to add them so we have a square matrix.
    trade_cols = list(df.columns.values)
    trade_rows = list(df.index.values)

    trade_cols_set = set(trade_cols)
    trade_rows_set = set(trade_rows)
    trade_ba_ref_set = set(names)

    # To add
    trade_col_diff = list(trade_ba_ref_set - trade_cols_set)
    trade_col_diff.sort(key=str.upper)

    trade_row_diff = list(trade_ba_ref_set - trade_rows_set)
    trade_row_diff.sort(key=str.upper)

    # Add in missing columns
    for i in trade_col_diff:
        logging.debug("Adding column, '%s'" % i)
        df[i] = 0

    # Add in missing rows
    for i in trade_row_diff:
        logging.debug("Adding row, '%s'" % i)
        df.loc[i,:] = 0

    # To remove
    trade_col_diff = list(trade_cols_set - trade_ba_ref_set)
    trade_col_diff.sort(key=str.upper)

    trade_row_diff = list(trade_rows_set - trade_ba_ref_set)
    trade_row_diff.sort(key=str.upper)

    # Remove untracked cols and rows
    for i in trade_col_diff:
        logging.debug("Removing column, '%s'" % i)
        df = df.drop(i, axis=1)

    for i in trade_row_diff:
        logging.debug("Removing row, '%s'" % i)
        df = df.drop(i, axis=0)

    # Sort alphabetically.
    # You should now square matrix with US BA codes as indexes and column names
    df = df.sort_index(axis=1)
    df = df.sort_index(axis=0)

    return df


def _make_trade_pivot(year, ba_cols, trade_df):
    """Create pivot table with rows representing exporting balancing
    authorities and columns representing importing balancing authorities.

    This function does the following:

    1.  Reformat the data to an annual basis.
    2.  Format the BA names in the corresponding columns.
    3.  Evaluate the trade values from both BA perspectives
        (e.g. BA1 as exporter and importer in a transaction with BA2).
    4.  Evaluate the trading data for any results that don't make sense

        -   both BAs designate as importers (negative value)
        -   both BAs designate as exporters (positive value)
        -   one of the BAs in the transaction reports a zero value and the
            other is nonzero

    5.  Calculate the percent difference in the transaction values reports
        by BAs.
    6.  Final exchange value based on the following logic:

        -   if percent diff is less than 20%, take mean,
        -   if not, use the value as reported by the exporting BAA
        -   designate each BA in the transaction either as the importer or
            exporter

    Parameters
    ----------
    year : int
        The trade year (e.g., 2020).
    ba_cols : list
        A list of balancing authority codes, see :func:`_read_ba_codes`,
        used to filter trade regions.
    trade_df : pandas.DataFrame
        A data frame of BA-to-BA trades with columns

        - 'from_region' (str)
        - 'to_region' (str)
        - 'datetime' (numpy.datetime64)
        - 'ba_to_ba' (float)

    Returns
    -------
    pandas.DataFrame
        A a pivot with index ('Exporting_BAA') representing exporting BAs and
        columns ('Importing_BAA') representing importing BAs, and values for
        the traded amount.
    """
    logging.info("Creating trading data frame")
    ba_trade = trade_df.set_index('datetime')
    ba_trade['transacting regions'] = (
        ba_trade['from_region'] + '-' + ba_trade['to_region'])

    # Keep only the columns that match the balancing authority names, there are
    # several other columns included in the dataset that represent states
    # (e.g., TEX, NY, FL) and other areas (US48)
    logging.info("Filtering trading data frame")
    filt1 = ba_trade['from_region'].isin(ba_cols)
    filt2 = ba_trade['to_region'].isin(ba_cols)
    filt = filt1 & filt2
    ba_trade = ba_trade[filt]

    # Subset for eia_gen_year, need to pivot first because of non-unique
    # datetime index.
    df_ba_trade_pivot = ba_trade.pivot(
        columns='transacting regions', values='ba_to_ba'
    )
    # Filter for year of interest (NOTE: UTC timestamps)
    df_ba_trade_pivot = df_ba_trade_pivot.loc[
        df_ba_trade_pivot.index.year==year]

    cols_to_change = df_ba_trade_pivot.columns[
        df_ba_trade_pivot.dtypes.eq('object')]
    df_ba_trade_pivot[cols_to_change] = df_ba_trade_pivot[
        cols_to_change].apply(pd.to_numeric, errors="coerce")

    # Sum columns - represents the net transacted amount between the two BAs
    df_ba_trade_sum = df_ba_trade_pivot.sum(axis=0).to_frame()
    df_ba_trade_sum = df_ba_trade_sum.reset_index()
    df_ba_trade_sum.columns = ['BAAs','Exchange']

    # Split BAA string into exporting and importing BAA columns
    # HOTFIX: 'maxsplit' is now 'n' in string split [2023-11-17; TWD]
    # Reference: https://www.statology.org/pandas-split-column/
    df_ba_trade_sum[['BAA1', 'BAA2']] = df_ba_trade_sum['BAAs'].str.split(
        '-', n=1, expand=True)

    df_ba_trade_sum = df_ba_trade_sum.rename(
        columns={'BAAs': 'Transacting BAAs'}
    )

    # Create two perspectives - import and export to use for comparison in
    # selection of the final exchange value between the BAAs
    df_trade_sum_1_2 = df_ba_trade_sum.groupby(
        ['BAA1', 'BAA2','Transacting BAAs'], as_index=False
    )[['Exchange']].sum()
    df_trade_sum_2_1 = df_ba_trade_sum.groupby(
        ['BAA2', 'BAA1', 'Transacting BAAs'], as_index=False
    )[['Exchange']].sum()

    df_trade_sum_1_2.columns = [
        'BAA1_1_2', 'BAA2_1_2','Transacting BAAs_1_2', 'Exchange_1_2']
    df_trade_sum_2_1.columns = [
        'BAA2_2_1', 'BAA1_2_1','Transacting BAAs_2_1', 'Exchange_2_1']

    # Combine two grouped tables for comparison for exchange values
    df_concat_trade = pd.concat([df_trade_sum_1_2, df_trade_sum_2_1], axis=1)
    df_concat_trade['Exchange_1_2_abs'] = df_concat_trade['Exchange_1_2'].abs()
    df_concat_trade['Exchange_2_1_abs'] = df_concat_trade['Exchange_2_1'].abs()

    # Create new column to check if BAAs designate as either both exporters or
    # both importers or if one of the entities in the transaction reports a
    # zero value. Drop combinations where any of these conditions are true,
    # keep everything else.
    df_concat_trade['Status_Check'] = np.where(
        (
            (df_concat_trade['Exchange_1_2'] > 0)
            & (df_concat_trade['Exchange_2_1'] > 0)
        ) | (
            (df_concat_trade['Exchange_1_2'] < 0)
            & (df_concat_trade['Exchange_2_1'] < 0)
        ) | (
            (df_concat_trade['Exchange_1_2'] == 0)
            | (df_concat_trade['Exchange_2_1'] == 0)
        ),
        'drop',
        'keep'
    )

    # Calculate the difference in exchange values
    df_concat_trade['Delta'] = (
        df_concat_trade['Exchange_1_2_abs']
        - df_concat_trade['Exchange_2_1_abs']
    )

    # Calculate percent diff of exchange_abs values.
    # This can be down two ways:
    # relative to 1_2 exchange or relative to 2_1 exchange.
    # Perform the calc both ways and take the average.
    df_concat_trade['Percent_Diff_Avg'] = 0.5*(
        abs(
            (
                df_concat_trade['Exchange_1_2_abs']
                / df_concat_trade['Exchange_2_1_abs']
            ) - 1
        ) + abs(
            (
                df_concat_trade['Exchange_2_1_abs']
                / df_concat_trade['Exchange_1_2_abs']
            ) - 1
        )
    )

    # Mean exchange value
    df_concat_trade['Exchange_mean'] = df_concat_trade[[
        'Exchange_1_2_abs', 'Exchange_2_1_abs']].mean(axis=1)

    # Percent diff equations creates NaN where both values are 0, fill with 0
    df_concat_trade['Percent_Diff_Avg'] = df_concat_trade[
        'Percent_Diff_Avg'].fillna(0)

    # Final exchange value based on logic;
    # if percent diff is less than 20%, take mean,
    # if not use the value as reported by the exporting BAA.
    # First figure out which BAA is the exporter by checking the value of the
    # Exchange_1_2. If that value is positive, it indicates that BAA1 is
    # exported to BAA2; if negative, use the value from Exchange_2_1.
    df_concat_trade['Final_Exchange'] = np.where(
        df_concat_trade['Percent_Diff_Avg'].abs() < 0.2,
        df_concat_trade['Exchange_mean'],
        np.where(
            df_concat_trade['Exchange_1_2'] > 0,
            df_concat_trade['Exchange_1_2'],
            df_concat_trade['Exchange_2_1']
        )
    )

    # Assign final designation of BAA as exporter or importer based on
    # logical assignment.
    df_concat_trade['Export_BAA'] = np.where(
        (df_concat_trade['Exchange_1_2'] > 0),
        df_concat_trade['BAA1_1_2'],
        np.where(
            (df_concat_trade['Exchange_1_2'] < 0),
            df_concat_trade['BAA2_1_2'],
            ''
            )
    )
    df_concat_trade['Import_BAA'] = np.where(
        (df_concat_trade['Exchange_1_2'] < 0),
        df_concat_trade['BAA1_1_2'],
        np.where(
            (df_concat_trade['Exchange_1_2'] > 0),
            df_concat_trade['BAA2_1_2'],
            ''
        )
    )
    df_concat_trade = df_concat_trade[df_concat_trade['Status_Check'] == 'keep']

    # Create the final trading matrix; first grab the necessary columns,
    # rename the columns and then pivot.
    df_concat_trade_subset = df_concat_trade[[
        'Export_BAA', 'Import_BAA', 'Final_Exchange']]

    df_concat_trade_subset.columns = [
        'Exporting_BAA', 'Importing_BAA', 'Amount']

    trade_pivot = df_concat_trade_subset.pivot_table(
        index='Exporting_BAA',
        columns='Importing_BAA',
        values='Amount').fillna(0)

    return trade_pivot


def _match_df_cols(base_df, return_df):
    """Helper method to return a data frame with column names matching a
    reference data frame (used for pandas.concat).

    Parameters
    ----------
    base_df : pandas.DataFrame
        A reference data frame with desired columns.
    return_df : pandas.DataFrame
        A data frame with a subset of the desired columns.

    Returns
    -------
    pandas.DataFrame
        The return data frame with padded columns (values set to zero).
    """
    to_cols = list(return_df.columns.values)
    from_cols = list(base_df.columns.values)

    to_set = set(to_cols)
    ref_set = set(from_cols)

    col_diff = list(ref_set - to_set)
    col_diff.sort(key=str.upper)

    # Add in missing columns,
    for i in col_diff:
        return_df[i] = 0

    # Sort in alphabetical order
    return_df = return_df.sort_index(axis=1)

    return (return_df)


def _make_ba_trade(trade_df, ba_list):
    """Calculate the trade fractions between exporting and importing balancing authorities.

    Parameters
    ----------
    trade_df : pandas.DataFrame
        The export/import trade data between balancing authorities. Should include columns: 'export BAA', 'import BAA', and 'value' (e.g., the output from :func:`qio_model`).
    ba_list : list
        A list of all relevant balancing authority codes (used as a filter).

    Returns
    -------
    pandas.DataFrame
        Columns include:

        -   'export BAA' (str: code)
        -   'import BAA' (str: code)
        -   'fraction' (float)
    """
    BAA_import_grouped_tot = trade_df.groupby(
        ['import BAA'])['value'].sum().reset_index()
    BAA_final_trade = trade_df.copy()
    BAA_final_trade = BAA_final_trade.drop(
        columns=[
            'import ferc region',
            'export ferc region',
            'import ferc region abbr',
            'export ferc region abbr']
    )
    BAA_final_trade = BAA_final_trade.merge(
        BAA_import_grouped_tot,
        left_on='import BAA',
        right_on='import BAA'
    )
    BAA_final_trade = BAA_final_trade.rename(
        columns={
            'value_x': 'value',
            'value_y': 'total'}
    )
    BAA_final_trade['fraction'] = (
        BAA_final_trade['value'] / BAA_final_trade['total'])
    BAA_final_trade = BAA_final_trade.fillna(value=0)
    BAA_final_trade = BAA_final_trade.drop(columns=['value', 'total'])

    # Remove Canadian BAs in import list
    # BUG: This doesn't do what the comment above suggests.
    #      ``ba_list`` includes Canadian balancing authority codes.
    BAA_filt = BAA_final_trade['import BAA'].isin(ba_list)
    BAA_final_trade = BAA_final_trade[BAA_filt].copy()

    return BAA_final_trade


def _make_us_trade(df):
    """Calculate the U.S. fractions of trade by exporting balancing authority.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with exporting and importing balancing authority regions
        and their trade values along with the associated FERC region name and
        abbreviation.

    Returns
    -------
    pandas.DataFrame
        Fractions of US trade by exporting balancing authority.
    """
    us_import_grouped_tot = df['value'].sum()
    us_trade = df.copy()
    us_trade = us_trade.groupby(['export BAA'])['value'].sum().reset_index()
    us_trade["fraction"] = us_trade["value"]/us_import_grouped_tot
    us_trade = us_trade.fillna(value=0)
    us_trade=us_trade.drop(columns=["value"])

    return us_trade


def _fix_final_trade(final_trade, z_traders, z_trade_w_demand, keep=False):
    """Set fraction amounts between balancing authorities that show no imports
    but show a demand to one and remove other zero importers with no demand
    (if keep is false). If keep is true, zero importers with zero demand are
    given a fraction near zero (i.e., 1e-9).

    Parameters
    ----------
    final_trade : pandas.DataFrame
        A data frame with export and import balancing authority codes and the
        fraction of their trade amount.
    z_traders : list
        A list of balancing authority codes with zero trade.
    z_trade_w_demand : list
        A list of balancing authority codes with zero trade, but positive
        demand.
    keep : bool, optional
        Whether to keep zero traders with no demand, by default False

    Returns
    -------
    pandas.DataFrame
        The same as `final_trade`, but with updated values and (optionally)
        filtered rows.
    """
    # HOTFIX: use 'loc' to set values against boolean lists [2023-11-17; TWD]
    for baa in z_trade_w_demand:
        final_trade.loc[
            (final_trade["import BAA"] == baa)
            & (final_trade["export BAA"] == baa),
            "fraction"
        ] = 1

    for baa in list(set(z_traders)-set(z_trade_w_demand)):
        if keep:
            # Set the value to something small to avoid zero errors.
            final_trade.loc[
                (final_trade["import BAA"] == baa)
                & (final_trade["export BAA"]==baa),
                "fraction"
            ] = 1E-15
        else:
            # It was decided to not create consumption mixes for BAs that
            # don't have imports. Remove BAA from import list.
            final_trade.drop(
                final_trade[final_trade["import BAA"] == baa].index,
                inplace=True
            )

    return final_trade


def _get_zero_traders(df):
    """Return a list of balancing authority codes associated with no trading.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with export and import balancing authority codes and the
        fraction of their trade amounts. Must include columns, 'import BAA'
        and 'fraction'.

    Returns
    -------
    list
        A list of balancing authority codes for importers with no trade.
    """
    r_list = [
        x for x in list(df["import BAA"].unique())
        if df.loc[df["import BAA"] == x, "fraction"].sum() == 0]
    return r_list


def _get_zero_traders_w_demand(z_traders, demand):
    """Return a list of balancing authority codes with positive demand.

    Parameters
    ----------
    z_traders : list
        A list of balancing authority codes associated with zero traders.
    demand : list
        A list of dictionaries. Each dictionary contains the hourly demand
        for a given region.

    Returns
    -------
    list
        A list of zero trader balancing authority codes that have positive
        demand.
    """
    r_list = []
    for d_row in demand:
        ba_code = d_row["series_id"].split('.')[1].split('-')[0]
        if ba_code in z_traders:
            r_list.append(ba_code)

    r_list = list(set(r_list))

    return r_list


def ba_io_trading_model(year=None, subregion=None, regions_to_keep=None):
    """Use EIA trading data to calculate the consumption mix for
    balancing authority area, FERC region, and U.S. national levels.

    Parameters
    ----------
    year : int, optional
        Specified year to pull transaction data between balancing authorities.
    subregion : str, optional
        Description of a group of regions. Options include 'FERC' for all FERC
        market regions, 'BA' for all balancing authorities.
    regions_to_keep : list, optional
        A list of balancing authority names of interest.
        Otherwise, returns all balancing authorities.

    Returns
    -------
    dict
        A dictionary of data frames. Each with import region, export region,
        transaction amount, total imports for import region, and fraction of
        total.

        The dictionary keys are the level of aggregation: "BA", "FERC", "US".

    Notes
    -----
    A candidate for parsing out the long list of methods into their own
    separate functions.

    Warning
    -------
    This method has a habit of requiring a lot of memory. A test run of
    ELCI_1 maxed at 11.1 GB during the call to :func:`ba_exchange_to_df`.
    If you are hitting Python segmentation faults, try restarting your
    computer and re-running the model with limited other applications running.
    You may just need more memory.

    Examples
    --------
    >>> d = ba_io_trading_model()  # uses modelconfig values
    >>> ferc_final_trade = d['FERC']
    >>> ferc_final_trade.head()
    import ferc region export ferc region         value         total  fraction
0              CAISO              CAISO  2.662827e+08  3.225829e+08  0.825471
1              CAISO             Canada  1.119572e+06  3.225829e+08  0.003471
2              CAISO              ERCOT  0.000000e+00  3.225829e+08  0.000000
3              CAISO             ISO-NE  0.000000e+00  3.225829e+08  0.000000
4              CAISO               MISO  0.000000e+00  3.225829e+08  0.000000
    """
    if year is None:
        year = model_specs.NETL_IO_trading_year

    if subregion is None:
        subregion = model_specs.regional_aggregation
    logging.info(
        "Using trade year %d and aggregation level '%s'" % (year, subregion))

    if subregion not in ['BA', 'FERC','US']:
        raise ValueError(
            'Subregion or regional_aggregation must have a value of "BA" '
            'or "FERC" when calculating trading with input-output, '
            f'not {subregion}'
        )

    # Import US and CA BA into single North America data frame.
    df_BA_NA, ba_cols, ferc_list = _read_ba()

    # Read necessary data from EIA's bulk data download.
    # WARNING: this is a lot of data in memory!
    NET_GEN_ROWS, BA_TO_BA_ROWS, DEMAND_ROWS = _read_bulk(ba_cols)

    # Net Generation Data Import
    df_net_gen = _make_net_gen(year, ba_cols, NET_GEN_ROWS)
    del(NET_GEN_ROWS)

    # Create EIA generation dataset and Form 860 balancing authority list.
    eia_gen_ba, eia860_ba_list = _read_eia_gen(year)

    # Read Canadian import data. Based on annual aggregated Canadian export
    # sales (in MWh) between Canadian and US balancing authorities.
    # https://www.cer-rec.gc.ca/en/data-analysis/energy-commodities/    \
    # electricity/statistics/electricity-trade-summary/
    # The first data frame has annual net trades from CA to US,
    # the second data frame has CA exports to US balancing authorities;
    # both are for this trade year.
    logging.info("Reading canadian import data")
    df_CA_Imports_Gen, df_CA_Imports_Rows = _read_ca_imports(year)

    # Combine and correct net generation data frame with Canada.
    df_net_gen_sum = _make_net_gen_sum(
        df_net_gen, eia_gen_ba, df_CA_Imports_Gen)

    # Group and resample trading data so that it is on an annual basis
    # WARNING: Peaks around 11 GB of memory
    logging.info("Creating trading data frame")
    df_ba_trade = ba_exchange_to_df(BA_TO_BA_ROWS, data_type='ba_to_ba')
    del(BA_TO_BA_ROWS)

    # Make export-import trade pivot table, make it square.
    df_trade_pivot = _make_trade_pivot(year, ba_cols, df_ba_trade)
    df_trade_pivot = _make_square_pivot(df_trade_pivot, ba_cols)

    # Add Canadian Imports to the trading matrix
    df_CA_Imports_Rows = _match_df_cols(df_trade_pivot, df_CA_Imports_Rows)
    df_concat_trade_CA = pd.concat([df_trade_pivot, df_CA_Imports_Rows])

    # Make it square. BUG: ELCI_1 has extra 'GRIS' column with no data.
    all_baa = list(df_concat_trade_CA.index.values)
    df_concat_trade_CA = _make_square_pivot(df_concat_trade_CA, all_baa)
    df_trade_pivot = df_concat_trade_CA

    # Create list of BA codes for EIA 860 data and run the QIO model.
    eia860_bas = sorted(eia860_ba_list + list(df_CA_Imports_Rows.index))

    df_final_trade_out_filt_melted_merge = qio_model(
        df_net_gen_sum,
        df_trade_pivot,
        df_BA_NA,
        eia860_bas,
        regions_to_keep,
        thresh=0.00001)

    # Develop final df for BAA
    BAA_final_trade = _make_ba_trade(
        df_final_trade_out_filt_melted_merge, eia860_bas)

    # There are some BAs that will have 0 trade. Some of these are legitimate.
    # Alcoa Yadkin has no demand (i.e., all power generation is exported)
    # others seem to be errors. For those BAs with actual demand, we'll set
    # the consumption mix to 100% from that BA. For those without demand,
    # fraction will be set to near 0 just to make sure systems can be built
    # in openLCA.

    # Find the zero traders.
    # TODO: combine w/ _fix_final_trade
    BAA_zero_trade = _get_zero_traders(BAA_final_trade)

    # Find zero traders w/ demand.
    # TODO: combine w/ _fix_final_trade
    BAAs_from_zero_trade_with_demand = _get_zero_traders_w_demand(
        BAA_zero_trade, DEMAND_ROWS)
    del(DEMAND_ROWS)

    # Set these regions' fractions to 1
    BAA_final_trade = _fix_final_trade(
        BAA_final_trade, BAA_zero_trade, BAAs_from_zero_trade_with_demand)

    # Write final trade table to CSV
    out_file = 'BAA_final_trade_{}.csv'.format(year)
    write_csv_to_output(out_file, BAA_final_trade)

    # Add balancing authority names to final trade data frame.
    BAA_final_trade["export_name"] = BAA_final_trade["export BAA"].map(
        df_BA_NA[["BA_Acronym", "BA_Name"]].set_index("BA_Acronym")["BA_Name"])
    BAA_final_trade["import_name"] = BAA_final_trade["import BAA"].map(
        df_BA_NA[["BA_Acronym", "BA_Name"]].set_index("BA_Acronym")["BA_Name"])

    # Calculate fractions of trade between BA and FERC regions.
    ferc_final_trade = _make_ferc_trade(
        df_final_trade_out_filt_melted_merge, ferc_list)

    out_file = 'ferc_final_trade_{}.csv'.format(year)
    write_csv_to_output(out_file, ferc_final_trade)

    # Add balancing authority name to export regions.
    ferc_final_trade["export_name"] = ferc_final_trade["export BAA"].map(
        df_BA_NA[["BA_Acronym", "BA_Name"]].set_index("BA_Acronym")["BA_Name"])

    # Calculate US trade fractions by exporting balancing authority.
    us_final_trade = _make_us_trade(df_final_trade_out_filt_melted_merge)

    # Add balancing authority name to export regions.
    us_final_trade["export_name"] = us_final_trade["export BAA"].map(
        df_BA_NA[["BA_Acronym", "BA_Name"]].set_index("BA_Acronym")["BA_Name"])

    return {
        'BA': BAA_final_trade,
        'FERC': ferc_final_trade,
        'US': us_final_trade}


def qio_model(net_gen_df, trade_pivot, ba_map, ba_list, roi=None, thresh=1e-5):
    """The quasi-input-output trading model.

    Written by G. Cooney. This method perform trading calculations as provided in Qu et al (2018) to determine the composition of a BA consumption mix with the following qualities:

    -   Input-output approach developed by Qu et al.
    -   Transfer is enabled through infinite electricity supply chains
    -   Virtual flows of emissions should follow the pattern of inter-grid
        electricity transfers.

    Parameters
    ----------
    net_gen_df : pandas.DataFrame
        A single-column data frame where row indices are the balancing authority codes and the values are the net generation amounts (MWh)
    trade_pivot : pandas.DataFrame
        A pivot table of trades (exported from row to column). The column names and row indices should match the indices of ``net_get_df``.
    ba_map : pandas.DataFrame
        A data frame mapping balancing authority codes to their names, EIA region, FERC region, and those regions' abbreviations.
    ba_list : list
        A list of balancing authority codes used as a filter for balancing authorities that are associated with EIA Form 860 plants.
    roi : list, optional
        A list of balancing authority names used to filter the rows in the return data frame, by default None
    thresh : float, optional
        A small floating point value used as a threshold to filter values in the final trading matrix, as there are lots of really small values as a result of the matrix calculate (e.g., 2.0e-15), by default 1e-5

    Returns
    -------
    pandas.DataFrame
        Columns include the following.

        - 'export BAA' (str): Balancing authority code of exporter
        - 'import BAA' (str): Balancing authority code of importer
        - 'value' (float): Electricity trade (MWh)
        - 'import ferc region' (str): FERC region name associated with import
        - 'import ferc region abbr' (str): FERC importer abbreviation
        - 'export ferc region' (str): FERC region name associated with export
        - 'export ferc region abbr' (str): FERC exporter abbreviation
    """
    # Create total inflow vector x and then convert to a diagonal matrix x-hat
    logging.info("Inflow vector")
    x = []
    for i in range(len(net_gen_df)):
        x.append(net_gen_df.iloc[i] + trade_pivot.sum(axis=0).iloc[i])
    x_np = np.array(x)

    # If values are zero, x_hat matrix will be singular,
    # set BAAs with 0 to small value (e.g., 1.0)
    df_x = pd.DataFrame(data=x_np, index=trade_pivot.index)
    df_x = df_x.rename(columns={0: 'inflow'})
    df_x.loc[df_x['inflow'] == 0] = 1

    x_np = df_x.values
    x_hat = np.diagflat(x_np)

    # Create consumption vector c and then convert to a diagonal matrix c-hat
    # Calculate c based on x and T
    logging.info("consumption vector")
    c = []
    for i in range(len(net_gen_df)):
        c.append(x[i] - trade_pivot.sum(axis=1).iloc[i])

    c_np = np.array(c)
    c_hat = np.diagflat(c_np)

    # Convert df_trade_pivot to matrix
    T = trade_pivot.values

    # Create matrix to split T into distinct interconnections -
    # i.e., prevent trading between eastern and western interconnects.
    # Connections between the western and eastern interconnects are through
    # SWPP and WAUE.
    logging.info("Matrix operations")
    interconnect = trade_pivot.copy()
    interconnect[:] = 1
    interconnect.loc['SWPP',['EPE', 'PNM', 'PSCO', 'WACM']] = 0
    interconnect.loc['WAUE',['WAUW', 'WACM']] = 0
    interconnect_mat = interconnect.values
    T_split = np.multiply(T, interconnect_mat)

    # Matrix trading math (see Qu et al. 2018 ES&T paper)
    x_hat_inv = np.linalg.inv(x_hat)
    B = np.matmul(T_split, x_hat_inv)
    I = np.identity(len(net_gen_df))
    diff_I_B = I - B
    G = np.linalg.inv(diff_I_B)
    H = np.matmul(G, c_hat, x_hat_inv)
    df_H = pd.DataFrame(H)

    # Convert H to pandas data frame, populate index and columns
    df_final_trade_out = df_H
    df_final_trade_out.columns = net_gen_df.index
    df_final_trade_out.index = net_gen_df.index

    # Develop trading input for the eLCI code.
    # Need to melt the data frame to end up with a three column
    # data frame: Repeat for both possible aggregation levels -
    #   BA and FERC market region.
    df_final_trade_out_filt = df_final_trade_out.copy()
    col_list = df_final_trade_out.columns.tolist()

    # Filter for balancing authorities that are not associated
    # with any specific plants in EIA860 - there won't be any data for them in
    # the emissions data frames.
    keep_rows = [x for x in df_final_trade_out_filt.index if x in ba_list]
    keep_cols = [x for x in df_final_trade_out_filt.columns if x in ba_list]
    df_final_trade_out_filt = df_final_trade_out_filt.loc[keep_rows, keep_cols]

    # Set their quantities to 0 so that the consumption mixes are made up
    # of the rest of the incoming balancing authority areas.
    col_list = df_final_trade_out_filt.columns.tolist()
    for i in col_list:
        df_final_trade_out_filt[i] = np.where(
            (
                df_final_trade_out_filt[i].abs()
                / df_final_trade_out_filt[i].sum() < thresh
            ),
            0,
            df_final_trade_out_filt[i].abs()
        )

    df_final_trade_out_filt = df_final_trade_out_filt.reset_index()
    df_final_trade_out_filt = df_final_trade_out_filt.rename(
        columns={'index': 'Source BAA'})

    df_final_trade_out_filt_melted = df_final_trade_out_filt.melt(
        id_vars='Source BAA',
        value_vars=col_list
    )
    df_final_trade_out_filt_melted = df_final_trade_out_filt_melted.rename(
        columns={
            'Source BAA': 'export BAA',
            'variable': 'import BAA'}
    )

    # IMPORTS
    # Merge to bring in import region name matched with BAA
    df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted.merge(
        ba_map,
        left_on='import BAA',
        right_on='BA_Acronym'
    )
    df_final_trade_out_filt_melted_merge.rename(
        columns={
            'FERC_Region': 'import ferc region',
            'FERC_Region_Abbr':'import ferc region abbr'},
        inplace=True
    )
    # HOTFIX: missing column name in drop column list [240812; TWD]
    d_cols = [
        'BA_Acronym',
        'BA_Name',
        'NCR ID#',
        'EIA_Region',
        'EIA_Region_Abbr',
        'Time Zone',
        'Retirement Date',
        'Generation Only BA',
        'Demand by BA Subregion',
        'Active BA',
        'U.S. BA',
        'Activation Date',
    ]
    d_col = [
        x for x in d_cols if x in df_final_trade_out_filt_melted_merge.columns]
    df_final_trade_out_filt_melted_merge.drop(
        columns=d_col,
        inplace=True
    )

    # EXPORTS
    # Merge to bring in export region name matched with BAA
    df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted_merge.merge(
        ba_map,
        left_on='export BAA',
        right_on='BA_Acronym'
    )

    # Region of interest filtering (on exporting BAAs).
    if roi is not None:
        df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted_merge.loc[
            df_final_trade_out_filt_melted_merge["BA_Name"].isin(roi), :].copy()

    df_final_trade_out_filt_melted_merge.rename(
        columns={
            'FERC_Region': 'export ferc region',
            'FERC_Region_Abbr': 'export ferc region abbr'},
        inplace=True
    )

    d_col = [
        x for x in d_cols if x in df_final_trade_out_filt_melted_merge.columns]
    df_final_trade_out_filt_melted_merge.drop(
        columns=d_col,
        inplace=True
    )

    return df_final_trade_out_filt_melted_merge


def olca_schema_consumption_mix(database, gen_dict, subregion="BA"):
    """Convert the consumption mix dataframe into a openLCA-schema compatible
    dictionary.

    Parameters
    ----------
    database : pandas.DataFrame
        A trade data frame generated by :func:`ba_io_trading_model`
    gen_dict : dictionary
        A dictionary of generation mix data already processed for JSON-LD.
    subregion : str, optional
        The aggregation level for trade data. Also the dictionary key
        associated with the given database.

    Returns
    -------
    dict
        A process dictionary with exchanges ready for openLCA.
    """
    consumption_mix_dict = {}
    if subregion == "FERC":
        aggregation_column = "import ferc region"
        region = list(pd.unique(database[aggregation_column]))
        export_column = 'export_name'
    elif subregion == "BA":
        aggregation_column = "import_name"  # "import BAA"
        region = list(pd.unique(database[aggregation_column]))
        export_column = "export_name"  # 'export BAA'
    elif subregion == "US":
        export_column = "export_name"
        region=["US"]

    for reg in region:
        if subregion =="US":
            database_reg = database
        else:
            database_reg = database.loc[database[aggregation_column] == reg, :]

        exchanges_list = []

        # Filter regions for positive trade.
        database_filt = database_reg['fraction'] > 0
        database_reg = database_reg[database_filt].copy()
        exchange(exchange_table_creation_ref(database_reg), exchanges_list)

        for export_region in list(database_reg[export_column].unique()):
            database_f1 = database_reg[
                database_reg[export_column] == export_region
            ]
            if database_f1.empty != True:
                ra = exchange_table_creation_input_con_mix(
                    database_f1, export_region
                )
                ra["quantitativeReference"] = False
                ra['amount'] = database_reg.loc[
                    database_reg[export_column] == export_region,
                    'fraction'].values[0]
                matching_dict = None
                for gen in gen_dict:
                    if (
                        gen_dict[gen]["name"]
                        == 'Electricity; at grid; generation mix - ' + export_region
                    ):
                        matching_dict = gen_dict[export_region]
                        break

                if matching_dict is None:
                    logging.warning(
                        f"Trouble matching dictionary for {export_region} "
                        f"- {reg}"
                    )
                else:
                    ra["provider"] = {
                        "name": matching_dict["name"],
                        "@id": matching_dict["uuid"],
                        "category": matching_dict["category"].split("/"),
                    }
                exchange(ra, exchanges_list)
        final = process_table_creation_con_mix(reg, exchanges_list)
        final["name"] = (
            f"Electricity; at grid; consumption mix - {reg} - {subregion}"
        )
        consumption_mix_dict[f"{reg} - {subregion}"] = final

    return consumption_mix_dict
