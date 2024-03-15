#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# eia_io_trading.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
from datetime import datetime
import json
import logging
import os
import zipfile

import numpy as np
import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.globals import output_dir
from electricitylci.globals import paths
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
from electricitylci.process_dictionary_writer import (
    exchange,
    process_table_creation_con_mix,
    exchange_table_creation_input_con_mix,
    exchange_table_creation_ref_cons,
)


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module uses data from the EIA bulk data file to determine
how much electricity flows between either balancing authority areas (BAAs)
or Federal Energy Regulatory Commission (FERC) regions. These electricity
flows are then used to generate the consumption mix for a given region or
balancing authority area.

Last updated:
    2024-03-15
"""
__all__ = [
    "ba_io_trading_model",
    "olca_schema_consumption_mix",
]


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


def _read_bulk():
    """Read and parse EIA's U.S. Electric System Operating Data.

    Creates three lists of JSON-based dictionaries.
    Each dictionary contains metadata and a timeseries of data.
    Time series data appear to go back to 2015.

    Returns
    -------
    tuple
        A tuple of length three.

        - list : rows associated with net generation.
        - list : rows associated with BA-to-BA trade.
        - list : rows associated with demand.
    """
    REGION_ACRONYMS = [
        'TVA', 'MIDA', 'CAL', 'CAR', 'CENT', 'ERCO', 'FLA',
        'MIDW', 'ISNE', 'NYIS', 'NW', 'SE', 'SW',
    ]

    # Read in the bulk data
    path = os.path.join(paths.local_path, 'bulk_data', 'EBA.zip')
    NET_GEN_ROWS = []
    BA_TO_BA_ROWS = []
    DEMAND_ROWS = []

    # HOTFIX: Check file vintage [2024-03-12; TWD]
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
            # All but one BA is currently reporting net generation in UTC
            # and local time. For that one BA (GRMA) only UTC time is
            # reported - so only pulling that for now.
            if b'EBA.NG.H' in line and b'EBA.NG.HL' not in line:
                NET_GEN_ROWS.append(json.loads(line))
            # Similarly there are 5 interchanges that report interchange
            # in UTC but not in local time.
            elif b'EBA.ID.H' in line and b'EBA.ID.HL' not in line:
                exchange_line = json.loads(line)
                s_txt = exchange_line['series_id'].split('-')[0][4:]
                if s_txt not in REGION_ACRONYMS:
                    BA_TO_BA_ROWS.append(exchange_line)
            # Keeping these here just in case
            elif b'EBA.D.H' in line and b'EBA.D.HL' not in line:
                DEMAND_ROWS.append(json.loads(line))

    logging.debug(f"Net gen rows: {len(NET_GEN_ROWS)}")
    logging.debug(f"BA to BA rows:{len(BA_TO_BA_ROWS)}")
    logging.debug(f"Demand rows:{len(DEMAND_ROWS)}")

    return (NET_GEN_ROWS, BA_TO_BA_ROWS, DEMAND_ROWS)


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
    # Subset for specified eia_gen_year
    start_datetime = '{}-01-01 00:00:00+00:00'.format(year)
    end_datetime = '{}-12-31 23:00:00+00:00'.format(year)

    start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S%z')
    end_datetime = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S%z')

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
    col_diff.sort(key = str.upper)

    # Add in missing columns, then sort in alphabetical order
    logging.info("Cleaning net_gen dataframe")
    for i in col_diff:
        df_net_gen[i] = 0

    # Keep only the columns that match the balancing authority names,
    # there are several other columns included in the dataset
    # that represent states (e.g., TEX, NY, FL) and other areas (US48)
    df_net_gen = df_net_gen[ba_cols]

    # Re-sort columns so the headers are in alpha order
    df_net_gen = df_net_gen.sort_index(axis=1)
    df_net_gen = df_net_gen.fillna(value=0)
    df_net_gen = df_net_gen.loc[start_datetime:end_datetime]

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

    # Zero-fill any mis-matches in the merge.
    net_gen_check = net_gen_check.fillna({0: 0, 'Electricity': 0})

    # Calculate percent difference between EIA generation and net trades.
    # HOTFIX: add a tiny bit to denom.; avoid zero division [2024-03-14; TWD]
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

    trade_col_diff = list(trade_ba_ref_set - trade_cols_set)
    trade_col_diff.sort(key=str.upper)

    trade_row_diff = list(trade_ba_ref_set - trade_rows_set)
    trade_row_diff.sort(key=str.upper)

    # Add in missing columns, then sort in alphabetical order
    for i in trade_col_diff:
        df[i] = 0

    df = df.sort_index(axis=1)

    # Add in missing rows, then sort in alphabetical order
    for i in trade_row_diff:
        df.loc[i,:] = 0

    # Square matrix with US BA codes as indexes and column names
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
    # Subset for specified eia_gen_year
    start_datetime = '{}-01-01 00:00:00+00:00'.format(year)
    end_datetime = '{}-12-31 23:00:00+00:00'.format(year)

    start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S%z')
    end_datetime = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S%z')

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
    df_ba_trade_pivot = df_ba_trade_pivot.loc[start_datetime:end_datetime]

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
    df_concat_trade = pd.concat([df_trade_sum_1_2,df_trade_sum_2_1], axis=1)
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
    NET_GEN_ROWS, BA_TO_BA_ROWS, DEMAND_ROWS = _read_bulk()

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

    # Net Generation Data Import
    df_net_gen = _make_net_gen(year, ba_cols, NET_GEN_ROWS)
    del(NET_GEN_ROWS)

    # Combine and correct net generation data frame with Canada.
    df_net_gen_sum = _make_net_gen_sum(
        df_net_gen, eia_gen_ba, df_CA_Imports_Gen)

    # Group and resample trading data so that it is on an annual basis
    logging.info("Creating trading data frame")
    df_ba_trade = ba_exchange_to_df(BA_TO_BA_ROWS, data_type='ba_to_ba')
    del(BA_TO_BA_ROWS)

    # Make export-import trade pivot table, make it square.
    df_trade_pivot = _make_trade_pivot(year, ba_cols, df_ba_trade)
    df_trade_pivot = _make_square_pivot(df_trade_pivot, ba_cols)

    # Add Canadian Imports to the trading matrix
    df_CA_Imports_Rows = _match_df_cols(df_trade_pivot, df_CA_Imports_Rows)
    df_concat_trade_CA = pd.concat([df_trade_pivot, df_CA_Imports_Rows])

    # Make it square.
    all_baa = list(df_concat_trade_CA.index.values)
    df_concat_trade_CA = _make_square_pivot(df_concat_trade_CA, all_baa)
    df_trade_pivot = df_concat_trade_CA

    ## TODO PICK UP HERE.

    # Perform trading calculations as provided in Qu et al (2018) to
    # determine the composition of a BA consumption mix.

    # Create total inflow vector x and then convert to a diagonal matrix x-hat
    logging.info("Inflow vector")
    x = []
    for i in range (len(df_net_gen_sum)):
        x.append(df_net_gen_sum.iloc[i] + df_trade_pivot.sum(axis=0).iloc[i])
    x_np = np.array(x)

    # If values are zero, x_hat matrix will be singular,
    # set BAAs with 0 to small value (1)
    df_x = pd.DataFrame(data=x_np, index=df_trade_pivot.index)
    df_x = df_x.rename(columns={0: 'inflow'})
    df_x.loc[df_x['inflow'] == 0] = 1

    x_np = df_x.values
    x_hat = np.diagflat(x_np)

    # Create consumption vector c and then convert to a diagonal matrix c-hat
    # Calculate c based on x and T
    logging.info("consumption vector")
    c = []

    for i in range(len(df_net_gen_sum)):
        c.append(x[i] - df_trade_pivot.sum(axis = 1).iloc[i])

    c_np = np.array(c)
    c_hat = np.diagflat(c_np)

    # Convert df_trade_pivot to matrix
    T = df_trade_pivot.values

    # Create matrix to split T into distinct interconnections -
    # i.e., prevent trading between eastern and western interconnects.
    # Connections between the western and eastern interconnects are through
    # SWPP and WAUE.
    logging.info("Matrix operations")
    interconnect = df_trade_pivot.copy()
    interconnect[:] = 1
    interconnect.loc['SWPP',['EPE', 'PNM', 'PSCO', 'WACM']] = 0
    interconnect.loc['WAUE',['WAUW', 'WACM']] = 0
    interconnect_mat = interconnect.values
    T_split = np.multiply(T, interconnect_mat)

    # Matrix trading math (see Qu et al. 2018 ES&T paper)
    x_hat_inv = np.linalg.inv(x_hat)
    B = np.matmul(T_split, x_hat_inv)
    I = np.identity(len(df_net_gen_sum))
    diff_I_B = I - B
    G = np.linalg.inv(diff_I_B)
    H = np.matmul(G, c_hat, x_hat_inv)
    df_H = pd.DataFrame(H)

    # Convert H to pandas dataframe, populate index and columns
    df_final_trade_out = df_H
    df_final_trade_out.columns = df_net_gen_sum.index
    df_final_trade_out.index = df_net_gen_sum.index

    # Develop trading input for the eLCI code.
    # Need to melt the dataframe to end up with a three column
    # dataframe:Repeat for both possible aggregation levels -
    #   BA and FERC market region.

    # Establish a threshold of 0.00001 to be included in the final
    # trading matrix.
    # Lots of really small values as a result of the matrix calculate
    # (e.g., 2.0e-15)
    df_final_trade_out_filt = df_final_trade_out.copy()
    col_list = df_final_trade_out.columns.tolist()

    # Adding in a filter for balancing authorities that are not associated
    # with any specific plants in EIA860 - there won't be any data for them in
    # the emissions dataframes. We'll set their quantities to 0 so that the
    # consumption mixes are made up of the rest of the incoming balancing
    # authority areas.
    eia860_bas = sorted(
        eia860_ba_list + list(df_CA_Imports_Cols.columns)
    )
    keep_rows = [x for x in df_final_trade_out_filt.index if x in eia860_bas]
    keep_cols = [x for x in df_final_trade_out_filt.columns if x in eia860_bas]

    df_final_trade_out_filt = df_final_trade_out_filt.loc[keep_rows, keep_cols]
    col_list = df_final_trade_out_filt.columns.tolist()
    for i in col_list:
        df_final_trade_out_filt[i] = np.where(
            (
                df_final_trade_out_filt[i].abs()
                / df_final_trade_out_filt[i].sum() < 0.00001
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

    # Merge to bring in import region name matched with BAA
    df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted.merge(
        df_BA_NA,
        left_on='import BAA',
        right_on='BA_Acronym'
    )
    df_final_trade_out_filt_melted_merge.rename(
        columns={
            'FERC_Region': 'import ferc region',
            'FERC_Region_Abbr':'import ferc region abbr'},
        inplace=True
    )
    df_final_trade_out_filt_melted_merge.drop(
        columns=[
            'BA_Acronym',
            'BA_Name',
            'NCR ID#',
            'EIA_Region',
            'EIA_Region_Abbr'],
        inplace = True
    )

    # Merge to bring in export region name matched with BAA
    df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted_merge.merge(
        df_BA_NA,
        left_on='export BAA',
        right_on='BA_Acronym'
    )

    if regions_to_keep is not None:
        df_final_trade_out_filt_melted_merge = df_final_trade_out_filt_melted_merge.loc[
            df_final_trade_out_filt_melted_merge["BA_Name"].isin(regions_to_keep),
            :]
    df_final_trade_out_filt_melted_merge.rename(
        columns={
            'FERC_Region': 'export ferc region',
            'FERC_Region_Abbr':'export ferc region abbr'},
        inplace=True
    )
    df_final_trade_out_filt_melted_merge.drop(
        columns=[
            'BA_Acronym',
            'BA_Name',
            'NCR ID#',
            'EIA_Region',
            'EIA_Region_Abbr'],
        inplace=True
    )

    # Develop final df for BAA
    BAA_import_grouped_tot = df_final_trade_out_filt_melted_merge.groupby(
        ['import BAA'])['value'].sum().reset_index()
    BAA_final_trade = df_final_trade_out_filt_melted_merge.copy()
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
    BAA_filt = BAA_final_trade['import BAA'].isin(eia860_bas)
    BAA_final_trade = BAA_final_trade[BAA_filt]

    # There are some BAs that will have 0 trade. Some of these are legitimate
    # Alcoa Yadkin has no demand (i.e., all power generation is exported) others
    # seem to be errors. For those BAs with actual demand, we'll set the
    # consumption mix to 100% from that BA. For those without demand,
    # fraction will be set to near 0 just to make sure systems can be built
    # in openLCA
    BAA_zero_trade = [
        x for x in list(BAA_final_trade["import BAA"].unique())
        if (
            BAA_final_trade.loc[
                BAA_final_trade["import BAA"] == x, "fraction"].sum() == 0)]

    BAAs_from_zero_trade_with_demand = []
    for d_row in DEMAND_ROWS:
        if d_row["series_id"].split('.')[1].split('-')[0] in BAA_zero_trade:
            BAAs_from_zero_trade_with_demand.append(
                d_row["series_id"].split('.')[1].split('-')[0])

    BAAs_from_zero_trade_with_demand = list(
        set(BAAs_from_zero_trade_with_demand))
    del(DEMAND_ROWS)

    # HOTFIX: use 'loc' not 'at' for setting values against boolean lists
    # [2023-11-17; TWD]
    for baa in BAAs_from_zero_trade_with_demand:
        BAA_final_trade.loc[
            (
                (BAA_final_trade["import BAA"] == baa)
                & (BAA_final_trade["export BAA"] == baa)
            ), "fraction"
        ] = 1

    for baa in list(set(BAA_zero_trade)-set(BAAs_from_zero_trade_with_demand)):
        BAA_final_trade.loc[
            (
                (BAA_final_trade["import BAA"] == baa)
                & (BAA_final_trade["export BAA"]==baa)
            ), "fraction"
        ] = 1E-15
        # It was later decided to not create consumption mixes for BAs that
        # don't have imports.
        BAA_final_trade.drop(
            BAA_final_trade[BAA_final_trade["import BAA"] == baa].index,
            inplace=True
        )

    f_trade_file = os.path.join(
        output_dir, 'BAA_final_trade_{}.csv'.format(year))
    logging.info("Writing final trade data to file, %s" % f_trade_file)
    BAA_final_trade.to_csv(f_trade_file)

    BAA_final_trade["export_name"] = BAA_final_trade["export BAA"].map(
        df_BA_NA[["BA_Acronym","BA_Name"]].set_index("BA_Acronym")["BA_Name"])
    BAA_final_trade["import_name"] = BAA_final_trade["import BAA"].map(
        df_BA_NA[["BA_Acronym","BA_Name"]].set_index("BA_Acronym")["BA_Name"])

    ferc_import_grouped_tot = df_final_trade_out_filt_melted_merge.groupby(
        ['import ferc region'])['value'].sum().reset_index()

    # Develop final df for FERC Market Region
    ferc_final_trade = df_final_trade_out_filt_melted_merge.copy()
    ferc_final_trade = ferc_final_trade.groupby([
        'import ferc region abbr',
        'import ferc region',
        'export BAA'])['value'].sum().reset_index()
    ferc_final_trade = ferc_final_trade.merge(
        ferc_import_grouped_tot,
        left_on='import ferc region',
        right_on='import ferc region'
    )
    ferc_final_trade = ferc_final_trade.rename(
        columns={
            'value_x': 'value',
            'value_y':'total'}
    )
    ferc_final_trade['fraction'] = (
        ferc_final_trade['value']/ferc_final_trade['total'])
    ferc_final_trade = ferc_final_trade.fillna(value = 0)
    ferc_final_trade = ferc_final_trade.drop(columns = ['value', 'total'])

    # Remove Canadian entry in import list
    ferc_list.remove('CAN')
    ferc_filt = ferc_final_trade['import ferc region abbr'].isin(ferc_list)
    ferc_final_trade = ferc_final_trade[ferc_filt]

    f_trade_file = os.path.join(
        output_dir, 'ferc_final_trade_{}.csv'.format(year))
    logging.info("Writing FERC trade data to file, %s" % f_trade_file)
    ferc_final_trade.to_csv(f_trade_file)

    ferc_final_trade["export_name"] = ferc_final_trade["export BAA"].map(
        df_BA_NA[["BA_Acronym", "BA_Name"]].set_index("BA_Acronym")["BA_Name"])

    us_import_grouped_tot = df_final_trade_out_filt_melted_merge['value'].sum()
    us_final_trade = df_final_trade_out_filt_melted_merge.copy()
    us_final_trade = us_final_trade.groupby(
        ['export BAA'])['value'].sum().reset_index()
    us_final_trade["fraction"] = us_final_trade["value"] / us_import_grouped_tot
    us_final_trade = us_final_trade.fillna(value = 0)
    us_final_trade=us_final_trade.drop(columns = ["value"])
    us_final_trade["export_name"] = us_final_trade["export BAA"].map(
        df_BA_NA[["BA_Acronym", "BA_Name"]].set_index("BA_Acronym")["BA_Name"])

    return {
        'BA': BAA_final_trade,
        'FERC': ferc_final_trade,
        'US': us_final_trade}


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

        # TODO: pandas futurewarning
        #       Should this reference 'database' and not 'database_reg'?
        database_filt = database['fraction'] > 0
        database_reg = database_reg[database_filt]

        exchange(exchange_table_creation_ref_cons(database_reg), exchanges_list)

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
                    database_reg[export_column] == export_region,'fraction'
                    ].values[0]
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
