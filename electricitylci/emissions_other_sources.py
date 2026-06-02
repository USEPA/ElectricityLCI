#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# emissions_other_sources.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

import pandas as pd


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module integrates emissions from data sources that were not
included in stewi. For now, this specifically means emissions from Air Markets
Program Data (AMPD).

Last updated:
    2026-03-24
"""
__all__ = [
    "integrate_replace_emissions",
]


##############################################################################
# FUNCTIONS
##############################################################################
def integrate_replace_emissions(new_emissions, stewi_emissions):
    """
    Replace and/or add emissions to those compiled in stewi. This is done by
    concatenating the two data frames and dropping duplicates (keep the new values).

    For reference, the following FlowNames are used in stewi and should be used
    in the new_emissions dataframe:

    - Sulfur dioxide
    - Carbon dioxide
    - Methane
    - Nitrogen oxides
    - Nitrous oxide

    Parameters
    ----------
    new_emissions : pandas.DataFrame
        Total annual emissions from a facility.
        Columns must include:

        - FlowAmount          float64
        - FlowName             object
        - DataReliability    float64
        - Source               object
        - Unit                 object
        - Year                  int64
        - eGRID_ID             object

    stewi_emissions : pandas.DataFrame
        Annual facility emissions that have been compiled in stewi.
        Columns include:

        - FRS_ID                int64
        - FacilityID           object
        - FlowAmount          float64
        - FlowName             object
        - DataReliability    float64
        - Source               object
        - Unit                 object
        - Year                  int64
        - eGRID_ID             object

    Returns
    -------
    pandas.DataFrame
        The new emissions data frame.
    """
    logging.info("Correcting StEWI emissions")
    required_cols = [
        'Compartment',
        'FlowAmount',
        'FlowName',
        'DataReliability',
        'Source',
        'Unit',
        'Year',
        'eGRID_ID'
    ]
    assert set(required_cols).issubset(set(new_emissions.columns))
    stewi_emissions["eGRID_ID"] = stewi_emissions["eGRID_ID"].astype(int)

    # NEI data sourced from StEWI has different capitalization than eGRID,
    # while these are handled in stewicombo, here this issue persists due to
    # mapping on flow name. Temporarily remap those names, but then
    # reverse for NEI flows only after dropping duplicates
    flow_list = [
        "Carbon Dioxide",
        "Nitrous Oxide",
        "Sulfur Dioxide",
        "Nitrogen Oxides",
        ]
    # HOTFIX: use a filter to match left and right sides [26.03.24; TWD]
    fl_filter = stewi_emissions['FlowName'].isin(flow_list)
    stewi_emissions.loc[fl_filter, 'FlowName'] = stewi_emissions.loc[
        fl_filter, 'FlowName'].str.capitalize()

    # Added line below because eGRID_ID got duplicated somewhere causing
    # error in concat
    dup_col_filter = stewi_emissions.columns.duplicated()
    num_dup_cols = dup_col_filter.sum()
    if num_dup_cols > 0:
        logging.warning(
            "Encountered %d duplicate columns in StEWI data; fixing." % (
                num_dup_cols)
        )
        stewi_emissions = stewi_emissions.loc[:, ~dup_col_filter].copy()
    updated_emissions = pd.concat([stewi_emissions, new_emissions])

    # Remove Year from this list, otherwise results in duplicate emissions
    # by facility if years don't match in specs
    subset_cols = [
        'Compartment',
        'FlowName',
        'Unit',
        'eGRID_ID'
    ]
    updated_emissions.drop_duplicates(
        subset=subset_cols, keep='last', inplace=True)
    updated_emissions.reset_index(drop=True, inplace=True)

    # NOTE: all flow mapping is done with lowercase flow names (see
    # elementaryflows.py); removing unnecessary retitling step. [26.03.24; TWD]

    drop_columns = [
        'operator_name',
        'net_generation_megawatthours',
        'Total Fuel Consumption (MMBtu)',
        'Net Efficiency',
        'Compartment_path',
    ]
    drop_columns = [
        c for c in drop_columns
        if c in updated_emissions.columns.values.tolist()
    ]
    updated_emissions.drop(columns=drop_columns, inplace=True)

    return updated_emissions
