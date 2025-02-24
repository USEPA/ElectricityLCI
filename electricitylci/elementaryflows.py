#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# elementaryflows.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

import pandas as pd

import fedelemflowlist


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This script compares the names of the wastes (and other flows)
and replaces them with names in the Federal LCA Commons elementary flows list.
Types of flows and compartment information are also determined and indexed.

Last updated:
    2025-02-05
"""
__all__ = [
    "add_flow_direction",
    "compartment_to_flowtype",
    "correct_netl_flow_names",
    "map_compartment_to_flow_type",
    "map_emissions_to_fedelemflows",
    "map_renewable_heat_flows_to_fedelemflows",
    "mapping_to_fedelemflows",
]


##############################################################################
# GLOBALS
##############################################################################
mapping_to_fedelemflows = fedelemflowlist.get_flowmapping()
mapping_to_fedelemflows = mapping_to_fedelemflows[[
    "SourceListName",
    "SourceFlowName",
    "SourceFlowContext",
    "SourceUnit",
    "ConversionFactor",
    "TargetFlowName",
    "TargetFlowUUID",
    "TargetFlowContext",
    "TargetUnit",
]]
'''pandas.DataFrame : A map between emission/wastes to openLCA UUIDs.'''

# See
# http://greendelta.github.io/olca-schema/html/FlowType.html
compartment_to_flowtype = pd.DataFrame(
    columns=["Compartment", "FlowType"],
    data=[
        ["air", "ELEMENTARY_FLOW"],
        ["water", "ELEMENTARY_FLOW"],
        ["ground", "ELEMENTARY_FLOW"],
        ["input", "PRODUCT_FLOW"],
        ["output", "PRODUCT_FLOW"],
        ["waste", "WASTE_FLOW"],
        ["emission/air","ELEMENTARY_FLOW"],
        ["emission/water","ELEMENTARY_FLOW"],
        ["emission/ground","ELEMENTARY_FLOW"],
        ["soil","ELEMENTARY_FLOW"]
    ],
)
'''pandas.DataFrame : A map between compartments and valid olca flow types.'''


##############################################################################
# FUNCTIONS
##############################################################################
def correct_netl_flow_names(df, amount_col="FlowAmount"):
    """A helper method that replaces NETL air, water, and ground emissions
    with Federal Elementary Flow List equivalents based on a subset of
    flows defined in USEPA's eLCI mapping using the Python package
    `fedelemflowlist <https://github.com/USEPA/fedelemflowlist>`_

    Parameters
    ----------
    df : pandas.DataFrame
        A life cycle inventory data frame with columns, 'FlowName',
        'Compartment', 'Unit', and ``amount_col``.
    amount_col : str, optional
        The column title representing the flow amount, by default "FlowAmount"

    Returns
    -------
    pandas.DataFrame
        A new data frame with the same number of rows and columns as the
        sent data frame. Flow names, compartments, units, and flow amounts
        are updated based on emissions matches with the FEDEFL. All unmatched
        flows are returned 'as is'. If FlowUUID was not in the column list,
        it is created; otherwise, the matched UUIDs are updated.
    """
    # This data frame has about 4k source flow names and contexts associated
    # with NETL unit process models (e.g., petro, nuclear, coal).
    flow_mapping = fedelemflowlist.get_flowmapping('eLCI')

    # Matching occurs on name and compartment; help this along by lowering the
    # case (improves coal UP matches from 10% to 42%).
    df["FlowName_orig"] = df["FlowName"]
    df["Compartment_orig"] = df["Compartment"]
    df["FlowName"] = df["FlowName"].str.lower().str.rstrip()
    df["Compartment"] = df["Compartment"].str.lower().str.rstrip()

    flow_mapping['SourceFlowName'] = flow_mapping['SourceFlowName'].str.lower()
    flow_mapping['SourceFlowContext'] = flow_mapping["SourceFlowContext"].str.lower()

    # Some compartments in NETL UPs are complex (e.g., 'Emission to water/fresh
    # water'), but are listed simply in the FEDEFL eLCI mapper (e.g., 'emission/
    # water'). Improves coal mining UP matches from 42% to 62%.
    is_emission = df['input'] == False
    is_water = df['Compartment'].str.contains('water')
    is_air = df['Compartment'].str.contains('air')
    is_ground = df['Compartment'].str.contains('ground')

    df.loc[is_emission * is_water, 'Compartment'] = 'emission/water'
    df.loc[is_emission * is_air, 'Compartment'] = 'emission/air'
    df.loc[is_emission * is_ground, 'Compartment'] = 'emission/ground'

    # HOTFIX: Map against source units [250205; TWD]
    # For coal mining, reduces matches from >62% to <62% (about 2k less rows)
    logging.info("Mapping emissions to FEDEFL")
    mapped_df = pd.merge(
        df,
        flow_mapping,
        left_on=["FlowName", "Compartment", "Unit"],
        right_on=["SourceFlowName", "SourceFlowContext", "SourceUnit"],
        how="left",
    )

    # If TargetFlowName is present, there was a match.
    is_match = mapped_df["TargetFlowName"].notnull()
    logging.info("Correcting %d NETL flows" % is_match.sum())

    # Quality Check (coal_df)
    #   Check that target unit matches source unit.
    #   No! Hydrogen, Uranium, and Lead-210/kg have mis-matched units.
    #   Therefore, unit conversions are necessary.

    # Return flow names and compartments back to their original values.
    df["FlowName"] = df["FlowName_orig"]
    df["Compartment"] = df["Compartment_orig"]
    del df['FlowName_orig']      # use this syntax since you're editing
    del df['Compartment_orig']   # a reference object that isn't returned
    mapped_df['FlowName'] = mapped_df['FlowName_orig']
    mapped_df["Compartment"] = mapped_df["Compartment_orig"]
    mapped_df = mapped_df.drop(columns=['FlowName_orig', 'Compartment_orig'])

    # Replace FlowName, Unit, and Compartment with new names (where matched)
    mapped_df.loc[is_match, "FlowName"] = mapped_df.loc[
        is_match, "TargetFlowName"]
    mapped_df.loc[is_match, "Compartment"] = mapped_df.loc[
        is_match, "TargetFlowContext"]
    mapped_df.loc[is_match, "Unit"] = mapped_df.loc[is_match, "TargetUnit"]

    # Correct values using the conversion factor
    mapped_df.loc[is_match, amount_col] *= mapped_df.loc[
        is_match, 'ConversionFactor']

    if 'FlowUUID' in mapped_df.columns:
        # Update existing values with new UUIDs
        mapped_df.loc[is_match, 'FlowUUID'] = mapped_df.loc[
            is_match, 'TargetFlowUUID']
    else:
        # Set UUIDs to target values
        mapped_df = mapped_df.rename(columns={"TargetFlowUUID": "FlowUUID"})

    # Drop all unneeded cols
    drop_cols = [x for x in flow_mapping.columns if x in mapped_df.columns]
    mapped_df = mapped_df.drop(columns=drop_cols)

    return mapped_df


def map_emissions_to_fedelemflows(df, amount_col='FlowAmount'):
    """Map emissions from a given data frame to the correct flows in the
    federal flow list based on matching sources (e.g., national emissions
    inventory [NEI], flow name, and compartment (e.g., emissions/air).

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with 'FlowName', 'Compartment', 'Units', and
        ``amount_col`` columns.
    amount_col : str, optional
        The column title representing the flow amount, by default "FlowAmount"

    Returns
    -------
    pandas.DataFrame
        A new data frame with the same number of rows as the input data frame,
        the same columns as the input data frame, and with new columns:

        - FlowUUID (str), the UUID for mapped flows (NaN if unmapped)
        - SourceListName (str), the source for the mapped flows

    Notes
    -----
    Referenced in coal_upstream.py and generation.py modules.
    """
    logging.info("Mapping emissions to FEDEFL")

    # At the continued cost of the user's computing memory, let's work on a
    # copy of the global map to avoid damaging it elsewhere.
    flow_mapping = mapping_to_fedelemflows.drop_duplicates(
        subset=["SourceFlowName", "SourceFlowContext", "SourceUnit"]
    ).copy()

    # Matching occurs on name, compartment, and unit;
    # help this along by lowering the case of the first two.
    df["FlowName_orig"] = df["FlowName"]
    df["Compartment_orig"] = df["Compartment"]
    df["FlowName"] = df["FlowName"].str.lower().str.rstrip()
    df["Compartment"] = df["Compartment"].str.lower().str.rstrip()
    flow_mapping['SourceFlowName'] = flow_mapping['SourceFlowName'].str.lower()
    flow_mapping['SourceFlowContext'] = flow_mapping["SourceFlowContext"].str.lower()

    mapped_df = pd.merge(
        df,
        flow_mapping,
        left_on=["FlowName", "Compartment", "Unit"],
        right_on=["SourceFlowName", "SourceFlowContext", "SourceUnit"],
        how="left",
    )

    # If a NewName is present there was a match
    is_match = mapped_df["TargetFlowName"].notnull()
    logging.info("Matched %d flows to FEDEFL" % is_match.sum())

    # Return flow names and compartments back to their original values.
    df["FlowName"] = df["FlowName_orig"]
    df["Compartment"] = df["Compartment_orig"]
    del df['FlowName_orig']
    del df['Compartment_orig']
    mapped_df['FlowName'] = mapped_df['FlowName_orig']
    mapped_df["Compartment"] = mapped_df["Compartment_orig"]
    mapped_df = mapped_df.drop(columns=['FlowName_orig', 'Compartment_orig'])

    # Update FlowName, Compartment, and Unit with new values
    mapped_df.loc[is_match, "FlowName"] = mapped_df.loc[
        is_match, "TargetFlowName"]
    mapped_df.loc[is_match, "Compartment"] = mapped_df.loc[
        is_match, "TargetFlowContext"]
    mapped_df.loc[is_match, "Unit"] = mapped_df.loc[is_match, "TargetUnit"]

    # Correct values using the conversion factor
    mapped_df.loc[is_match, amount_col] *= mapped_df.loc[
        is_match, 'ConversionFactor']

    if 'FlowUUID' in mapped_df.columns:
        # Update existing values with new UUIDs
        mapped_df.loc[is_match, 'FlowUUID'] = mapped_df.loc[
            is_match, 'TargetFlowUUID']
    else:
        # Set UUIDs to target values
        mapped_df = mapped_df.rename(columns={"TargetFlowUUID": "FlowUUID"})

    # If air, soil, or water assigned it directionality of emission.
    # Others will get assigned later as needed.
    emission_compartments = [
        "emission/air",
        "emission/ground",
        "emission/water"
    ]
    mapped_df.loc[
        mapped_df["Compartment"].isin(emission_compartments),
        "ElementaryFlowPrimeContext",
    ] = "emission"

    # Drop all unneeded cols
    drop_cols = [
        x for x in flow_mapping.columns
            if x in mapped_df.columns and x != 'SourceListName']
    mapped_df = mapped_df.drop(columns=drop_cols)

    return mapped_df


def map_renewable_heat_flows_to_fedelemflows(df_with_flows_compart_direction):
    """Map inputs of 'Heat' to the various energy types for renewables."""
    # ! Still need to consider amount conversion
    # # For all other fuel sources assume techonosphere flows and set to null
    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat"),
        "ElementaryFlowPrimeContext",
    ] = None

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (
            (df_with_flows_compart_direction["FuelCategory"] == "SOLAR")
            | (df_with_flows_compart_direction["FuelCategory"] == "GEOTHERMAL")
            | (df_with_flows_compart_direction["FuelCategory"] == "WIND")
            | (df_with_flows_compart_direction["FuelCategory"] == "HYDRO")
        ),
        "ElementaryFlowPrimeContext",
    ] = "resource"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "SOLAR"),
        "Compartment",
    ] = "air"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "SOLAR"),
        "FlowName",
    ] = "Energy, solar"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "GEOTHERMAL"),
        "Compartment",
    ] = "ground"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "GEOTHERMAL"),
        "FlowName",
    ] = "Energy, geothermal"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "WIND"),
        "Compartment",
    ] = "air"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "WIND"),
        "FlowName",
    ] = "Energy, wind"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "HYDRO"),
        "Compartment",
    ] = "water"

    df_with_flows_compart_direction.loc[
        (df_with_flows_compart_direction["FlowName"] == "Heat")
        & (df_with_flows_compart_direction["FuelCategory"] == "HYDRO"),
        "FlowName",
    ] = "Energy, hydro"

    # TODO: Need to handle steam separately

    return df_with_flows_compart_direction


def map_compartment_to_flow_type(df_with_compartments):
    """Add new columns to a data frame that maps flows based on compartment."""
    df_with_flowtypes = pd.merge(
        df_with_compartments,
        compartment_to_flowtype,
        on=["Compartment"],
        how="left",
    )

    return df_with_flowtypes


def add_flow_direction(df_with_flowtypes):
    """Add 'FlowDirection' column indicating input/output flow direction."""
    df_with_flowtypes["FlowDirection"] = "output"
    df_with_flowtypes.loc[
        (df_with_flowtypes["Compartment"] == "input")
        | (df_with_flowtypes["ElementaryFlowPrimeContext"] == "resource"),
        "FlowDirection",
    ] = "input"

    return df_with_flowtypes
