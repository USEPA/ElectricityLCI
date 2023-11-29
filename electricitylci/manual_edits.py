#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# manual_edits.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os

import yaml

from electricitylci.globals import data_dir


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module provides methods that read a YAML file with
reassignment or removal instructions for electricity generation inventories.
For details on the issues these manual edits fix, see the following issues
on GitHub:

-   https://github.com/USEPA/ElectricityLCI/issues/77
-   https://github.com/USEPA/ElectricityLCI/issues/121

Referenced by 'create_generation_process_df' in generation.py.

Last updated:
    2023-11-29
"""
__all__ = [
    "check_for_edits",
    "manual_edits",
    "reassign",
    "remove",
]


##############################################################################
# GLOBALS
##############################################################################
with open(os.path.join(data_dir, "manual_edits.yml"), 'r') as f:
    manual_edits = yaml.safe_load(f)


##############################################################################
# FUNCTIONS
##############################################################################
def check_for_edits(data, calling_module, calling_function):
    """Perform manual edits to a given data frame.

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame in which edits are to made.
    calling_module : str
        Module name (e.g., 'generation.py')
    calling_function : str
        Function name (e.g., 'create_generation_process_df')

    Returns
    -------
    pandas.DataFrame
        The same data frame sent with manual edits applied.
    """
    try:
        # Pull dictionary of edit entries from YAML.
        edits_to_make = manual_edits[calling_module][calling_function]

        # Iterate over each entry & check edit type (e.g., remove or reassign)
        for ed in edits_to_make.keys():
            logging.info(f"Edits found for {calling_module}.{calling_function}")
            if edits_to_make[ed]["edit_type"] == "reassign":
                data = reassign(data, edits_to_make[ed])
            elif edits_to_make[ed]["edit_type"] == "remove":
                data = remove(data, edits_to_make[ed])
            else:
                logging.warning("Edits found but no handler for function!")
    except KeyError:
        logging.info("No manual edits found")

    return data


def reassign(data, edit_dict):
    """Perform value reassignment in a given data frame based on filtering
    criteria.

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame with columns of data to be reassigned.
    edit_dict : dict
        A dictionary with 'filters' key with column:value pairs to be used
        for filtering rows, 'column_to_reassign' to identify the data frame
        column where reassignment should occur, 'incoming_value' to find the
        rows with the wrong value (to be replace), and 'outgoing_value' with
        the new value to replace the incoming value.

    Returns
    -------
    pandas.DataFrame
        The same data frame received with values matching the filter and
        reassignment criteria replaced.
    """
    try:
        if edit_dict["data_source"] == "yaml":
            logging.info("Re-assigning using data from yaml")

            col = edit_dict["column_to_reassign"]
            logging.debug(f"{col}")

            in_val = edit_dict["incoming_value"]
            logging.debug(f"{in_val}")

            out_val = edit_dict["outgoing_value"]
            logging.debug(f"{out_val}")

            # Find all rows associated with the incoming value in the
            # reassignment column.
            combined_filter = data[col]==in_val

            # Reduce to only those rows that match each additional filter:
            for filt in edit_dict["filters"].keys():
                logging.debug(
                    f"Filters for {filt} are {edit_dict['filters'][filt]}")
                combined_filter = (
                    combined_filter &
                    data[filt].isin(edit_dict["filters"][filt])
                )
            data.loc[combined_filter, col] = out_val
    except KeyError as ke:
        logging.warning("Problem found with manual edit - reassign")
        logging.warning("%s" % str(ke))

    return data


def remove(data, edit_dict):
    """Drop rows from a data frame based on filtering criteria.

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame from which rows are to be dropped.
    edit_dict : dict
        A dictionary including a 'filters' key with column:value pairs to be
        used to identify rows in the data frame to drop.

    Returns
    -------
    pandas.DataFrame
        The same data frame received, but with rows dropped based on the
        filter criteria.
    """
    try:
        if edit_dict["data_source"] == "yaml":
            # Find the rows associated with the filter keywords
            logging.info("Removing using data from yaml")
            combined_filter = None
            for filt in edit_dict["filters"].keys():
                logging.debug(
                    f"Filters for {filt} are {edit_dict['filters'][filt]}")
                if combined_filter is None:
                    combined_filter = data[filt].isin(
                        edit_dict["filters"][filt])
                else:
                    combined_filter = (
                        combined_filter &
                        data[filt].isin(edit_dict["filters"][filt])
                    )
            # Drop rows from dataset
            data = data.loc[~combined_filter, :]
    except KeyError as ke:
        logging.warning("Problem found with manual edit - remove")
        logging.warning("%s" % str(ke))

    return data
