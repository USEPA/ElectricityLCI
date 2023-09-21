#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# manual_edits.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import yaml
from electricitylci.globals import data_dir
import logging


##############################################################################
# FUNCTIONS
##############################################################################
module_logger = logging.getLogger("manual_edits.py")

with open(f"{data_dir}/manual_edits.yml", "r") as f:
    manual_edits = yaml.safe_load(f)

def reassign(data, edit_dict):
    try:
        if edit_dict["data_source"]=="yaml":
            logging.info("Re-assigning using data from yaml")
            # logging.info(f"{edit_dict}")
            col = edit_dict["column_to_reassign"]
            # logging.info(f"{col}")
            in_val = edit_dict["incoming_value"]
            # logging.info(f"{in_val}")
            out_val = edit_dict["outgoing_value"]
            # logging.info(f"{out_val}")
            combined_filter=data[col]==in_val
            # logging.info(f"{combined_filter}")
            for filt in edit_dict["filters"].keys():
                # logging.info(f"Filters are {edit_dict['filters'][filt]}")
                combined_filter=(
                    combined_filter &
                    data[filt].isin(edit_dict["filters"][filt])
                )
                # logging.info(f"{combined_filter}")
            data.loc[combined_filter,col]=out_val
        return data
    except KeyError:
        module_logger.warning("Problem found with manual edit - reassign")
        return data

def remove(data, edit_dict):
    try:
        if edit_dict["data_source"]=="yaml":
            logging.info("Removing using data from yaml")
            combined_filter=None
            # logging.info(f"{combined_filter}")
            for filt in edit_dict["filters"].keys():
                # logging.info(f"Filters are {edit_dict['filters'][filt]}")
                if combined_filter is None:
                    combined_filter=data[filt].isin(edit_dict["filters"][filt])
                else:
                    combined_filter=(
                        combined_filter &
                        data[filt].isin(edit_dict["filters"][filt])
                    )
                # logging.info(f"{combined_filter}")
            data = data.loc[~combined_filter,:]
        return data
    except KeyError:
        module_logger.warning("Problem found with manual edit - remove")
        return data

def check_for_edits(data, calling_module, calling_function):
    try:
        edits_to_make=manual_edits[calling_module][calling_function]
        for ed in edits_to_make.keys():
            module_logger.info(f"Edits found for {calling_module}.{calling_function}")
            if edits_to_make[ed]["edit_type"]=="reassign":
                data=reassign(data,edits_to_make[ed])
            elif edits_to_make[ed]["edit_type"]=="remove":
                data=remove(data,edits_to_make[ed])
            else:
                logging.info("Edit found but no handler for function")
        return data
    except KeyError:
        module_logger.info("No manual edits found")
        return data
    