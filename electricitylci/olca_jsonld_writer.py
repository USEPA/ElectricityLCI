#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# olca_jsonld_writer.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import datetime
import io
import json
import logging
import math
import os
import re
import uuid
from zipfile import ZipFile

from esupy.location import olca_location_meta
import fedelemflowlist
import olca_schema as o
import olca_schema.units as o_units
import olca_schema.zipio as zipio
import numpy as np
import pandas as pd
import pytz
import requests

from electricitylci.globals import COAL_BASIN_CODES
from electricitylci.globals import C2G_LCI_METHOD
from electricitylci.globals import G2G_LCI_METHOD
from electricitylci.globals import GH_URL
from electricitylci.globals import US_STATES
from electricitylci.globals import paths
from electricitylci.globals import elci_version as VERSION
from electricitylci.utils import check_output_dir
from electricitylci.utils import read_ba_codes


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module provides methods that support the writing of
olca-schema formatted dictionaries to JSON-LD project files for openLCA.

Unit groups and flow properties are based on the Federal Elementary Flow List
found on the LCA Commons (https://www.lcacommons.gov/lca-collaboration/). This
was chosen because the UUIDs are consistent with GreenDelta's openLCA schema
and provide the full metadata where olca-schema Python package provides only
Ref objects. This could be avoided if opting for 'Units and flow properties'
is ticked when creating a new openLCA database.

References:

    -   GreenDelta openLCA schema

        -   https://greendelta.github.io/olca-schema/
        -   https://github.com/GreenDelta/olca-schema

Changelog (since v2.0):

    -   [26.03.27] Skip existing product systems.
    -   [26.03.19] Fix location name and code finder & set IMP to GLO.
    -   [26.03.19] Add LCI_Method description correction to clean_json.
    -   [26.03.05] Add coal basin to location finder.
    -   [26.02.12] Check v3 & v4 UUIDs for locations.
    -   [25.12.19] New update providers helper function.
    -   [25.12.16] New build residual processes method.
    -   [25.06.11] New method for updating product system description text.

Last edited:
    2026-03-27
"""
__all__ = [
    "add_to_product_system_description",
    "build_product_systems",
    "build_residual_processes",
    "check_exchanges",
    "clean_json",
    "write",
]


##############################################################################
# FUNCTIONS
##############################################################################
def add_to_product_system_description(file_path, description_txt):
    """Add a string to each product system description in a JSON-LD.

    Parameters
    ----------
    file_path : str
        File path to an existing JSON-LD created by ElectricityLCI.
    description_txt : str
        The string of text to be appended to the product system description
        text.

    Examples
    --------
    >>> txt_to_add = (
    ...     "The background data used to generate this inventory "
    ...     "model may be accessed at https://doi.org/10.18141/2569193."
    ... )
    >>> json_file = "ELCI_2022_jsonld_20250528_200843.zip"
    >>> add_to_product_system_description(json_file, txt_to_add)
    """
    try:
        data = _read_jsonld(file_path, _root_entity_dict())
    except OSError:
        logging.warning("Failed to read JSON-LD file, %s" % file_path)
    else:
        num_ps = len (data['ProductSystem']['ids'])
        logging.info("Updating %d product system entity descriptions" % num_ps)

        for p in data["ProductSystem"]['objs']:
            if p.description:
                # Description has text; append to it
                p.description += " "
                p.description += description_txt
            else:
                # No description; set as new text
                p.description = description_txt

            # Update change date/time
            p.last_change = _current_time()

        # Overwrite
        _save_to_json(file_path, data)


def build_product_systems(file_path, elci_config, add_residuals=False):
    """Generates product systems for electricity at user consumption mixes.

    Parameters
    ----------
    file_path : str
        A file path to an existing JSON-LD file with process data saved.
    elci_config : str
        The model configuration used to make the inventory (e.g., "ELCI_1")
    add_residuals : bool, optional
        Whether to create consumption mix product systems for residual mix
        processes.

    Notes
    -----
    -   This method checks process exchange amounts for NaNs, which cause
        openLCA to crash.
    -   This method overwrites the existing JSON-LD with the new product
        systems.
    -   This method checks for existing product systems (based on the name
        of the reference process) and skips any if found.
    """
    try:
        # Read all JSON-LD data in order to overwrite.
        data = _read_jsonld(file_path, _root_entity_dict())
    except OSError:
        logging.warning("Failed to read JSON-LD file, %s" % file_path)
    else:
        check_exchanges(data['Process']['objs'])
        logging.info("Building product systems in JSON-LD")

    # Find all processes for 'at user' consumption mixes
    # NOTE: residual processes could also be converted to product systems.
    qs1 = "^Electricity; at user; consumption mix - (.*) - BA$"
    qs2 = "^Electricity; at user; consumption mix - (.*) - FERC$"
    qs3 = "^Electricity; at user; consumption mix - US - US$"

    q1 = re.compile(qs1)
    q2 = re.compile(qs2)
    q3 = re.compile(qs3)
    q_list = [q1, q2, q3]

    # Provide residual mix support
    if add_residuals:
        qr1 = qs1.replace("; consumption", "; residual consumption")
        qr2 = qs2.replace("; consumption", "; residual consumption")
        qr3 = qs3.replace("; consumption", "; residual consumption")

        q4 = re.compile(qr1)
        q5 = re.compile(qr2)
        q6 = re.compile(qr3)

        q_list += [q4, q5, q6]

    r = []
    for q in q_list:
        r += _match_process_names(data['Process']['objs'], q)

    logging.info("Identified %d processes slated for product systems" % len(r))

    # Create a common description text
    t_now = datetime.datetime.now()
    d_txt = (
        "This product system was created in openLCA "
        "by linking default providers. "
        "The processes were generated by ElectricityLCI "
        f"({GH_URL}) "
        f"version {VERSION} using "
        f"the {elci_config} configuration. "
        f"Created: {t_now.isoformat()}."
    )

    for pid in r:
        p_idx = data['Process']['ids'].index(pid)
        p_obj = data['Process']['objs'][p_idx]

        # Note product system may already exist; check and skip if found.
        q = re.compile(p_obj.name)
        ps_list = _match_process_names(data['ProductSystem']['objs'], q)
        if len(ps_list) == 1:
            logging.info(
                "Product system, '%s', exists! Skipping." % (p_obj.name)
            )
        else:
            logging.info("Creating product system, '%s'" % p_obj.name)
            ps_obj = _make_product_system(file_path, p_obj, d_txt)

            # Update master data dictionary
            data['ProductSystem']['objs'].append(ps_obj)
            data['ProductSystem']['ids'].append(ps_obj.id)
            logging.debug("Created %s" % ps_obj.name)

    # Overwrite JSON-LD
    _save_to_json(file_path, data)


# IN PROGRESS - needs tested
def build_residual_processes(json_path, rem_ref, rem_txt=""):
    """Post-processing step that creates residual mix processes for a given
    JSON-LD.

    Parameters
    ----------
    json_path : str
        File path to JSON-LD.
    rem_ref : str, pandas.DataFrame
        Either a file path to CSV file or a pandas data frame containing
        the balancing authority level residual mixes (e.g., as provided by
        :func:`get_rem` in residual_grid_mix.py).
    rem_txt : str, optional
        Additional residual process description text, by default ""

    Raises
    ------
    FileNotFoundError
        If a non-existent CSV file path was provided for ``rem_ref``.
    TypeError
        If ``rem_ref`` was not provided as a file path or data frame.

    Notes
    -----
    See reference in :func:`add_residual_mixes` in residual_grid_mix.py.
    """
    # 1. READ RESIDUAL MIX DATA
    # Two options supported here: send pandas DataFrame or CSV file path.
    if isinstance(rem_ref, pd.DataFrame):
        logging.info(
            "Building residual mix processes using provided data frame."
        )
        rem = rem_ref
    elif isinstance(rem_ref, str) and os.path.isfile(rem_ref):
        # NOTE: the CSV file may round data frame's machine precision
        # (e.g., 1.0499999999 -> 1.05).
        logging.info(
            "Building residual mix processes using provided CSV file."
        )
        rem = pd.read_csv(rem_ref)
    elif isinstance(rem_ref, str) and not os.path.isfile(rem_ref):
        raise FileNotFoundError(
            "Failed to find the residual mix CSV located at %s!" % rem_ref
        )
    else:
        raise TypeError(
            "Residual mixes may be passed as either a pandas DataFrame or "
            "CSV file path, not %s" % type(rem_ref)
        )

    # 2. READ JSON-LD DATA
    try:
        data = _read_jsonld(json_path, _root_entity_dict())
    except OSError:
        logging.warning("Failed to read JSON-LD file, %s" % json_path)
        raise

    # 3. CREATE RESIDUAL GENERATION MIX PROCESSES
    logging.info("Creating residual generation mix processes")

    # Initialize process mapping dictionary
    p_map = {}

    # Find the electricity generation processes in JSON-LD
    q = re.compile("^Electricity; at grid; generation mix - (.*)$")
    r = _match_process_names(data['Process']['objs'], q)
    logging.debug("Found %d generation mix processes." % len(r))

    for pid in r:
        p_idx = data['Process']['ids'].index(pid)
        p_obj = data['Process']['objs'][p_idx]
        ba = q.match(p_obj.name).group(1)
        rid, data = _make_rem_gen_process(p_obj.id, ba, data, rem_txt, rem)
        p_map[pid] = rid

    # 4. CREATE RESIDUAL CONSUMPTION MIX PROCESSES
    logging.info("Creating residual consumption mix processes")

    # Find the electricity consumption processes in JSON-LD
    q = re.compile(
        "^Electricity; at (?:user|grid); consumption mix - (.*) - (BA|FERC|US)$"
    )
    r = _match_process_names(data['Process']['objs'], q)
    logging.debug("Found %d consumption mix processes." % len(r))

    for pid in r:
        rid, data = _make_rem_con_process(pid, data, rem_txt)
        p_map[pid] = rid

    # 5. UPDATE CONSUMPTION MIX PROCESS PROVIDERS
    logging.info("Updating residual mix process default providers")
    for pid in r:
        # Find the residual process associated with this consumption process.
        rid = p_map[pid]
        r_idx = data['Process']['ids'].index(rid)
        r_obj = data['Process']['objs'][r_idx]  # the object being modified

        # The replacement step---
        #   Read through a process's exchanges (r_obj.exchanges).
        #   Examine any/all default providers.
        #   Find replacement UUID in ``p_map`` (note: may throw KeyError)
        #   Replace with Ref object (i.e., search for it in ``data``).
        r_obj = _update_providers(r_obj, p_map, data)

        # Update the master entity dictionary w/ updated process
        data['Process']['objs'][r_idx] = r_obj

    # 6. SAVE RESULTS
    _save_to_json(json_path, data)


def check_exchanges(p_list):
    """Iterate over process exchanges and log as error when an amount is nan.

    This occurrence causes openLCA to crash on upload of JSON-LD.
    It needs only to be run once; currently implemented in
    :func:`build_product_systems`, which is the last post-processes
    step.

    Parameters
    ----------
    p_list : list
        A list of olca-schema.Process objects.
    """
    logging.info("Checking process exchange amounts for NaNs")
    for p in p_list:
        for ex in p.exchanges:
            if ex.amount != ex.amount:
                e_str = "output"
                if ex.is_input:
                    e_str = "input"
                logging.error(
                    "Found nan for '%s' in %s exchange %d of process '%s'" % (
                        ex.flow.name,
                        e_str,
                        ex.internal_id,
                        p.name,
                ))


def clean_json(file_path):
    """Perform the following clean-up steps on JSON-LD.

    1.  Remove zero-valued product flows from processes.
    2.  Consecutively number exchange internal IDs.
    3.  Update flows with FEDEFL metadata.
    4.  Re-label heat inputs as elementary flow.
    5.  Fix compartment path 'Elementary Flows/Elementary Flows'
        associated with the resources 'Heat' and 'Water, reclaimed'
    6.  Fix compartment for two product flows: 'Light fuel oil' and
        'Ammonium nitrate' from the coal model.
    7.  Fix inventory method description for C2G and G2G processes.

    Parameters
    ----------
    file_path : str
        A file path to an existing JSON-LD zip archive.
    """
    try:
        data = _read_jsonld(file_path, _root_entity_dict())
    except OSError:
        logging.warning("Failed to read JSON-LD file, %s" % file_path)
    else:
        logging.info("Cleaning JSON-LD")

        # Pull flows from each process's exchange list; remove zero product
        # flows along the way.
        # https://github.com/NETL-RIC/ElectricityLCI/issues/217
        e_list = []
        for p in data["Process"]['objs']:
            # Issue #328; check if unit processes are C2G or G2G [260319;TWD]
            has_provider = False

            for e in p.exchanges:
                # New tracker for default provider existence [26.03.19;TWD]
                if e.default_provider:
                    has_provider = True

                # Get the flow object
                fid = data["Flow"]['ids'].index(e.flow.id)
                f_obj = data["Flow"]['objs'][fid]

                # Remove if flow is a product flow with zero exchange value
                # NOTE: don't add as an exchange flow!
                if e.amount == 0 and (
                        f_obj.flow_type != o.FlowType.ELEMENTARY_FLOW):
                    logging.debug(
                        "Removing zero product flow, %s, from %s" % (
                            e.flow.name, p.name))
                    p.exchanges.remove(e)
                else:
                    # Add to list of tracked exchanges
                    e_list.append(e.flow.id)

                # Check if output exchange is labeled as a resource flow
                # https://github.com/NETL-RIC/ElectricityLCI/issues/233
                if not e.is_input and 'resource' in f_obj.category.lower():
                    logging.warning(
                        "Fixing resource flow in output exchange! "
                        "'%s' in %s (%s)" % (f_obj.name, p.name, p.id))
                    # HOTFIX: remove troublesome exchange, fix meta & re-add:
                    p.exchanges.remove(e)
                    e.is_input = True
                    e.description = "mislabeled resources"
                    p.exchanges.append(e)

                # Correct heat resource flows;
                # https://github.com/NETL-RIC/ElectricityLCI/issues/293
                if e.is_input and e.flow.name == 'Heat':
                    # The new FEDEFL heat resource flow
                    h_flow = _heat_elem_flow()
                    # Add elementary flow if missing
                    if h_flow.id not in data['Flow']['ids']:
                        data['Flow']['ids'].append(h_flow.id)
                        data['Flow']['objs'].append(h_flow)
                    # Add new heat to tracked list (if not already)
                    if h_flow.id not in e_list:
                        e_list.append(h_flow.id)
                    # Remove the defunct heat flow from tracked list
                    if e.flow.id in e_list:
                        e_fid = e_list.index(e.flow.id)
                        e_list.pop(e_fid)
                    # Remove old exchange, add new with updated description.
                    p.exchanges.remove(e)
                    e.flow = h_flow.to_ref()
                    if e.description:
                        e.description = "mapped to FEDEFL; " + e.description
                    else:
                        e.description = "mapped to FEDEFL"
                    p.exchanges.append(e)

                # Correct double Elementary Flows category
                # https://github.com/NETL-RIC/ElectricityLCI/issues/149
                if f_obj.category.startswith(
                        "Elementary flows/Elementary Flows"):
                    logging.warning(
                        "Fixing duplicate Elementary flows category "
                        "for '%s'" % f_obj.name)
                    f_obj.category = f_obj.category.replace(
                        "/Elementary Flows/", "/")

                # Map third-party technosphere flows to NAICS
                # https://github.com/NETL-RIC/ElectricityLCI/issues/149
                # NOTE: this overwrite breaks the reproducibility of the
                # UUIDs for these two flows
                tech_cat = "Technosphere Flows"
                pri_cat = "31-33: Manufacturing"
                lfo_cat = "3241: Petroleum and Coal Products Manufacturing"
                an_cat = (
                    "3253: Pesticide, Fertilizer, and Other Agricultural "
                    "Chemical Manufacturing"
                )
                if (f_obj.name == "Light fuel oil") and (
                    f_obj.flow_type == o.FlowType.PRODUCT_FLOW) and not (
                        pri_cat in f_obj.category):
                    logging.warning(
                        "Mapping 'Light fuel oil' technosphere flow to "
                        "NAICS 3241")
                    f_obj.category = "/".join([tech_cat, pri_cat, lfo_cat])
                elif (f_obj.name == "Ammonium nitrate") and (
                    f_obj.flow_type == o.FlowType.PRODUCT_FLOW) and not (
                        pri_cat in f_obj.category):
                    logging.warning(
                        "Mapping 'Ammonium nitrate' technosphere flow to "
                        "NAICS 3253")
                    f_obj.category = "/".join([tech_cat, pri_cat, an_cat])

            # Loop through exchanges a second time and re-number their
            # internal IDs to a consecutive order.
            p.last_internal_id = 0
            for e in p.exchanges:
                p.last_internal_id += 1
                e.internal_id = p.last_internal_id

            # Update inventory method description
            if p.process_type == o.ProcessType.LCI_RESULT:
                # All system processes are currently cradle-to-gate
                p.process_documentation.inventory_method_description = \
                    C2G_LCI_METHOD
            elif p.process_type == o.ProcessType.UNIT_PROCESS and has_provider:
                # Unit process w/ providers are considered gate-to-gate.
                p.process_documentation.inventory_method_description = \
                    G2G_LCI_METHOD
            elif (p.process_type == o.ProcessType.UNIT_PROCESS) and (
                    not has_provider):
                # Unit process w/o providers are considered cradle-to-gate.
                p.process_documentation.inventory_method_description = \
                    C2G_LCI_METHOD
            else:
                logging.warning("You should not be here")

        # Overwrite
        _save_to_json(file_path, data)


def write(processes, file_path, to_save=True):
    """Write a process dictionary as a olca-schema zip file to the given path.

    Note that a process has several root entity types associated with it,
    namely:

    - 'Actor',
    - 'Currency'
    - 'DQSystem'
    - 'EPD'
    - 'Flow'
    - 'FlowProperty'
    - 'ImpactCategory'
    - 'ImpactMethod'
    - 'Location'
    - 'Parameter'
    - 'Process'
    - 'ProductSystem'
    - 'Project'
    - 'Result'
    - 'SocialIndicator'
    - 'Source'
    - 'UnitGroup'

    and each of which need to be tracked and stored in the JSON-LD. Changes
    to any root entities triggers a modification of the JSON-LD file. Because
    the JSON-LD is a zip archive, the uuid.json files within it cannot be
    edited without first extracting from zip, editing, then re-zipping.

    To allow for editing of root entities (assuming any new information is
    good information), and because this method is called (again and again) in
    electricity.main, in each method call, the JSON-LD file is examined, its
    data extracted and updated with the latest data, and re-zipped to the same
    JSON-LD archive (deleting the old version in the process).

    The same methodology is adopted in NetlOlca Python class for interfacing
    with openLCA v2 projects. This is the way.

    Parameters
    ----------
    processes : dict
        OLCA schema dictionaries (e.g. Process).
    file_path : str
        A path to a zip file where the JSON-LD will be written.
    to_save : bool
        Whether this method should write the JSON-LD to zip file.

    Returns
    -------
    dict
        Original processes dictionary updated.

    Notes
    -----
    GreenDelta, olca-schema, Python tests (e.g., test_zipio.py).
    Online: https://github.com/GreenDelta/olca-schema/
    """
    # Make sure output folder exists
    file_dir = os.path.dirname(file_path)
    if not os.path.exists(file_dir):
        logging.info("Creating folder, '%s'" % file_dir)
        os.makedirs(file_dir)

    # Initialize root entity mapper (i.e., dictionary), which includes all
    # entities already written to the JSON-LD file, or simply GreenDelta's
    # FlowProperties and UnitGroups.
    spec_map = _init_root_entities(file_path)

    for p_key in processes.keys():
        # Pull the process dictionary
        d_vals = processes[p_key]

        # Create new process object and find quantitative reference exchange
        logging.info("Generating process for %s" % p_key)
        p, spec_map, e = _process(d_vals, spec_map)
        spec_map['Process']['ids'].append(p.id)
        spec_map['Process']['objs'].append(p)

        # Update the process dictionary and add UUID and reference details
        processes[p_key].update(p.to_dict())
        processes[p_key]['uuid'] = p.id
        if e is not None and isinstance(e, o.Exchange):
            try:
                processes[p_key]['q_reference_name'] = e.flow.name
                processes[p_key]['q_reference_id'] = e.flow.id
                processes[p_key]['q_reference_cat'] = e.flow.category
                processes[p_key]['q_reference_unit'] = e.unit.name
            except Exception as exception:
                logging.warning(
                    "Unexpected error when accessing quantitative "
                    "reference exchange for '%s'. %s" % (
                        p_key, str(exception)
                    )
                )

    # Write to JSON-LD zip format
    if to_save:
        logging.info("Saving to '%s'" % file_path)
        _save_to_json(file_path, spec_map)

    return processes


def _actor(name, dict_s):
    """Generate a reference object to an actor.

    Parameters
    ----------
    name : str
        Actor name
    dict_s : dict
        Dictionary with created olca schema root entities.

    Returns
    -------
    tuple
        olca_schema.Ref : Reference object to an Actor.
        dict : The updated root entities dictionary, ``dict_s``.

    Note
    ----
    Creates/overwrites Actor UUID using a standard method.
    """
    # HOTFIX: extract actor names from dictionary; [25.12.16; TWD]
    if isinstance(name, dict):
        name = _val(name, 'name', default="")

    # Skip unnamed or missing actors.
    if not isinstance(name, str) or name == '':
        return (None, dict_s)  # HOTFIX return type; [25.12.16; TWD]

    # Generate a standard UUID based on actor name:
    uid = _uid(o.ModelType.ACTOR, name)

    # Check to see if Actor is already recorded.
    # If so, retrieve it; otherwise, make new and record it!
    if uid in dict_s['Actor']['ids']:
        idx = dict_s['Actor']['ids'].index(uid)
        actor = dict_s['Actor']['objs'][idx]
        logging.debug("Found existing actor, %s" % actor.name)
    else:
        logging.debug("Creating new actor entity for '%s'" % name)
        actor = o.Actor()
        actor.id = uid
        actor.name = name
        dict_s['Actor']['ids'].append(uid)
        dict_s['Actor']['objs'].append(actor)

    return (actor.to_ref(), dict_s)


def _add_fed_commons(spec_map):
    """Append openLCA unit groups, flow properties, and DQI to a spec map
    dictionary.

    Parameters
    ----------
    spec_map : dict
        A dictionary of openLCA root entities.
        Requires 'UnitGroup' key dictionary value with keys, 'objs' and 'ids'.

    Returns
    -------
    dict
        The same spec map with UnitGroup, FlowProperty, DQSystem, and Source
        objects and UUIDs appended appropriately.

    Raises
    ------
    TypeError
        If the spec map is not a dictionary.
    KeyError
        If the spec map is missing the required key(s).
    """
    # A little bit of error handling :)
    if not isinstance(spec_map, dict):
        raise TypeError("Expected a dictionary, received %s" % type(spec_map))
    if "UnitGroup" not in spec_map.keys():
        raise KeyError("Failed to find required UnitGroup key!")
    if "FlowProperty" not in spec_map.keys():
        raise KeyError("Failed to find required FlowProperty key!")
    if "DQSystem" not in spec_map.keys():
        raise KeyError("Failed to find required DQSystem key!")
    if "Source" not in spec_map.keys():
        raise KeyError("Failed to find required Source key!")

    u_list, p_list = _read_fedefl()
    for u_obj in u_list:
        spec_map['UnitGroup']['objs'].append(u_obj)
        spec_map['UnitGroup']['ids'].append(u_obj.id)
    for p_obj in p_list:
        spec_map['FlowProperty']['objs'].append(p_obj)
        spec_map['FlowProperty']['ids'].append(p_obj.id)

    d_list, s_list =  _read_fedcore()
    for d_obj in d_list:
        spec_map['DQSystem']['objs'].append(d_obj)
        spec_map['DQSystem']['ids'].append(d_obj.id)
    for s_obj in s_list:
        spec_map['Source']['objs'].append(s_obj)
        spec_map['Source']['ids'].append(s_obj.id)

    return spec_map


def _archive_json(data_list, file_path):
    """Write a list of dictionaries to a JSON file.

    Parameters
    ----------
    data_list : list
        A list of olca schema objects.
    file_path : str
        A valid filepath to be written to (CAUTION: overwrites existing data)
    """
    logging.debug("Writing %d items to %s" % (len(data_list), file_path))
    out_str = ",".join([
        json.dumps(x.to_dict(), ensure_ascii=False) for x in data_list
    ])
    out_str = "[%s]" % out_str
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(out_str)


def _build_supply_chain(zh, pid, e_list=[], p_list=[]):
    """Populate the processes and process links lists based on the
    default providers assigned to a given process.

    Parameters
    ----------
    zh : zipio.ZipReader
        File handle to an opened JSON-LD archive.
    pid : str
        A process's universally unique identifier.
    e_list : list, optional
        A list of ProcessLinks, by default [].
    p_list : list, optional
        A list of Process UUIDs, by default [].

    Returns
    -------
    tuple
        A tuple of length two: list of ProcessLinks and a list of
        Process UUIDs.

    Notes
    -----
    1.  This method does not apply any standardization for UUID generation.
        It is arbitrarily generated by olca_schema class initialization.
    2.  The methods here are heavily based on those from NETL's NetlOlca class
        currently available online: https://github.com/NETL-RIC/netlolca.
    """
    # Pull process object from JSON-LD and add to the processes list.
    p_obj = zh.read(o.Process, pid)
    if p_obj and (pid not in p_list):
        logging.debug("Adding process, '%s'" % p_obj.name)
        p_list.append(pid)
        # Iterate over input exchanges w/ default providers.
        for ex in p_obj.exchanges:
            if ex.is_input and (ex.default_provider is not None):
                # NOTE: should id be generated?
                p_link = o.ProcessLink(
                    exchange=o.ExchangeRef(internal_id=ex.internal_id),
                    flow=ex.flow,
                    process=_make_process_ref(p_obj),
                    provider=ex.default_provider,
                )
                e_list.append(p_link)

                # Build supply chain for default provider, which should
                # itself be a process.
                e_list, p_list = _build_supply_chain(
                    zh, ex.default_provider.id, e_list, p_list)

    return (e_list, p_list)


def _check_source_year(s_year):
    """Checks value as valid year.

    Implements two basic checks:

    1. Type check (i.e., integer)
    2. Range check (values between 0 and current year + 1)

    Parameters
    ----------
    s_year : str, int
        A year.

    Returns
    -------
    int
        Year.
        Defaults to current year for invalid parameters.
    """
    try:
        int(s_year)
    except:
        logging.warning("Invalid source year, defaulting to current year.")
        r_year = _current_year()
    else:
        r_year = int(s_year)
        if (r_year < 0) or (r_year > _current_year() + 1):
            logging.warning(
                "Source year out of range! Default to current year.")
            r_year = _current_year()
    finally:
        return r_year


def _current_time():
    """Return a ISO-formatted time stamp for right now.

    Returns
    -------
    str
        Time stamp in ISO format.

    Examples
    --------
    >>> _current_time()
    '2023-10-25T21:06:19.200675+00:00'
    """
    return datetime.datetime.now(pytz.utc).isoformat()


def _current_year():
    """Return today's calendar year.

    Returns
    -------
    int
        Year.
    """
    return datetime.datetime.now(pytz.utc).year


def _dq_entry(dict_d):
    """Return the data quality entry vector.

    Searches both the process dictionary and processDocumentation dictionary
    (if available) for the dqEntry value.

    Parameters
    ----------
    dict_d : dict
        Process or processDocumentation dictionary.

    Returns
    -------
    str
        Data quality entry vector, for example: '(1;3;2;5)'.
    """
    return _format_dq_entry(_find_dq(dict_d, 'dqEntry'))


def _dq_system(dict_d, dict_s, dq_type):
    """Generate reference to olca-schema DQSystem for a given process.

    Parameters
    ----------
    dict_d : dict
        Process or processDocumentation data dictionary.
    dict_s : dict
        Dictionary storing olca-schema root entities.
    dq_type : str
        The data quality type. Valid values include 'dqSystem' and
        'exchangeDqSystem'.

    Returns
    -------
    tuple
        olca_schema.Ref : Reference to DQSystem object.
        dict : Updated dictionary storing olca-schema root entities.

    Raises
    ------
    ValueError
        If invalid dq_type received.

    Notes
    ----
    This method introduces a standard for defining new DQSystems;
    however, this should never occur, given the definition provided in
    process_dictionary_writer.py.

    For more information on the DQSystem attribute for a Process, see
    https://greendelta.github.io/olca-schema/classes/Process.html#dqsystem
    """
    if dq_type not in ['dqSystem', 'exchangeDqSystem']:
        raise ValueError(
            "Expected 'dqSystem' or 'exchangeDqSystem', "
            "received '%s'" % dq_type
        )
    # Pre-define description text.
    dq_desc = "A process data quality system entry."
    if dq_type == 'exchangeDqSystem':
        dq_desc = "An exchange data quality system entry."

    dq = _find_dq(dict_d, dq_type)
    if not isinstance(dq, dict):
        return (None, dict_s)

    dq_id = _val(dq, '@id')
    dq_name = _val(dq, 'name', default="none")
    if dq_id in dict_s['DQSystem']['ids']:
        idx = dict_s['DQSystem']['ids'].index(dq_id)
        dq_obj = dict_s['DQSystem']['objs'][idx]
        logging.debug("Found existing DQSystem, %s" % dq_obj.name)
    else:
        logging.debug("Creating new DQSystem entity for '%s'" % dq_name)
        dq_obj = o.DQSystem()
        # HOTFIX: pre-defined UUIDs are set using version 4 (not 3)
        if not _uid_is_valid(dq_id, 4):
            # HOTFIX: add standard UUID naming
            logging.debug("Generating DQSystem UUID")
            dq_id = _uid(o.ModelType.DQ_SYSTEM, dq_type, dq_name)
        dq_obj.id = dq_id
        dq_obj.name = dq_name
        dq_obj.description = dq_desc
        # NOTE: uncertainty, indicators, and source are not included here!
        dict_s['DQSystem']['ids'].append(dq_obj.id)
        dict_s['DQSystem']['objs'].append(dq_obj)
    return (dq_obj.to_ref(), dict_s)


def _exchange(dict_d, dict_s):
    """Generate an Exchange object.

    Note that the process_dictionary_writer.py methods responsible for
    generating the data dictionary for exchanges does not include location
    and includes 'baseUncertainty' and 'pedigreeUncertainty', which
    appear to be data quality indicators.

    Note also that the internalId is meant to be an integer and helps identify
    duplicate input or output flows within the same process, so they can be
    linked to different providers.

    Parameters
    ----------
    dict_d : dict
        Data dictionary for an exchange.
        Expected keys are defined in `exchange_table_creation_*` methods
        found in process_dictionary_writer.py, and may include the following:

        - internalID : str (meant to be unique integer within a process)
        - avoidedProduct : bool
        - comment : str
        - flow : dict
        - flowProperty: str
        - input: bool
        - quantitativeReference : bool
        - baseUncertainty : str
        - provider : str
        - amount : float
        - amountFormula : str
        - unit : dict
        - pedigreeUncertainty : str

    dict_s : dict
        Dictionary with olca-schema root entity information.

    Returns
    -------
    tuple
        olca_schema.Exchange : Exchange object (or NoneType)
        dict : The olca-schema root entity dictionary, updated

    Note
    ----
    Secondary dictionary keywords based on olca-schema v2 added in 2025
    in order to create residual mix processes. The first keywords are based
    on the eLCI model runs, while the secondary keywords are the standard
    openLCA keywords.
    """
    # Error handle missing data:
    if dict_d is None:
        logging.debug("No exchange data!")
        return (None, dict_s)

    e = o.Exchange.from_dict({
        'isQuantitativeReference': _val(
            dict_d,
            'quantitativeReference',
            'isQuantitativeReference', # HOTFIX: add 2nd keyword [25.12.16;TWD]
            default=False),
        'isInput': _val(
            dict_d,
            'input',
            'isInput',                 # HOTFIX: add 2nd keyword [25.12.16;TWD]
            default=False),
        'isAvoidedProduct': _val(
            dict_d,
            'avoidedProduct',
            'isAvoidedProduct',        # HOTFIX: add 2nd keyword [25.12.16;TWD]
            default=False),
        'amount': _val(dict_d, 'amount', default=0.0),
        'dqEntry': _format_dq_entry(_val(dict_d, 'dqEntry')),
        'description': _val(
            dict_d,
            'comment',
            'description')             # HOTFIX: add 2nd keyword [25.12.16;TWD]
    })

    # Set unit (uses olca unit references)
    unit_name = _val(dict_d, 'unit', default='kg')
    e.unit = _unit(unit_name)

    # Set reference to flow property
    f_prop = _flow_property(unit_name, dict_s)
    if f_prop is not None:
        e.flow_property = f_prop.to_ref()

    # Set flow and uncertainty
    e.flow, dict_s = _flow(_val(dict_d, 'flow'), f_prop, dict_s)
    e.uncertainty = _uncertainty(_val(dict_d, 'uncertainty'))

    # Find the provider process reference (or create one);
    #  note that this does not update the dict_s entries, but searches them!
    #  BUG: are you sure this doesn't update dict_s?
    #  HOTFIX: add secondary keyword for 'defaultProvider' [25.12.16; TWD]
    p_ref, dict_s, _ = _process(
        _val(dict_d, 'provider', 'defaultProvider'), dict_s
    )
    if p_ref is not None:
        e.default_provider = p_ref.to_ref()

    return (e, dict_s)


def _exchange_list(dict_d, dict_s):
    """Generates a list of exchanges.

    Parameters
    ----------
    dict_d : dict
        Data dictionary for a process.
        Expected key is 'exchanges', which should return a list of
        dictionaries, where each dictionary describes an exchange.
    dict_s : dict
        Data dictionary for storing olca-schema root entities.

    Returns
    -------
    tuple
        list : List of Exchange objects
        dict : The olca-schema root entity dictionary, updated
        olca_schema.Exchange or NoneType : quantitative reference exchange
    """
    r_list = []
    last_id = 0
    q_ref = None
    for e in _val(dict_d, 'exchanges', default=[]):
        ex_obj, dict_s = _exchange(e, dict_s)
        if ex_obj is not None:
            last_id += 1
            ex_obj.internal_id = last_id
            r_list.append(ex_obj)

            # NOTE: there should be at most one quantitative reference in an
            # exchange list; if there is more than one, then the last
            # instance is returned.
            if ex_obj.is_quantitative_reference:
                q_ref = ex_obj
    return (r_list, dict_s, q_ref)


def _find_dq(dict_d, dict_key):
    """Search a process dictionary (and its documentation) for a given data
    quality attribute.

    Parameters
    ----------
    dict_d : dict
        Process (or processDocumentation) dictionary.
    dict_key : str
        Data quality key (e.g., 'dqEntry', 'dqSystem' or 'exchageDqSystem')

    Returns
    -------
    str, dict
        Data quality value.
    """
    dq = _val(dict_d, dict_key)
    if isinstance(dict_d, dict) and not dq:
        # NOTE: dq attributes may be found under processDocumentation!
        logging.debug("Searching process documentation for data quality key!")
        dq = _find_dq(_val(dict_d, 'processDocumentation'), dict_key)
    return dq


def _find_location_code_name(loc):
    """Helper function to find location codes and names amongst
    balacing authority areas, EIA regions, NERC regions, U.S. states,
    and openLCA countries & regions.

    Parameters
    ----------
    loc : str
        Location name or code.

    Returns
    -------
    tuple
        A tuple of length two:

        - str, location code
        - str, location name

    Notes
    -----
    This method searches BA names (from ``utils``), US states (from
    ``globals``) and openLCA locations (from :func:`_get_olca_locations`).

    This method prioritizes U.S. state names over the FERC regions for
    Hawaii and Alaska.
    """
    ba = read_ba_codes()
    ba = ba.reset_index(drop=False)

    # For any matched column, quickly find the code and name columns
    code_name_map = {
        'BA_Acronym': {'code': 'BA_Acronym', 'name': 'BA_Name'},
        'BA_Name':  {'code': 'BA_Acronym', 'name': 'BA_Name'},
        'EIA_Region_Abbr':  {'code': 'EIA_Region_Abbr', 'name': 'EIA_Region'},
        'EIA_Region': {'code': 'EIA_Region_Abbr', 'name': 'EIA_Region'},
        'FERC_Region': {'code': 'FERC_Region_Abbr', 'name': 'FERC_Region'},
        'FERC_Region_Abbr': {'code': 'FERC_Region_Abbr', 'name': 'FERC_Region'}
    }

    # Don't search these columns for matches (looking at you, Time Zone!)
    drop_cols = [x for x in ba.columns if x not in code_name_map.keys()]
    ba = ba.drop(columns=drop_cols)

    # Correct for U.S. states
    if loc in US_STATES.keys():
        logging.info("Found U.S. state abbreviation")
        loc = "US-%s" % loc
    elif loc in US_STATES.values():
        logging.info("Found U.S. state name")
        loc = "United States of America, %s" % loc

    mask = (ba == loc)
    _, col_indices = np.where(mask)

    # Get the matching column indices
    match_cols = list(set(col_indices))

    # Initialize string returns
    name = "%s" % loc
    code = "%s" % loc

    # Check to see if the location code is a BA, EIA, or FERC region
    if len(match_cols) == 0:
        logging.info("Failed first attempt find location for '%s'" % loc)

        # Check upstream coal basins locations [26.03.05; TWD]
        coal_dict = COAL_BASIN_CODES
        coal_rev = {v: k for k, v in COAL_BASIN_CODES.items()}

        # Check openLCA standard locations
        olca_locs = _get_olca_locations()
        olca_dict = {x.code: x.name for x in olca_locs}
        rev_dict = {x.name: x.code for x in olca_locs}

        if loc in coal_dict.keys():
            logging.info("Found name in coal basin locations")
            code = coal_dict[loc]
            name = loc
        elif loc in coal_rev.keys():
            logging.info("Found code in coal basin locations")
            code = loc
            name = coal_rev[loc]
        elif loc in olca_dict.keys():
            logging.info("Found code in openLCA locations")
            code = loc
            name = olca_dict[loc]
        elif loc in rev_dict.keys():
            logging.info("Found name in openLCA locations")
            code = rev_dict[loc]
            name = loc

        # HOTFIX: use global for coal import process [26.03.19;TWD]
        if name == 'Import' or code == 'IMP':
            logging.info("Setting import location to Global (GLO)")
            name = "Global"
            code = "GLO"
    else:
        # Assumes hierarchy if multiple columns were matched:
        #   BA first, EIA second, FERC last.
        match_col = list(ba.columns)[match_cols[0]]
        row_mask = ba[match_col] == loc
        num_matches = row_mask.sum()

        if num_matches > 1:
            logging.warning(
                "Found multiple matches for '%s' under '%s'" % (loc, match_col)
            )
        code = ba.loc[row_mask, code_name_map[match_col]['code']].values[0]
        name = ba.loc[row_mask, code_name_map[match_col]['name']].values[0]

    return (code, name)


def _find_ref_exchange(p):
    """Return the exchange class object associated as the quantitative
    reference.

    Note that there should be only one in a list of exchanges for a single
    process.

    Parameters
    ----------
    p : o.Process
        An olca_schema.Process class.

    Returns
    -------
    o.Exchange
        An olca_schema.Exchange object (or NoneType if not found)
    """
    e_obj = None
    if p.exchanges is None or len(p.exchanges) == 0:
        pass
    else:
        for e in p.exchanges:
            if e.is_quantitative_reference:
                e_obj = e
    return e_obj


def _flow(dict_d, flowprop, dict_s):
    """Generate a reference to a flow object.

    Called by :func:`_exchange`.

    Parameters
    ----------
    dict_d : dict
        Flow data dictionary.
    flowprop : olca_schema.FlowProperty or olca_schema.Ref
        FlowProperty or Ref to a FlowProperty object.
    dict_s : dict
        Dictionary with olca_schema root entities.

    Returns
    -------
    tuple
        olca_schema.Ref : Reference to a flow object.
        dict : Updated dictionary with olca_schema root entities.
    """
    if not isinstance(dict_d, dict):
        logging.warning("No flow data received!")
        return (None, dict_s)

    uid = _val(dict_d, 'id', '@id')
    name = _val(dict_d, 'name')
    category_path = _val(dict_d, 'category', default='')
    is_waste = "waste" in category_path.lower()

    # HOTFIX: remove technosphere/3rd party flow check;
    # it duplicates every waste flow in the JSON-LD [2023-12-05; TWD]

    # Check for flow existence
    if uid in dict_s['Flow']['ids']:
        idx = dict_s['Flow']['ids'].index(uid)
        flow = dict_s['Flow']['objs'][idx]
        logging.debug("Found previous flow, '%s'" % flow.name)
    else:
        logging.debug("Creating new flow for, '%s' (%s)" % (name, uid))
        if _uid_is_valid(uid, 3) or _uid_is_valid(uid, 4):
            # Keep the good UUID
            pass
        else:
            # Generate new v3 ID based on standard format
            logging.debug("Generating new UUID for flow, '%s'" % name)
            uid = _uid(o.ModelType.FLOW, category_path, name)

        # Correct the default flow type for waste flows.
        def_type = "ELEMENTARY_FLOW"
        if is_waste:
            dict_d['flowType'] = "WASTE_FLOW"
            def_type = "WASTE_FLOW"

        f_type = _flow_type(_val(dict_d, 'flowType', default=def_type))
        flow = o.new_flow(name, f_type, flowprop)

        # Add perfunctory flow metadata now; update w/ FEDEFL metadata in post
        flow.id = uid
        flow.category = category_path

        # Update master list
        dict_s['Flow']['ids'].append(uid)
        dict_s['Flow']['objs'].append(flow)
    return (flow.to_ref(), dict_s)


def _flow_property(unit_name, dict_s):
    """Return the openLCA flow property reference for the given unit.

    Parameters
    ----------
    unit_name : str or dict
        The unit name (e.g., 'kg') or unit dictionary as generated by
        the unit() method in process_dictionary_writer.py.
    dict_s : dict
        The dictionary with openLCA root entity data.

    Returns
    -------
    olca_schema.FlowProperty
        Flow property object (or NoneType, if not defined).

    Examples
    --------
    >>> import pandas as pd
    >>> f = ("https://github.com/GreenDelta/olca-schema/"
    ...      "blob/master/py/olca_schema/units/units.csv")
    >>> df = pd.read_csv(f)
    >>> list(df['flow property name'].unique())
    ['Area*time',
     'Duration',
     'Market value, bulk prices',
     'Volume',
     'Person transport',
     'Goods transport (mass*distance)',
     'Area',
     'Radioactivity',
     'Mass*time',
     'Length*time',
     'Length',
     'Volume*Length',
     'Mass',
     'Energy',
     'Vehicle transport',
     'Biotic Production (Transf.)',
     'Volume*time',
     'Number of items',
     'Energy/area*time',
     'Mechanical Filtration (Transf.)',
     'Items*Length',
     'Groundwater Replenishment (Transf.)',
     'Energy/mass*time',
     'Groundwater Replenishment (Occ.)',
     'Physicochemical Filtration (Transf.)',
     'Mechanical Filtration (Occ.)',
     'Physicochemical Filtration (Occ.)']
    """
    r_obj = None
    if isinstance(unit_name, dict):
        try:
            unit_name = unit_name["name"]
        except KeyError:
            logging.error(
                "Missing the required 'name' key in unit dictionary!")
            return r_obj

    # HOTFIX: reference new flow property list
    p_ref = o_units.property_ref(unit_name)
    if p_ref is None:
        logging.error(
            "Unknown unit, '%s'; no flow property reference!" % unit_name)
    elif p_ref.id in dict_s['FlowProperty']['ids']:
        logging.debug("Reading existing flow property")
        pid = dict_s['FlowProperty']['ids'].index(p_ref.id)
        r_obj = dict_s['FlowProperty']['objs'][pid]
    else:
        # Assumes federal elementary flow list was used to populate flow
        # properties; therefore, the old way of trying to recreate a
        # flow property from only Ref objects is removed.
        logging.info("Failed to find flow property for '%s'" % unit_name)

    return r_obj


def _flow_type(f_type):
    """Retrieve olca-schema FlowType object.

    Parameters
    ----------
    f_type : str
        Flow type (e.g., "ELEMENTARY_FLOW", "PRODUCT_FLOW" or "WASTE_FLOW").

    Returns
    -------
    olca_schema.FlowType
        An enum type as defined in olca-schema package.
    """
    for i in o.FlowType:
        if i.value == f_type:
            return i
    return None


def _format_date(entry):
    """Convert traditional M/D/YYYY date string to ISO format.

    Parameters
    ----------
    entry : str
        Date string in the format: month/day/year.

    Returns
    -------
    str
        ISO-formatted date string, 'YYYY-MM-DDZHH:MM:SS'.
         returns NoneType.
    """
    try:
        d_obj = datetime.datetime.strptime(entry, '%m/%d/%Y')
    except TypeError:
        logging.warning("Expected date as string, found %s" % type(entry))
        return None
    except ValueError:
        # HOTFIX: add a second-level test for ISO correctness; [25.12.16;TWD]
        try:
            d_obj = datetime.datetime.fromisoformat(entry)
        except ValueError:
            logging.warning(
                "Received unexpected date format (M/D/YYYY): '%s'" % entry)
            return None
        else:
            return d_obj.isoformat()
    except Exception as e:
        logging.warning("Encountered an unexpected error. %s" % str(e))
        return None
    else:
        return d_obj.isoformat()


def _format_dq_entry(entry):
    """Format data quality entries.

    Parameters
    ----------
    entry : str
        Data quality entry, like (1;2;5;3).

    Returns
    -------
    str
        Data quality entry where floating point numbers are converted to ints.
        Returns NoneType if an invalid parameter is encountered.

    Notes
    -----
    For more information on data quality strings, see
    https://greendelta.github.io/olca-schema/classes/Process.html#dqentry
    """
    if not isinstance(entry, str):
        return None
    e = entry.strip()
    if len(e) < 2:
        return None
    e = e.rstrip(')').lstrip('(')
    nums = e.split(';')
    for i in range(len(nums)):
        if ((nums[i] == 'n.a.') or (nums[i] == "nan")):
            continue
        else:
            nums[i] = str(round(float(nums[i])))
    return '(%s)' % ';'.join(nums)


def _get_olca_locations():
    """Helper method to create a list of location objects based on
    Green Delta's openLCA list of locations.

    Returns
    -------
    list
        List of Location objects.
    """
    # Putting these in Fed Commons data store; albeit from GreenDelta.
    data_dir = os.path.join(paths.local_path, "fedcommons")

    loc_file = "locations.json"
    loc_path = os.path.join(data_dir, loc_file)
    loc_list = []

    if not os.path.isfile(loc_path) and check_output_dir(data_dir):
        # esupy's method reads GreenDelta's GitHub.
        # https://github.com/USEPA/esupy/blob/main/esupy/location.py
        loc_meta = olca_location_meta()

        # Create a location dictionary.
        # Based on `build_location_dict` in generate_processes.py
        # on flac-utils https://github.com/FLCAC-admin/flcac-utils.
        loc_meta = loc_meta.drop(columns='Category')
        loc_meta.columns = loc_meta.columns.str.lower()
        loc_meta['description'] = loc_meta['description'].fillna("")
        loc_meta = loc_meta.rename(
            columns={'id': '@id'}
        ).to_dict(orient='index')

        # Create your location objects
        for loc_code in loc_meta:
            loc_list.append(o.Location().from_dict(loc_meta[loc_code]))

        _archive_json(loc_list, loc_path)

    # Only read locally if needed (i.e., if data wasn't just downloaded)
    if os.path.exists(loc_path) and len(loc_list) == 0:
        logging.debug("Reading locations from local JSON")
        with open(loc_path, 'r', encoding='utf-8') as f:
            my_list = json.load(f)
        for my_item in my_list:
            loc_list.append(o.Location.from_dict(my_item))

    return loc_list


def _get_location_dict():
    """Helper method to create a dictionary of location metadata based on
    GreenDelta's openLCA locations.

    Returns
    -------
    dict
        A dictionary of location metadata where keys are the location codes
        (e.g., 'CA', 'CA-BC', 'US', 'US-PA').

    Notes
    -----
    Used in :func:`_location` to reference against location codes for processes
    """
    loc_dict = {}
    loc_list = _get_olca_locations()
    for loc_item in loc_list:
        code = loc_item.code
        if code not in loc_dict:
            loc_dict[code] = loc_item.to_dict()
    return loc_dict


def _heat_elem_flow():
    """Returns Energy, heat resource from FEDEFL Elementary Flow List

    Returns
    -------
    o.Flow
        Flow object for 'Energy, heat' as found in the Commons' FEDEFL.

    Notes
    -----
    The dictionary used to create this flow object is based on the downloaded
    JSON-LD found `here <https://www.lcacommons.gov/lca-collaboration/Federal_LCA_Commons/elementary_flow_list/dataset/FLOW/8c959db8-d359-36e3-8517-588e1c21df4a>`_.
    """
    json_dict = {
        "@type":"Flow",
        "@id":"8c959db8-d359-36e3-8517-588e1c21df4a",
        "name":"Energy, heat",
        "description":"From Federal Elementary Flow List v1.3.0, written by fedelemflowlist v1.3.0. Flow Class: Energy. Preferred flow.",
        "category":"Elementary flows/resource/air",
        "version":"01.03.000",
        "lastChange":"2024-12-28T13:02:35.764Z",
        "flowType":"ELEMENTARY_FLOW",
        "isInfrastructureFlow":False,
        "flowProperties":[{
            "@type":"FlowPropertyFactor",
            "isRefFlowProperty":True,
            "flowProperty":{
                "@type":"FlowProperty",
                "@id":"f6811440-ee37-11de-8a39-0800200c9a66",
                "name":"Energy",
                "category":"Technical flow properties",
                "refUnit":"MJ"
            },
            "conversionFactor":1.0
        }]
    }
    heat_flow = o.Flow.from_dict(json_dict)

    return heat_flow


def _init_root_entities(json_file):
    """Generate dictionary for each openLCA schema root entity.

    Check for JSON-LD file existence and read root entities from file;
    otherwise, pre-populate with GreenDelta's UnitGroups and FlowProperties.

    Returns
    -------
    dict
        Dictionary with primary keys for each root entity (camel-case).
        The values are dictionaries with three keys: 'class', 'objs', and 'ids'.
        The 'ids' list is for quick referencing and 'objs' list is for actual
        writing to file. The 'class' is value added (if needed).
    """
    # Create the empty dictionary for each olca schema root entity
    # (these are the ones that need to be written to the JSON-LD zip file)
    r_dict = _root_entity_dict()

    # Check to see if the JSON-LD file was already written to;
    # if so, read the old root entity data; otherwise, add data from the
    # Federal LCA Commons (e.g., unit groups, flow properties, and DQI data).
    if os.path.exists(json_file):
        r_dict = _read_jsonld(json_file, r_dict)
    else:
        r_dict = _add_fed_commons(r_dict)

    return r_dict


def _isnum(n):
    """Type checking for number.

    Performs no type casting of number strings (e.g., '23' is not a number).

    Parameters
    ----------
    n : Any
        A Python object (e.g., str, int, float) to be tested as numeric.

    Returns
    -------
    bool
        Whether the argument is a number.
    """
    if not isinstance(n, (float, int)):
        return False
    return not math.isnan(n)


def _location(dict_d, dict_s):
    """Create a new location or reference an existing one.

    Notes
    -----
    Overwrites openLCA's random UUID generator.

    Parameters
    ----------
    dict_d : dict or str
        A dictionary with location data or a string of location name.
    dict_s : dict
        Dictionary with created root entities.

    Returns
    -------
    tuple
        olca.Ref :
            A reference object to an olca Location.
        dict :
            The dict_s, updated.

    Notes
    -----
    For information on Location attributes, see
    https://greendelta.github.io/olca-schema/classes/Location.html
    """
    # Check for missing location code (e.g., ISO 2-letter country code)
    # No code, no location!
    # HOTFIX: locations may just be a string [2023-11-14; TWD]
    if isinstance(dict_d, str):
        code, name = _find_location_code_name(dict_d)
        uid = None
    elif isinstance(dict_d, dict):
        code, name = _find_location_code_name(_val(dict_d, 'name'))
        uid = _val(dict_d, 'id', '@id')
    else:
        code = ""
        name = ""
        uid = ""

    if code == '':
        logging.debug("No location!")
        return (None, dict_s)

    # HOTFIX: Use GreenDelta's location codes [26.02.13; TWD].
    gd_loc = _get_location_dict()
    if code in gd_loc:
        logging.debug(f"Found location {code} in openLCA default locations")
        dict_d = gd_loc.get(code)
        uid = _val(dict_d, 'id', '@id')

    # Check for valid UUID; otherwise, generate one
    if not (_uid_is_valid(uid, 3) or _uid_is_valid(uid, 4)):
        uid = _uid(o.ModelType.LOCATION, code)

    # Check if location already exists in our records; otherwise, create
    # and record the new location.
    if uid in dict_s['Location']['ids']:
        idx = dict_s['Location']['ids'].index(uid)
        location = dict_s['Location']['objs'][idx]
        logging.debug("Using existing location, %s" % location.name)
    else:
        logging.debug("Creating new location entry for '%s'" % code)
        location = o.Location(id=uid, code=code)
        location.name = name  # fix name based on new lookup [26.02.26;TWD]
        location.latitude = _val(dict_d, 'latitude')
        location.longitude = _val(dict_d, 'longitude')
        location.description = _val(dict_d, 'description')
        dict_s['Location']['ids'].append(uid)
        dict_s['Location']['objs'].append(location)
    return (location.to_ref(), dict_s)


def _make_entity_dict(e_dict, e_key):
    """Convenience function to convert two lists into a single dictionary.

    Parameters
    ----------
    e_dict : dict
        A data dictionary containing olca schema UUIDs (ids) and their respective class objects (objs)
    e_key : str
        The data dictionary key to convert into a dictionary; corresponds to olca root entity names (e.g., 'Actor' or 'Flow')

    Returns
    -------
    dict
        A dictionary of UUID keys and their class objects as values.
    """
    r_dict = {}
    num_ids = len(e_dict[e_key]['ids'])
    for i in range(num_ids):
        uid = e_dict[e_key]['ids'][i]
        try:
            obj = e_dict[e_key]['objs'][i]
        except IndexError:
            # Happens, for example, if current data dictionary was
            # created with IDs only (i.e., no objects). Since we
            # don't have the object data, it isn't getting copied!
            logging.warning("Skipping %s (%s)! Missing class info!" % (
                e_key, uid))
        else:
            r_dict[uid] = obj

    return r_dict


def _make_product_system(f_path, process, description=""):
    """Generate a product system for a given process.

    Parameters
    ----------
    f_path : str
        A file path to an existing JSON-LD file with all process data saved.
    process : olca-schema.Process
        A Process object to be converted to a Product System.
    description : str, optional
        The product system description text, by default "".

    Returns
    -------
    olca-schema.ProductSystem
        A product system built on default providers for the given process.
    """
    # Find the reference process
    r_ex = _find_ref_exchange(process)

    # Create a new product system
    # https://greendelta.github.io/olca-schema/classes/ProductSystem.html
    # NOTE: ``last_change`` automatically set to current date/time
    product = o.ProductSystem(
        description=description,
        name=process.name,
        ref_exchange=o.ExchangeRef(r_ex.internal_id),
        ref_process=process.to_ref(),
        target_amount=r_ex.amount,
        target_flow_property=r_ex.flow_property,
        target_unit=r_ex.unit,
        version=process.version
    )

    f = zipio.ZipReader(f_path)

    # Build processLinks and processes; hotfix w/ empty lists
    ex_list, pd_list = _build_supply_chain(f, process.id, [], [])
    product.processes = [
        _make_process_ref(f.read(o.Process, x)) for x in pd_list]
    product.process_links = ex_list

    f.close()

    return product


def _make_process_ref(p_obj):
    """Generate a Ref object for a given process, preserving as much
    metadata as possible.

    Parameters
    ----------
    p_obj : olca_schema.Process
        A process object.

    Returns
    -------
    olca_schema.Ref
        A reference object to the given process.

    Notes
    -----
    An attempt to make up for poor `to_ref` class method.
    See https://github.com/GreenDelta/olca-schema/issues/8
    """
    ref_obj = o.Ref()
    if isinstance(p_obj, o.Process):
        ref_obj = o.Ref(
            id=p_obj.id,
            category=p_obj.category,
            description=p_obj.description,
            location=p_obj.to_dict().get("location", {}).get("name", ""),
            process_type=p_obj.process_type,
            ref_type=o.RefType.Process,
        )
    return ref_obj


# IN PROGRESS: needs tested
def _make_rem_con_process(pid, e_dict, rem_txt):
    """Helper function to create residual consumption process.

    Parameters
    ----------
    pid : str
        A universally unique identified associated with a consumption mix
        process (e.g., 'Electricity; at grid; consumption mix - CAISO - FERC')
    e_dict : dict
        The master entity dictionary.
    rem_txt : str
        Additional residual process description.

    Returns
    -------
    tuple
        A tuple of length two:

        - str, the new residual consumption process UUID
        - dict, the updated master entity dictionary

    Notes
    -----
    This method adds the new residual consumption mix to the master
    entity dictionary.
    """
    # Extract the original process data & convert to dictionary
    p_idx = e_dict['Process']['ids'].index(pid)
    p_obj = e_dict['Process']['objs'][p_idx]
    p_dict = p_obj.to_dict()

    # Rename to a residual process; try both combinations
    try:
        p_dict['name'] = _make_residual_process_name(
            p_dict['name'], True, False
        )
    except ValueError:
        p_dict['name'] = _make_residual_process_name(
            p_dict['name'], False, False
        )

    # Reset UUID (to trigger new UUID generation) and update description.
    p_dict['@id'] = None
    if isinstance(p_dict['description'], str):
        p_dict['description'] += " "
        p_dict['description'] += rem_txt
    else:
        p_dict['description'] = rem_txt

    # Create new residual process
    rem_obj, e_dict, _ = _process(p_dict, e_dict)

    # Add new process to master entity list
    if rem_obj.id not in e_dict['Process']['ids']:
        e_dict['Process']['ids'].append(rem_obj.id)
        e_dict['Process']['objs'].append(rem_obj)
    else:
        # This message should never display.
        logging.warning(
            "New residual process UUID already exists! %s" % rem_obj.id
        )

    return (rem_obj.id, e_dict)


def _make_rem_gen_process(pid, ba_name, e_dict, rem_txt, rem_df):
    """Create a residual mix process for a given at-grid generation mix
    process.

    Parameters
    ----------
    pid : str
        At-grid generation mix process for a given balancing authority.
    ba_name : str
        The name of the given balancing authority.
    e_dict : dict
        Master entity dictionary (stored all olca-schema root entities).
    rem_txt : str
        Additional process description text.
    rem_df : pandas.DataFrame
        Data frame for the given balancing authority with new generation mixes.

    Returns
    -------
    tuple
        A tuple of length two:

        - str, the new residual grid mix process UUID
        - dict, the updated master entity database
    """
    #
    # Step 1: Create a new Process object.
    #
    # Extract (the original process data)
    p_idx = e_dict['Process']['ids'].index(pid)
    p_obj = e_dict['Process']['objs'][p_idx]
    # Convert (to dictionary for editing)
    p_dict = p_obj.to_dict()
    # Rename (to a residual process)
    # NOTE: may want to pass the two booleans along as arguments to this func.
    p_dict['name'] = _make_residual_process_name(p_dict['name'], True, True)
    # Reset (to trigger new UUID generation)
    p_dict['@id'] = None
    # Update (w/ new residual process description)
    if isinstance(p_dict['description'], str):
        p_dict['description'] += " "
        p_dict['description'] += rem_txt
    else:
        p_dict['description'] = rem_txt
    # Create (new residual process)
    rem_obj, e_dict, _ = _process(p_dict, e_dict)

    #
    # Step 2: Update exchange amounts to reflect residuals.
    #
    # Define query for searching fuel category from exchange description
    fq = re.compile("^from (\\w+) - (.*)$")
    # Extract (residual mix data for current balancing authority)
    b = rem_df.query("`%s` == '%s'" % ('Subregion', ba_name))
    # Iterate (over each new process exchange)
    for p_ex in rem_obj.exchanges:
        # NOTE: For electricity grid mixes, there are always two or more
        # exchanges: one output and X inputs. The inputs have the grid mix
        # values we want.
        if p_ex.is_input:
            # Get fuel name (e.g. 'GAS').
            f_name = ""
            fc = fq.match(p_ex.description)
            if fc:
                f_name = fc.group(1)

            # Query BA data for new mix associated with current fuel.
            a = b.query("`%s` == '%s'" % ('FuelCategory', f_name))

            # Update mix amounts
            if len(a) == 1:
                # Best case scenario; set new mix amount
                new_mix = a.iloc[0].Gen_Ratio_new
                logging.debug("Replacing %s with %s for %s in %s" % (
                    p_ex.amount, new_mix, f_name, ba_name))
                p_ex.amount = new_mix
            elif len(a) == 0 and len(b) == 0:
                # Failed to find BA in the data frame.
                # Could be that there is just no REC data to remove.
                # For the time being, keep it, because we're none the wiser.
                logging.info("Failed to find '%s'; skipping" % ba_name)
            elif len(a) == 0:
                # Failed to find fuel for a known BA; set to zero.
                # TODO: consider removing this exchange from exchanges list
                logging.info("Zeroing mix for '%s' in %s" % (f_name, ba_name))
                p_ex.amount = 0.0
            else:
                # This is a bad place to be.
                logging.warning(
                    "Found multiple matches of '%s' for '%s'!" % (
                        f_name, ba_name
                    )
                )

    # NOTE: Consider adding a new Source here, also.

    #
    # Step 3: Add new residual process to the master entity list
    #
    if rem_obj.id not in e_dict['Process']['ids']:
        e_dict['Process']['ids'].append(rem_obj.id)
        e_dict['Process']['objs'].append(rem_obj)
    else:
        logging.warning(
            "New residual process UUID already exists! %s" % rem_obj.id
        )

    return (rem_obj.id, e_dict)


def _make_residual_process_name(p_name, at_grid=True, is_gen=True):
    """Create a new process name for residual generation at grid.

    Parameters
    ----------
    p_name : str
        process name (e.g., Electricity; at grid; generation mix)
    at_grid : bool, optional
        Whether name includes "at grid"; otherwise, "at user";
        defaults to True
    is_gen : bool, optional
        Whether names includes "generation"; otherwise, "consumption";
        defaults to True

    Returns
    -------
    str
        The same electricity generation grid mix process name, but with
        'residual' added to the name.

    Raises
    ------
    ValueError
        For a process name that is not 'Electricity; at grid; generation mix'.

    Notes
    -----
    This method is taken from Davis et al. (2025) NetlOlca. Online:
    https://edx.netl.doe.gov/dataset/netlolca, DOI:10.18141/2503973.

    Examples
    --------
    >>> orig_name = (
    ...     "Electricity; at grid; generation mix - Arlington Valley, LLC"
    ... )
    >>> _make_residual_process_name(orig_name, True, True)
    'Electricity; at grid; residual generation mix - Arlington Valley, LLC'
    """
    g_txt = "at user"
    if at_grid:
        g_txt = "at grid"
    c_txt = "consumption"
    if is_gen:
        c_txt = "generation"
    q = re.compile("^(Electricity; %s;)( %s mix - .*)$" % (g_txt, c_txt))
    if q.match(p_name):
        return q.sub("\\1 residual\\2", p_name)
    else:
        raise ValueError(
            "Expected 'Electricity; %s; %s process', found '%s'" % (
                g_txt, c_txt, p_name
            )
        )


def _match_process_names(p_list, q):
    """Return a list of process UUIDs that match a given name query.

    Parameters
    ----------
    p_list : list
        A list of Process objects (e.g., as read from JSON-LD file).
    q : re.Pattern
        A regular expression pattern object.
        For example, ``q = re.compile("^Electricity; at grid; .*")``

    Returns
    -------
    list
        A list of process universally unique identifiers (str)

    Notes
    -----
    This should also work on other root entities with a name and id
    attribute (e.g., ProductSystem).
    """
    r_list = []
    for ref in p_list:
        r = q.match(ref.name)
        if r:
            r_list.append(ref.id)
    return r_list


def _process(dict_d, dict_s):
    """Generate a new Process object.

    If the process includes exchanges, and one of the exchanges is marked
    as a quantitative reference, then that exchange object is also returned.

    Parameters
    ----------
    dict_d : dict
        Process data dictionary.
    dict_s : dict
        olca-schema root entity dictionary.

    Returns
    -------
    tuple
        A tuple of length three.

        - olca_schema.Process or NoneType, the process object
        - dict, the (potentially) updated root entity dictionary
        - olca_schema.Exchange or NoneType, quantitative reference exchange

    Notes
    -----
    The root entity dictionary does not get the new process by default;
    however, other entities (e.g., Location, DQSystem, Actor, and Source) are
    added to the root entity dictionary if encountered for the first time.
    """
    if not isinstance(dict_d, dict):
        return (None, dict_s, None)

    uid = _val(dict_d, '@id')
    name = _val(dict_d, 'name')
    category = _val(dict_d, 'category', default='')
    location_code = _val(dict_d, 'location', 'name', default='')

    # Hotfix location code dictionary [25.12.16; TWD]
    if isinstance(location_code, dict) and 'name' in location_code:
        location_code = location_code['name']

    # Generate the standardized UUID, if absent
    if uid is None:
        logging.debug("Generating new process UUID for '%s'" % name)
        uid = _uid(
            o.ModelType.PROCESS,
            category,
            location_code,
            name)

    # Check for process existence:
    if uid in dict_s['Process']['ids']:
        idx = dict_s['Process']['ids'].index(uid)
        p = dict_s['Process']['objs'][idx]
        logging.debug("Found existing process, %s" % p.name)
        # HOTFIX: add missing e_ref from existing process
        e_ref = _find_ref_exchange(p)
    else:
        logging.debug("Creating new Process entity for '%s'" % name)
        p = o.new_process(name=name)
        p.id = uid
        p.category = category
        p.version = _val(dict_d, 'version', default=VERSION)
        p.description = _val(dict_d, 'description')

        # The olca_schema.new_process() defaults to UNIT PROCESS.
        # Check for other type (i.e., LCI result)
        p_type = _val(dict_d, 'processType', default='UNIT_PROCESS')
        if p_type != "UNIT_PROCESS":
            logging.debug("Setting process type to LCI RESULT")
            p.process_type = o.ProcessType.LCI_RESULT

        # No location will return a none-type.
        p.location, dict_s = _location(_val(dict_d, 'location'), dict_s)
        p.process_documentation, dict_s = _process_doc(
            _val(dict_d, 'processDocumentation'),
            dict_s
        )

        # DQSystems have pre-defined UUIDs (v.4);
        #   see process_dictionary_writer.py
        p.dq_entry = _dq_entry(dict_d)
        p.dq_system, dict_s = _dq_system(dict_d, dict_s, 'dqSystem')
        p.exchange_dq_system, dict_s = _dq_system(
            dict_d, dict_s, 'exchangeDqSystem'
        )

        p.exchanges, dict_s, e_ref = _exchange_list(dict_d, dict_s)

    return (p, dict_s, e_ref)


def _process_doc(dict_d, dict_s):
    """Generate process documentation for an olca-schema Process object.

    Parameters
    ----------
    dict_d : dict
        A dictionary with process documentation.
    dict_s : dict
        Dictionary with olca-schema root entities.

    Returns
    -------
    tuple
        olca_schema.ProcessDocumentation : Process documentation object.
        dict : Updated dictionary with olca-schema root entities, ``dict_s``.

    Notes
    -----
    For details on the expected properties for process documentation, see:
    https://greendelta.github.io/olca-schema/classes/ProcessDocumentation.html
    """
    # Copy the fields that have the same format as in the olca-schema spec.
    copy_fields = [
        'timeDescription',
        'technologyDescription',
        'dataCollectionDescription',
        'completenessDescription',
        'dataSelectionDescription',
        'reviewDetails',
        'dataTreatmentDescription',
        'inventoryMethodDescription',
        'modelingConstantsDescription',
        'samplingDescription',
        'restrictionsDescription',
        'copyright',
        'intendedApplication',
        'projectDescription',
    ]

    doc = o.ProcessDocumentation()

    # Check to see if process documentation was sent; if so, update
    if isinstance(dict_d, dict):
        doc = doc.from_dict(
            {field: _val(dict_d, field) for field in copy_fields}
        )
        doc.valid_from = _format_date(_val(dict_d, 'validFrom'))
        doc.valid_until = _format_date(_val(dict_d, 'validUntil'))

        # Add Actor references
        doc.reviewer, dict_s = _actor(
            _val(dict_d, 'reviewer'),
            dict_s
        )
        doc.data_documentor, dict_s = _actor(
            _val(dict_d, 'dataDocumentor'),
            dict_s
        )
        doc.data_generator, dict_s = _actor(
            _val(dict_d, 'dataGenerator'),
            dict_s
        )
        doc.data_set_owner, dict_s = _actor(
            _val(dict_d, 'dataSetOwner'),
            dict_s
        )

        # Update sources
        doc.publication, dict_s = _source(_val(dict_d, 'publication'), dict_s)
        doc.sources, dict_s = _source_list(
            _val(dict_d, 'sources', default=[]),
            dict_s
        )
    doc.creation_date = _current_time()
    return (doc, dict_s)


def _read_fedefl():
    """Return list of GreenDelta's unit group and flow property objects.

    This utilizes the Federal LCA Commons' public API to pull data from the
    Elementary Flow List repository.

    A local copy of the LCA Commons' Federal Elementary Flow List unit groups
    is either accessed (in eLCI's data directory) or created (using requests).

    Notes
    -----
    This method writes up to two files in electricitylci's data directory:

    -   flow_properties.json
    -   unit_groups.json

    Returns
    -------
    tuple
        A tuple of length two.
        First item is a list of 27 olca-schema UnitGroup objects.
        Second item is a list of 33 olca-schema FlowProperty objects.
    """
    # Define the base URL for the public API
    url = (
        "https://www.lcacommons.gov/"
        "lca-collaboration/ws/public/download/json"
    )

    # Using "path=FLOW_PROPERTY" obtains all Flow properties and unit groups
    token_url = url + (
        "/prepare/Federal_LCA_Commons/elementary_flow_list?path=FLOW_PROPERTY"
    )

    # NEW data store [25.02.12; TWD]
    data_dir = os.path.join(paths.local_path, "fedcommons")
    check_output_dir(data_dir)

    u_file = "unit_groups.json"
    u_path = os.path.join(data_dir, u_file)
    u_list = []

    p_file = "flow_properties.json"
    p_path = os.path.join(data_dir, p_file)
    p_list = []

    if not os.path.exists(u_path) or not os.path.exists(p_path):
        # Pull from Federal Elementary Flow List
        logging.info("Reading data from Federal LCA Commons")
        #adding 20s timeout to avoid long delays due to server issues.
        token = requests.get(token_url, timeout=20).content.decode()
        r = requests.get(f"{url}/{token}", timeout=20)
        if r.ok:
            with ZipFile(io.BytesIO(r.content)) as z:
                # Find the unit groups, convert them to UnitGroup class
                for name in z.namelist():
                    # Note there are only three folders in the zip file:
                    # 'flow_properties', 'flows', and 'unit_groups';
                    # we want the 27 JSON files under unit_groups
                    # and the 33 JSON files under flow_properties.
                    if name.startswith("unit") and name.endswith("json"):
                        u_dict = json.loads(z.read(name))
                        u_obj = o.UnitGroup.from_dict(u_dict)
                        u_list.append(u_obj)
                    elif name.startswith("flow_") and name.endswith("json"):
                        p_dict = json.loads(z.read(name))
                        p_obj = o.FlowProperty.from_dict(p_dict)
                        p_list.append(p_obj)
        else:
            logging.error(
                "Failed to access Elementary Flow List on Fed Commons!")

        # Archive to avoid running requests again.
        _archive_json(u_list, u_path)
        logging.info("Saved unit groups from LCA Commons to JSON")

        _archive_json(p_list, p_path)
        logging.info("Saved flow properties from LCA Commons to JSON")

    # Only read locally if needed (i.e., if data wasn't just downloaded)
    if os.path.exists(u_path) and len(u_list) == 0:
        logging.info("Reading unit groups from local JSON")
        with open(u_path, 'r') as f:
            my_list = json.load(f)
        for my_item in my_list:
            u_list.append(o.UnitGroup.from_dict(my_item))

    if os.path.exists(p_path) and len(p_list) == 0:
        logging.info("Reading flow properties from local JSON")
        with open(p_path, 'r') as f:
            my_list = json.load(f)
        for my_item in my_list:
            p_list.append(o.FlowProperty.from_dict(my_item))

    return (u_list, p_list)


def _read_fedcore():
    """Return list of GreenDelta's DQSystem and Source objects.

    This utilizes the Federal LCA Commons' public API to pull data from the
    core database repository.

    A local copy of the LCA Commons' DQSystems and Source objects
    is either accessed (in user's data directory) or created (using requests).

    Notes
    -----
    This method writes up to two files in electricitylci's data store:

    -   dq_systems.json
    -   dq_sources.json

    Returns
    -------
    tuple
        A tuple of length two.
        First item is a list of 2 olca-schema DQSystem objects.
        Second item is a list of 1 olca-schema Source objects.
    """
    # Define the base URL for the public API
    url = (
        "https://www.lcacommons.gov/"
        "lca-collaboration/ws/public/download/json"
    )

    # Token for JSON-LD with data quality indicators
    token_url = url + (
        "/prepare/Federal_LCA_Commons/Fed_Commons_core_database?path=DQ_SYSTEM"
    )

    # Save data to the user's electricitylci data store.
    data_dir = os.path.join(paths.local_path, "fedcommons")
    check_output_dir(data_dir)

    d_file = "dq_systems.json"
    d_path = os.path.join(data_dir, d_file)
    d_list = []

    s_file = "dq_sources.json"
    s_path = os.path.join(data_dir, s_file)
    s_list = []

    if not os.path.exists(d_path) or not os.path.exists(s_path):
        # Pull from Federal Elementary Flow List
        logging.info("Reading data from Federal LCA Commons")
        #adding 20s timeout to avoid long delays due to server issues.
        token = requests.get(token_url, timeout=20).content.decode()
        r = requests.get(f"{url}/{token}", timeout=20)
        if r.ok:
            with ZipFile(io.BytesIO(r.content)) as z:
                for name in z.namelist():
                    # Note there are only two folders in the zip file:
                    # 'dq_systems' and 'sources'
                    if name.startswith("dq_systems") and name.endswith("json"):
                        d_dict = json.loads(z.read(name))
                        d_obj = o.DQSystem.from_dict(d_dict)
                        d_list.append(d_obj)
                    elif name.startswith("sources") and name.endswith("json"):
                        s_dict = json.loads(z.read(name))
                        s_obj = o.Source.from_dict(s_dict)
                        s_list.append(s_obj)
        else:
            logging.error(
                "Failed to access Elementary Flow List on Fed Commons!")

        # Archive to avoid running requests again.
        _archive_json(d_list, d_path)
        logging.info("Saved DQSystems from LCA Commons to JSON")

        _archive_json(s_list, s_path)
        logging.info("Saved DQI sources from LCA Commons to JSON")

    # Only read locally if needed (i.e., if data wasn't just downloaded)
    if os.path.exists(d_path) and len(d_list) == 0:
        logging.info("Reading DQSystems from local JSON")
        with open(d_path, 'r') as f:
            my_list = json.load(f)
        for my_item in my_list:
            d_list.append(o.DQSystem.from_dict(my_item))

    if os.path.exists(s_path) and len(s_list) == 0:
        logging.info("Reading DQI sources from local JSON")
        with open(s_path, 'r') as f:
            my_list = json.load(f)
        for my_item in my_list:
            s_list.append(o.Source.from_dict(my_item))

    return (d_list, s_list)


def _read_jsonld(json_file, root_dict, id_only=False):
    """Read root entities from JSON-LD file and append to root entity
    dictionary.

    Parameters
    ----------
    json_file : str
        A file path to a zipped JSON-LD file.
    root_dict : dict
        A dictionary with olca-schema root entity data (as provided by
        :func:`_root_entity_dict`).
    id_only : bool
        Whether to save UUIDs and olca-schema class objects.
        If true, only 'ids' list is read.

    Returns
    -------
    dict
        The same root entity dictionary with 'ids' and 'objs' lists updated.

    Raises
    ------
    OSError
        If the JSON-LD file path does not exist (or is not a file).

    Notes
    -----
    -   Methods are based on those from NETL's NetlOlca Python class.
    -   Reads full Class objects into memory (when `id_only` is false),
        which may be large for large projects (e.g., >2000 flows in the
        2016 baseline).

    Examples
    --------
    >>> my_file = "Federal_LCA_Commons-US_electricity_baseline.zip" # 2016 b.l.
    >>> my_dict = _read_jsonld(my_file, _root_entity_dict(), False)
    >>> print(
    ...     'Process',
    ...     len(my_dict['Process']['ids']), 'UUIDs',
    ...     len(my_dict['Process']['objs']), 'objects')
    Process 606 UUIDs 606 objects
    >>> my_dict = _read_jsonld(my_file, _root_entity_dict(), True)
    >>> print(
    ...     'Process',
    ...     len(my_dict['Process']['ids']), 'UUIDs',
    ...     len(my_dict['Process']['objs']), 'objects')
    Process 606 UUIDs 0 objects
    """
    if not os.path.isfile(json_file):
        raise OSError("File not found! %s" % json_file)
    else:
        # Create a file handle to the JSON-LD zip
        logging.info("Opening JSON-LD file, %s" % os.path.basename(json_file))
        j_file = zipio.ZipReader(json_file)
        for name in root_dict.keys():
            # Get IDs for each root entity
            spec = root_dict[name]['class']
            r_ids = j_file.ids_of(spec)
            logging.info("Read %d UUIDs for %s" % (len(r_ids), name))
            # Get the root entity object based on its type
            for rid in r_ids:
                # Only read from file when a new UUID is found.
                # (easier to debug this way, rather than a set for unique vals)
                if rid in root_dict[name]['ids']:
                    logging.debug(
                        "Skipping existing UUID for %s (%s)" % (name, rid))
                else:
                    if id_only:
                        root_dict[name]['ids'].append(rid)
                    else:
                        r_obj = None
                        try:
                            r_obj = j_file.read(spec, rid)
                        except Exception as e:
                            logging.warning(
                                "Failed to read %s (%s) from file! %s" % (
                                    name, rid, str(e)))
                        # Add the UUID and Class object pair to their lists
                        if r_obj is not None:
                            root_dict[name]['ids'].append(rid)
                            root_dict[name]['objs'].append(r_obj)
        j_file.close()

        return root_dict


def _rm_untracked_flows(data):
    """Post-processing method that identifies and removes untracked flows
    (i.e., flow objects that are not referenced in any exchanges).

    This is purely to reduce the size of the database.

    Parameters
    ----------
    data : dict
        A dictionary containing the olca-schema root element lists.

    Returns
    -------
    dict
        The same dictionary received, but with untracked flows removed
        from the Flow root element lists.

    Notes
    -----
    This finds 'Heat' technosphere input flow and elementary resource flow,
    which (somewhere in v2) are replaced with 'Energy, heat' elementary resource flow (from air).
    """
    # Pull full flow list (for removing untracked flows)
    f_list = sorted(data["Flow"]['ids'])

    # Initialize exchange flows
    e_list = []

    # Add flows to list of tracked exchanges
    for p in data["Process"]['objs']:
        for e in p.exchanges:
            e_list.append(e.flow.id)

    # Sort unique values to speed up search
    e_list = sorted(list(set(e_list)))

    # Remove untracked flows (i.e., any flows that aren't in an exchange)
    u_list = [x for x in f_list if x not in e_list]
    logging.info("Removing %d untracked flows" % len(u_list))
    for u_id in u_list:
        idx = data["Flow"]['ids'].index(u_id)
        logging.info("Untracked flow: '%s' in '%s'" % (
            data['Flow']['objs'][idx].name,
            data['Flow']['objs'][idx].category,
        ))
        data['Flow']['ids'].pop(idx)
        data['Flow']['objs'].pop(idx)

    return data


def _root_entity_dict():
    """Generate empty dictionary for each openLCA schema root entity.

    Returns
    -------
    dict
        Dictionary with primary keys for each root entity (camel-case).
        The values are dictionaries with three keys: 'class', 'objs', and 'ids'.
        The 'ids' list is for quick referencing and 'objs' list is for actual
        writing to file. The 'class' is value added (if needed).
    """
    return {
        'Actor': {
            'class': o.Actor,
            'objs': [],
            'ids': []},
        "Currency": {
            'class': o.Currency,
            'objs': [],
            'ids': []},
        'DQSystem': {
            'class': o.DQSystem,
            'objs': [],
            'ids': []},
        'EPD': {
            'class': o.Epd,
            'objs': [],
            'ids': []},
        'Flow': {
            'class': o.Flow,
            'objs': [],
            'ids': []},
        'FlowProperty': {
            'class': o.FlowProperty,
            'objs': [],
            'ids': []},
        'ImpactCategory': {
            'class': o.ImpactCategory,
            'objs': [],
            'ids': []},
        'ImpactMethod': {
            'class': o.ImpactMethod,
            'objs': [],
            'ids': []},
        'Location': {
            'class': o.Location,
            'objs': [],
            'ids': []},
        'Parameter': {
            'class': o.Parameter,
            'objs': [],
            'ids': []},
        'Process': {
            'class': o.Process,
            'objs': [],
            'ids': []},
        'ProductSystem': {
            'class': o.ProductSystem,
            'objs': [],
            'ids': []},
        'Project': {
            'class': o.Project,
            'objs': [],
            'ids': []},
        'Result': {
            'class': o.Result,
            'objs': [],
            'ids': []},
        'SocialIndicator': {
            'class': o.SocialIndicator,
            'objs': [],
            'ids': []},
        'Source': {
            'class': o.Source,
            'objs': [],
            'ids': []},
        'UnitGroup': {
            'class': o.UnitGroup,
            'objs': [],
            'ids': []}
    }


def _save_to_json(json_file, e_dict):
    """Write an entity dictionary to JSON-LD format.

    Parameters
    ----------
    json_file : str
        A file path to an existing or desired JSON-LD zip file.
    e_dict : dict
        An olca-schema entity dictionary where keys are entity names
        (e.g., 'Actor' and 'Flow') and the values are dictionaries
        containing lists of olca-schema objects (objs) and their universally
        unique identifiers (ids).
    """
    logging.info("Looking for %s" % os.path.basename(json_file))
    try:
        # Grab UUIDs and class objs from existing JSON-LD
        logging.info("Found existing data in JSON-LD")
        c_data = _read_jsonld(json_file, _root_entity_dict())
    except OSError:
        logging.info("No existing JSON-LD found")
        c_data = _root_entity_dict()
    else:
        logging.info("Successfully read data from previous JSON-LD")
        logging.info("Removing old archive file")
        os.remove(json_file)
    finally:
        # Update current data (c_data) with new (e_dict).
        # If JSON-LD exists, then current data are those UUIDs and class
        # objects from the file; otherwise, the current data is empty.
        e_dict = _update_data(c_data, e_dict)
        # Remove untracked flows; primarily to reduce database size.
        e_dict = _rm_untracked_flows(e_dict)

    logging.info("Writing to %s" % os.path.basename(json_file))
    with zipio.ZipWriter(json_file) as writer:
        for k in e_dict.keys():
            logging.info("Writing %d %s" % (len(e_dict[k]['ids']), k))
            if k == "Flow":
                # FEDEFL flows are not added as objects, write them separately
                # using fedelmflowlist.write_jsonld() [20240911; BY]
                # NOTE: calls `Writer._write_flows` in fedelemflowlist.jsonld,
                # which sets the last updated timestamp to .now() and will
                # always show up as a change when comparing two JSON-LD files.
                flowlist = fedelemflowlist.get_flows()
                flows = flowlist[flowlist['Flow UUID'].isin(e_dict[k]['ids'])]
                fedelemflowlist.write_jsonld(flows, path=None, zw=writer)

            for k_obj in e_dict[k]['objs']:
                # Last chance to fix Ref's and it's not perfect.
                if isinstance(k_obj, o.Ref):
                    logging.warning("Found Ref object in JSON-LD writer!")
                    logging.debug("%s Ref (%s)" % (k, k_obj.id))
                    k_type = k_obj.ref_type.value
                    k_dict = k_obj.to_dict()
                    k_obj = e_dict[k_type]['class'].from_dict(k_dict)

                if k == "Flow" and k_obj.id in flows['Flow UUID'].values:
                    # all FEDEFL flows written above
                    continue
                logging.debug("Writing %s entity (%s)" % (k, k_obj.id))
                writer.write(k_obj)


def _source(src_data, dict_s):
    """Generate a reference to a source object.

    Parameters
    ----------
    src_data : dict
        Source data dictionary.
    dict_s : dict
        Dictionary with created olca schema root entities.

    Returns
    -------
    tuple
        olca_schema.Ref : Reference object to Source (or NoneType).
        dict : Root entities dictionary, updated (``dict_s``).

    Notes
    -----
    For explanation of source object keys, see:
    https://greendelta.github.io/olca-schema/classes/Source.html
    """
    # If no source data retrieved, skip it!
    if not isinstance(src_data, dict):
        return (None, dict_s)
    if "Name" not in src_data.keys() or src_data['Name'] == '':
        return (None, dict_s)

    # Search for category and use it in conjunction with name for UUID
    # NOTE: categories are forward-slash separated strings, see:
    # https://greendelta.github.io/olca-schema/classes/RootEntity.html#category
    try:
        if isinstance(src_data['Category'], list):
            category = "/".join(src_data['Category'])
        elif isinstance(src_data["Category"], str):
            category = src_data["Category"]
        else:
            category = ''
    except KeyError:
        category = ''
    finally:
        uid = _uid(o.ModelType.SOURCE, category, src_data["Name"])

    # Check if source already exists.
    # If so, retrieve it; otherwise, create new source and record it!
    if uid in dict_s['Source']['ids']:
        idx = dict_s['Source']['ids'].index(uid)
        source = dict_s['Source']['objs'][idx]
        logging.debug("Found existing source, %s" % source.name)
    else:
        logging.debug("Creating new source entity for '%s'" % src_data['Name'])
        source = o.Source()
        source.id = uid
        source.category = category
        source.name = src_data["Name"]
        source.url = _val(src_data, "Url", default=None)
        source.version = _val(src_data, "Version", default=VERSION)
        source.text_reference = _val(
            src_data, "TextReference", default=src_data['Name'])
        source.year = _check_source_year(_val(src_data, "Year"))
        dict_s['Source']['ids'].append(uid)
        dict_s['Source']['objs'].append(source)

    return (source.to_ref(), dict_s)


def _source_list(s_data, dict_s):
    """Process all sources within a source list.

    Parameters
    ----------
    s_data : list
        A list of source dictionaries.
    dict_s : dict
        Dictionary of olca-schema root entities.

    Returns
    -------
    tuple
        list : List of source reference objects.
        dict : Updated dictionary of olca-schema entities, ``dict_s``.
    """
    r_list = []
    if not isinstance(s_data, list) or len(s_data) == 0:
        logging.warning("No source data provided!")
    else:
        for d in s_data:
            s_ref, dict_s = _source(d, dict_s)
            # skip missing references
            if s_ref:
                r_list.append(s_ref)
    return (r_list, dict_s)


def _uid(*args):
    """Generate UUID from the MD5 hash of a namespace identifier and a name.

    This method uses OID namespace, which assumes that the name is an ISO OID.
    Essentially, two strings are hashed together to create a UUID and, if the
    same namespace and path are given again, the same UUID would be returned.

    Warning
    -------
    The UUIDs generated by this method are version 3, which is different
    from standard processes defined elsewhere (e.g., DQSystems and Units).

    Parameters
    ----------
    args : tuple
        A tuple of key words representing a path (order matters).
        The path is a string with each argument separated by a forward slash.
        For flows, the path is 'modeltype.flow', flow name, compartment, and
        unit.
        For processes, the path is 'modeltype.process', process category,
        location, and name.

    Returns
    -------
    str
        A version 3 universally unique identifier (UUID)
    """
    path = '/'.join([str(arg).strip() for arg in args]).lower()
    logging.debug(path)
    return str(uuid.uuid3(uuid.NAMESPACE_OID, path))


def _uid_is_valid(uuid_str, version=3):
    """Check if string is a valid UUID.

    Parameters
    ----------
    uuid_str : str
    version : {1, 2, 3, 4}

    Returns
    -------
    bool
        `True` if uuid_str is a valid UUID, otherwise `False`.

    Examples
    --------
    >>> _uid_is_valid('c9bf9e57-1685-4c89-bafb-ff5af830be8a', 4)
    True
    >>> _uid_is_valid('c9bf9e58')
    False

    Notes
    -----
    Code snipped by Rafael (2020). CC-BY-SA 4.0. Online:
    https://stackoverflow.com/a/33245493
    """
    # HOTFIX: deal with non-strings (e.g., nan) [2023-11-14; TWD]
    try:
        uuid_obj = uuid.UUID(uuid_str, version=version)
    except (TypeError, ValueError, AttributeError):
        return False
    return str(uuid_obj) == uuid_str


def _uncertainty(dict_d):
    """Generate an uncertainty object.

    Creates an uncertainty object with a log-normal distribution with
    geometric mean and geometric standard deviation provided by the data
    dictionary.

    Parameters
    ----------
    dict_d : dict
        Uncertainty data dictionary.
        See uncertainty_table_creation in process_dictionary_writer.py.

    Returns
    -------
    olca_schema.Uncertainty
        A log-normal distribution uncertainty object.
        Returns NoneType for missing or invalid types.
    """
    if not isinstance(dict_d, dict):
        return None

    dt = _val(dict_d, 'distributionType')
    if dt != 'Logarithmic Normal Distribution':
        logging.debug("Found invalid uncertainty method, '%s'" % dt)
        return None

    gmean = _val(dict_d, 'geomMean')
    if isinstance(gmean, str):
        gmean = float(gmean)

    gsd = _val(dict_d, 'geomSd')
    if isinstance(gsd, str):
        gsd = float(gsd)

    if not _isnum(gmean) or not _isnum(gsd):
        logging.debug("Found invalid geometric mean/standard deviation!")
        return None

    u = o.Uncertainty.from_dict({
        'distributionType': o.UncertaintyType.LOG_NORMAL_DISTRIBUTION,
        'geomMean': gmean,
        'geomSd': gsd
    })

    return u


def _unit(unit_name):
    """Get the ID of the openLCA reference unit with the given name.

    Notes
    -----
    The version 4 UUIDs provided in this module are the same as those provided
    by GreenDelta's olca_schema.units sub-package, so it was replaced with
    their unit-reference method.

    Parameters
    ----------
    unit_name : str, dict
        If unit name is passed as a dictionary, it should have a key, "name"
        with a string value for the unit name (e.g., "MJ").

    Returns
    -------
    olca_schema.Ref
        A reference object for a Unit class.

    """
    if isinstance(unit_name, dict):
        try:
            unit_name = unit_name["name"]
        except KeyError:
            unit_name = ""
            logging.error(
                'dict passed as unit_name but does not contain name key')
    r_obj = o_units.unit_ref(unit_name)
    if r_obj is None:
        logging.error("Unknown unit, '%s'; no unit reference!" % unit_name)
    else:
        logging.debug("Returning unit, '%s'" % unit_name)

    return r_obj


def _update_data(cur_data, new_data):
    """Update a current data dictionary with new values.

    Parameters
    ----------
    cur_data : dict
        A data dictionary with UUIDs (ids) and olca root entities (objs) read
        from a JSON-LD zip archive (i.e., current data).
    new_data : dict
        A data dictionary with UUIDs (ids) and olca root entities (objs)
        processed by electricitylci.main; it may be the same or new values as
        already written to JSON-LD.

    Returns
    -------
    dict
        The current data updated with new values (i.e., overwrites existing
        values and appends new).
    """
    for k in cur_data.keys():
        # Make ids/obj lists to dict and update current data with new.
        d_cur = _make_entity_dict(cur_data, k)
        d_new = _make_entity_dict(new_data, k)
        d_cur.update(d_new)

        # Plop the new lists back into the data dictionary
        ids = []
        objs = []
        for uid, obj in d_cur.items():
            ids.append(uid)
            objs.append(obj)
        new_data[k]['ids'] = ids
        new_data[k]['objs'] = objs

    return new_data


def _update_providers(p, p_map, e_dict):
    """Helper function to replace default providers found in a process's
    exchange table based on a UUID replacement map (i.e., old UUID -> new UUID).

    Parameters
    ----------
    p : olca-schema.Process
        An instance of a Process class to be updated.
    p_map : dict
        A dictionary with Process UUIDs and keys (old providers) and Process
        UUIDs as values (new providers).
    e_dict : dict
        The master entity dictionary (e.g., see :func:`_init_root_entities`).

    Returns
    -------
    olca-schema.Process
        A modified version of parameter, ``p``, where default providers
        are updated based on the UUID map, ``p_map``.
    """
    # Read through residual process's exchange table
    num_ex = len(p.exchanges)
    for i in range(num_ex):
        p_ex = p.exchanges[i]
        # Skip outputs and inputs w/o providers (e.g., elem flows)
        if p_ex.is_input and p_ex.default_provider is not None:
            # Read the default provider UUID and find replacement provider
            dp_id = p_ex.default_provider.id
            rp_id = p_map[dp_id] # <- throws KeyError when not found
            # Get the replacement provider's Process object
            rp_idx = e_dict['Process']['ids'].index(rp_id)
            rp_obj = e_dict['Process']['objs'][rp_idx]
            # Update residual process object; link to new provider
            p.exchanges[i].default_provider = rp_obj.to_ref()

    return p


def _val(dict_d, *path, **kvargs):
    """Return value from a dictionary.

    If a valid key (path) is provided to a given dictionary (dict_d), the
    respective value of the dictionary is returned; otherwise, kvargs is
    checked for a default value. If a default value is present, then it is
    returned; otherwise, NoneType is returned.

    Parameters
    ----------
    dict_d : dict
    path : tuple, optional
        A tuple of dictionary keys (str).
        Note: there should only ever be one path; for multiple paths only
        the first key will be accessed.
    kvargs : dict
        Used to store default values.
        See key 'default'.

    Returns
    -------
    Variable
        The value from a given key to a given dictionary.
        If not found, a NoneType is returned.

    Examples
    --------
    >>> d = {'a': 1, 'b': 2, 'c': 3}
    >>> _val(d, 'a') # single key value
    1
    >>> _val(d,'x') # NoneType for missing key
    None
    >>> _val(d, 'x', 'y' 'z', 'a', 'b', 'c') # first real key's value
    1
    >>> _val(d, 'z', default=4) # default for missing key
    4
    """
    r_val = None
    if isinstance(dict_d, dict) and path:
        for p in path:
            if p in dict_d.keys():
                r_val = dict_d[p]
                break  # HOTFIX: stop on first found key
    if r_val is None and 'default' in kvargs:
        r_val = kvargs['default']
    return r_val


#
# SANDBOX --- testing grounds for ILCD process renaming.
#
if __name__ == '__main__':
    import os
    import re
    from electricitylci.utils import get_logger
    from electricitylci.globals import output_dir
    from electricitylci.olca_jsonld_writer import _read_jsonld
    from electricitylci.olca_jsonld_writer import _root_entity_dict

    log = get_logger(True, False)

    # Read the full JSON-LD into memory
    my_file = os.path.join(output_dir, "ELCI_2023_jsonld_20260213_091215.zip")
    my_dict = _read_jsonld(my_file, _root_entity_dict(), False)

    # The focus of ILCD renaming is on Process (and Product System)
    # Let's start with upstream (2121: Coal Mining)
    # The groups are (1) region, (2) coal type, (3) mine type / processing
    p = re.match("^coal extraction and processing - (.*), (.*), (.*)$")
