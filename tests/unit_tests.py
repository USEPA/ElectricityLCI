#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# unit_tests.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os
import re

import numpy as np
import olca_schema as o

from electricitylci.olca_jsonld_writer import _init_root_entities
from electricitylci.utils import read_ba_codes


##############################################################################
# GLOBALS
##############################################################################
PRINT_LEN = 49


##############################################################################
# FUNCTIONS
##############################################################################
def check_consumption_mix_percents(js_dict):
    a = 'Checking at-grid consumption mix fractions'
    is_okay = True
    err = None
    p_list = []

    gcq = re.compile("^Electricity; at grid; consumption mix - (.*)$")
    gcr = match_process_names(js_dict['Process']['objs'], gcq)
    pids = [x[0] for x in gcr]
    pidx = [js_dict['Process']['ids'].index(x) for x in pids]
    for idx in pidx:
        p = js_dict['Process']['objs'][idx]
        values = np.array([])
        for e in p.exchanges:
            if e.is_input:
                values = np.append(values, e.amount)
        # Looking for poorly added mixes (should add to 1.0)
        if values.sum() < 1.0 - 1e9 or values.sum() > 1.0 + 1e9:
            p_list.append(p.id)

    num_p = len(p_list)
    if num_p == 0:
        show_msg(a, 'PASSED')
    else:
        show_msg(a, 'FAILED')
        is_okay = False
        err = {
            'msg': 'Found %d processes with poor mix fractions!' % num_p,
            'details': '',
        }
        for pid in p_list:
            idx = js_dict['Process']['ids'].index(pid)
            p = js_dict['Process']['objs'][idx]
            err['details'] += '%s has poorly summed mix fraction.\n' % p.name

    return (is_okay, err)


def check_generation_mix_percents(js_dict):
    a = 'Checking at-grid generation mix fractions'
    is_okay = True
    err = None
    p_list = []

    # At grid; generation mixes (only at BA level and includes Canada)
    ggq = re.compile("^Electricity; at grid; generation mix - (.*)$")
    ggr = match_process_names(js_dict['Process']['objs'], ggq)
    pids = [x[0] for x in ggr]
    pidx = [js_dict['Process']['ids'].index(x) for x in pids]
    for idx in pidx:
        p = js_dict['Process']['objs'][idx]
        values = np.array([])
        for e in p.exchanges:
            if e.is_input:
                values = np.append(values, e.amount)
        # Looking for poorly added mixes (should add to 1.0)
        if values.sum() < 1.0 - 1e9 or values.sum() > 1.0 + 1e9:
            p_list.append(p.id)

    num_p = len(p_list)
    if num_p == 0:
        show_msg(a, 'PASSED')
    else:
        show_msg(a, 'FAILED')
        is_okay = False
        err = {
            'msg': 'Found %d processes with poor mix fractions!' % num_p,
            'details': '',
        }
        for pid in p_list:
            idx = js_dict['Process']['ids'].index(pid)
            p = js_dict['Process']['objs'][idx]
            err['details'] += '%s has poorly summed mix fraction.\n' % p.name

    return (is_okay, err)


def check_power_plant_construction(js_dict):
    a = "Checking power plant construction flow"
    err = None

    etq = re.compile("^Electricity - ([A-Z]*) - (.*)$")
    etr = match_process_names(js_dict['Process']['objs'], etq)
    num_p = len(etr)
    is_okay = True
    p_list = []

    # Get process UUIDs
    pids = [x[0] for x in etr]
    # Get process object indices
    pidx = [js_dict['Process']['ids'].index(x) for x in pids]
    for idx in pidx:
        p = js_dict['Process']['objs'][idx]
        for e in p.exchanges:
            if e.is_input and e.flow.name == 'power plant construction':
                p_list.append(p.id)

    num_found = len(p_list)
    if num_found == num_p:
        show_msg(a, 'PASSED')
    else:
        is_okay = False
        show_msg(a, 'FAILED')
        missing_pids = [x for x in pids if x not in p_list]
        err = {
            'msg': (
                "Found %d processes with no power plant "
                "construction" % len(missing_pids)),
            'details': '',
        }
        for m_pid in missing_pids:
            m_idx = js_dict['Process']['ids'].index(m_pid)
            p = js_dict['Process']['objs'][m_idx]
            err['details'] += '%s missing power plant construction.\n' % p.name

    return (is_okay, err)


def check_power_plant_provider(js_dict):
    a = "Checking power plant construction provider"
    err = None

    etq = re.compile("^Electricity - ([A-Z]*) - (.*)$")
    etr = match_process_names(js_dict['Process']['objs'], etq)
    is_okay = True
    p_list = []

    # Get process UUIDs
    pids = [x[0] for x in etr]
    # Get process object indices
    pidx = [js_dict['Process']['ids'].index(x) for x in pids]
    for idx in pidx:
        p = js_dict['Process']['objs'][idx]
        for e in p.exchanges:
            # If process has construction flow without a provider...
            if (e.is_input
                    and e.flow.name == 'power plant construction'
                    and e.default_provider is None):
                p_list.append(p.id)

    num_found = len(p_list)
    if num_found == 0:
        show_msg(a, 'PASSED')
    else:
        is_okay = False
        show_msg(a, 'FAILED')
        err = {
            'msg': (
                "Found %d generation processes with no power plant "
                "construction provider!" % num_found),
            'details': '',
        }
        for m_pid in p_list:
            m_idx = js_dict['Process']['ids'].index(m_pid)
            p = js_dict['Process']['objs'][m_idx]
            err['details'] += (
                '%s missing power plant construction provider.\n' % p.name
            )

    return (is_okay, err)


def check_unique_generation_flows(js_dict):
    a = "Checking unique generation process flows"
    err = {
        'msg': None,
        'details': "",
    }

    # Find generation processes
    etq = re.compile("^Electricity - ([A-Z]*) - (.*)$")
    etr = match_process_names(js_dict['Process']['objs'], etq)
    num_p = len(etr)
    all_okay = True
    err_count = 0

    # Get process UUIDs
    pids = [x[0] for x in etr]
    # Get process object indices
    pidx = [js_dict['Process']['ids'].index(x) for x in pids]
    for idx in pidx:
        p = js_dict['Process']['objs'][idx]
        e_list = []
        for e in p.exchanges:
            if not e.is_input:
                f_idx = js_dict['Flow']['ids'].index(e.flow.id)
                f = js_dict['Flow']['objs'][f_idx]
                if f.flow_type == o.FlowType.ELEMENTARY_FLOW:
                    e_list.append(e.flow.id)
        if len(e_list) != len(set(e_list)):
            all_okay = False
            err_count += 1
            err['details'] += '%s has duplicate FEDEFL emissions!\n' % p.name
        elif len(e_list) == 0:
            all_okay = False
            err_count += 1
            err['details'] += "%s has no FEDEFL emissions!\n" % p.name

    if err_count == 0:
        show_msg(a, 'PASSED')
        err = None
    else:
        show_msg(a, 'FAILED')
        err['msg'] = (
            'Found %d out of %d processes with duplicated or '
            'missing FEDEFL emissions' % (err_count, num_p))

    return (all_okay, err)


def compare_lists(*args):
    if not args:
        return True  # No lists provided, consider them equal

    # Check if all lists have the same length as the first list
    first_list_len = len(args[0])
    if not all(len(lst) == first_list_len for lst in args):
        return False

    # Convert each list to a set
    sets_of_lists = [set(lst) for lst in args]

    # Compare all sets to the first set
    first_set = sets_of_lists[0]
    return all(current_set == first_set for current_set in sets_of_lists[1:])


def find_differences(list1, list2, list3):
    # For ease of use, send 'at grid; generation' for list 1,
    # 'at grid; consumption mix' for list 2, and
    # 'at user; consumption mix' for list 3.
    set1 = set(list1)
    set2 = set(list2)
    set3 = set(list3)

    in_a_only = (set1 - set2) - set3
    in_b_only = (set2 - set1) - set3
    in_c_only = (set3 - set1) - set2
    in_a_b_not_c = (set1.intersection(set2)) - set3
    in_a_c_not_b = (set1.intersection(set3)) - set2
    in_b_c_not_a = (set2.intersection(set3)) - set1

    differences = {}
    if in_a_only:
        differences["in_a_only"] = list(in_a_only)
    if in_b_only:
        differences["in_b_only"] = list(in_b_only)
    if in_c_only:
        differences["in_c_only"] = list(in_c_only)
    if in_a_b_not_c:
        differences["in_a_b_not_c"] = list(in_a_b_not_c)
    if in_a_c_not_b:
        differences["in_a_c_not_b"] = list(in_a_c_not_b)
    if in_b_c_not_a:
        differences["in_b_c_not_a"] = list(in_b_c_not_a)

    return differences


def find_orphan_processes(json_d):
    passed = True
    err = None
    a = "Checking for orphan processess"

    providers = []
    # Go through processes and get all providers
    for p in json_d['Process']['objs']:
        for e in p.exchanges:
            if e.default_provider:
                providers.append(e.default_provider.id)
    # Go through each product system and add its reference process
    for p in json_d['ProductSystem']['objs']:
        providers.append(p.ref_process.id)

    # Remove duplicates and sort
    providers = sorted(list(set(providers)))
    orphans = [x for x in json_d['Process']['ids'] if x not in providers]
    num_orphan = len(orphans)

    if num_orphan > 0:
        passed = False
        show_msg(a, 'FAILED')
        err = {
            'msg': "Found %d orphan processes!" % num_orphan,
            'details': '',
        }
    else:
        show_msg(a, 'PASSED')

    for x in orphans:
        x_idx = json_d['Process']['ids'].index(x)
        x_obj = json_d['Process']['objs'][x_idx]
        err['details'] += "%s\n" % x_obj.name

    return (passed, err)


def find_triplet_ba_processes(js_dict):
    err = None
    canada_bas = get_canadian_ba_names()
    a = "Checking number of BA processes"

    # At grid; generation mixes (only at BA level and includes Canada)
    ggq = re.compile("^Electricity; at grid; generation mix - (.*)$")
    ggr = match_process_names(js_dict['Process']['objs'], ggq)
    ba_gg_names = [x[1] for x in ggr if x[1] not in canada_bas]

    # At grid; consumption mixes
    gcq = re.compile("^Electricity; at grid; consumption mix - (.*) - BA$")
    gcr = match_process_names(js_dict['Process']['objs'], gcq)
    ba_gc_names = [x[1] for x in gcr]

    # At user; user mixes
    ucq = re.compile("^Electricity; at user; consumption mix - (.*) - BA$")
    ucr = match_process_names(js_dict['Process']['objs'], ucq)
    ba_uc_names = [x[1] for x in ucr]

    ba_okay = compare_lists(ba_gg_names, ba_gc_names, ba_uc_names)
    if ba_okay:
        show_msg(a, 'PASSED')
    else:
        show_msg(a, 'FAILED')
        err = {
            'msg': 'Inconsistent BAs found from generation to consumption',
            'details': '',
        }
        ba_diff = find_differences(ba_gg_names, ba_gc_names, ba_uc_names)
        for k,v in ba_diff.items():
            err['details'] += "%s: %s\n" % (k, "; ".join(v))

    return (ba_okay, err)


def find_doublet_ferc_processes(js_dict):
    err = None
    a = "Checking number of FERC processes"

    # At grid; consumption mixes
    gcq = re.compile("^Electricity; at grid; consumption mix - (.*) - FERC$")
    gcr = match_process_names(js_dict['Process']['objs'], gcq)
    ferc_gc_names = [x[1] for x in gcr]

    # At user; consumption mixes
    ucq = re.compile("^Electricity; at user; consumption mix - (.*) - FERC$")
    ucr = match_process_names(js_dict['Process']['objs'], ucq)
    ferc_uc_names = [x[1] for x in ucr]

    ferc_okay = compare_lists(ferc_gc_names, ferc_uc_names)
    if ferc_okay:
        show_msg(a, 'PASSED')
    else:
        show_msg(a, 'FAILED')
        ferc_diff = find_differences(ferc_gc_names, ferc_uc_names, [])
        err = {
            'msg': 'Inconsistent FERC regions in processes!',
            'details': ''
        }
        for k,v in ferc_diff.items():
            err['details'] +=  "%s: %s\n" % (k, "; ".join(v))

    return (ferc_okay, err)


def find_doublet_us_processes(js_dict):
    err = None
    a = "Checking number of US processes"

    gcq = re.compile("^Electricity; at grid; consumption mix - US - US$")
    gcr = match_process_names(js_dict['Process']['objs'], gcq)

    # At user; consumption mixes
    ucq = re.compile("^Electricity; at user; consumption mix - US - US$")
    ucr = match_process_names(js_dict['Process']['objs'], ucq)

    # US region
    us_okay = len(gcr) == len(ucr)
    if us_okay:
        show_msg(a, 'PASSED')
    else:
        show_msg(a, 'FAILED')
        err = {
            'msg': "Inconsistent US processes!",
            'details': '',
        }
        if len(gcr) > len(ucr):
            err['details'] = 'Missing US - at user; consumption mix'
        elif len(ucr) > len(gcr):
            err['details'] = 'Missing US - at grid; consumption mix'

    return (us_okay, err)


def get_canadian_ba_names():
    ba = read_ba_codes()
    # The EIA region abbreviation is 'CAN' for Canadian BAs
    # and the FERC region is 'Canada'
    return ba.loc[ba['EIA_Region_Abbr'] == 'CAN', 'BA_Name'].tolist()


def match_process_names(p_list, q):
    """Return list of process names and IDs that match query.

    Parameters
    ----------
    q : re.Pattern
        A regular expression pattern object.
        For example: ``q = re.compile("^Electricity; at grid; .*")``

    Returns
    -------
    list
        List of tuples, each tuple of length two or an return empty
        if no processes are found.

        - str: process UUID
        - str: process name (or sub-text from name, if groups defined)
    """
    r_list = []
    for ref in p_list:
        r = q.match(ref.name)
        if r:
            try:
                # Pull the search text (if provided)
                r_list.append(tuple([ref.id, r.group(1)]))
            except IndexError:
                # If match successful, then group 0 is just the full name
                r_list.append(tuple([ref.id, r.group(0)]))
    return r_list


def print_messages(msg_list, char_count):
    """
    Name:     print_messages
    Inputs:   - list, error messages (msg_list)
              - int, character fill line length (char_count)
    Outputs:  None.
    Features: Prints formatted error messages
    """
    for i in range(len(msg_list)):
        # Break each error message into individual words:
        msg = msg_list[i]['msg'].split(" ")

        # Split the error message into separate lines based on each
        # line's length
        out_lines = []
        line_num = 0
        out_lines.append("")
        for j in range(len(msg)):
            out_lines[line_num] += msg[j]
            count = len(out_lines[line_num])
            if count > char_count - 7:
                line_num += 1
                out_lines.append("")
            out_lines[line_num] += " "
        for k in range(len(out_lines)):
            if not out_lines[k].isspace():
                if k == 0:
                    print("{0:2}. {1:}".format(i + 1, out_lines[k]))
                else:
                    print("   {}".format(out_lines[k]))
    print("{}".format('-'*char_count))
    # TODO: add details


def run_unit_tests(js_dict):
    passed = [0, 0]
    to_proceed = True
    err_msgs = []

    # ORPHAN PROCESS TEST
    ut1_ok, ut1_err = find_orphan_processes(js_dict)
    passed[1] += 1
    if ut1_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut1_err is not None:
        err_msgs.append(ut1_err)

    # BA TRIPLET PROCESS TEST
    ut2_ok, ut2_err = find_triplet_ba_processes(js_dict)
    passed[1] += 1
    if ut2_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut2_err is not None:
        err_msgs.append(ut2_err)

    # FERC DOUBLET PROCESS TEST
    ut3_ok, ut3_err = find_doublet_ferc_processes(js_dict)
    passed[1] += 1
    if ut3_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut3_err is not None:
        err_msgs.append(ut3_err)

    # US DOUBLET PROCESS TEST
    ut4_ok, ut4_err = find_doublet_us_processes(js_dict)
    passed[1] += 1
    if ut4_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut4_err is not None:
        err_msgs.append(ut4_err)

    # UNIQUE GENERATION FEDEFL FLOW TEST
    ut5_ok, ut5_err = check_unique_generation_flows(js_dict)
    passed[1] += 1
    if ut5_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut5_err is not None:
        err_msgs.append(ut5_err)

    # POWER PLANT CONSTRUCTION TEST
    ut6_ok, ut6_err = check_power_plant_construction(js_dict)
    passed[1] += 1
    if ut6_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut6_err is not None:
        err_msgs.append(ut6_err)

    # POWER PLANT CONSTRUCTION PROVIDER TEST
    ut7_ok, ut7_err = check_power_plant_provider(js_dict)
    passed[1] += 1
    if ut7_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut7_err is not None:
        err_msgs.append(ut7_err)

    # GENERATION GRID MIX FRACTION TEST
    ut8_ok, ut8_err = check_generation_mix_percents(js_dict)
    passed[1] += 1
    if ut8_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut8_err is not None:
        err_msgs.append(ut8_err)

    # GENERATION GRID MIX FRACTION TEST
    ut9_ok, ut9_err = check_consumption_mix_percents(js_dict)
    passed[1] += 1
    if ut9_ok:
        passed[0] += 1
    else:
        to_proceed = False
    if ut9_err is not None:
        err_msgs.append(ut9_err)

    return (to_proceed, passed, err_msgs)


def show_msg(msg, pf):
    print("{} {} {}".format(msg, "."*(PRINT_LEN - len(msg)), pf))


##############################################################################
# MAIN
##############################################################################
if __name__ == '__main__':
    jsonld_path = input("Enter JSON-LD file path: ")
    if not os.path.isfile(jsonld_path):
        raise OSError(
            "Failed to find file, '%s'! Check the path." % jsonld_path)
    elif not jsonld_path.endswith(".zip"):
        _, ext = os.path.splitext(jsonld_path)
        raise TypeError("Expected a zipped JSON-LD file, not, '%s'" % ext)

    # Read JSONLD to a standard dictionary.
    jsonld = _init_root_entities(jsonld_path)

    line_len = 57
    greeting = " ElectricityLCI Unit Tests "
    ending = " end tests "
    gdots = int(0.5*(line_len - len(greeting)))
    edots = int(0.5*(line_len - len(ending)))
    print("{}{}{}".format("-"*gdots, greeting, "-"*gdots))

    # TODO: consider reading the JSON-LD to dictionary, and pass it
    # off to these unit tests, rather than read them individually.
    is_okay, passed_checks, messages = run_unit_tests(jsonld)

    if is_okay:
        print("{}{}{}".format('-'*edots, ending, '-'*edots))
        print("Passed {}/{}".format(passed_checks[0], passed_checks[1]))
        print("")
    else:
        print("{}{}{}".format('-'*edots, ending, '-'*edots))
        print("Passed {}/{}".format(passed_checks[0], passed_checks[1]))
        print("")
        print("Encountered the following errors:")
        print_messages(messages, line_len)
