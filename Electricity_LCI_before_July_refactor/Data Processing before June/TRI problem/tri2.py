# -*- coding: utf-8 -*-
"""
Created on Fri May 25 14:05:01 2018

@author: TGhosh
"""

#!/usr/bin/env python

# TRI import and processing
# This script uses the TRI Basic Plus National Data File.
# Data files:https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-calendar-years-1987-2016
# Documentation on file format: https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides
# The format may change from yr to yr requiring code updates.
# This code has been tested for 2014.

import pandas as pd
import numpy as np
#from stewi import globals
#from stewi.globals import unit_convert

# Set some metadata
TRIyear = '2014'
#output_dir = globals.output_dir
#data_dir = globals.data_dir

# Import list of fields from TRI that are desired for LCI
def imp_fields(tri_fields_txt):
    tri_required_fields_csv = tri_fields_txt
    tri_req_fields = pd.read_table(tri_required_fields_csv, header=None)
    tri_req_fields = list(tri_req_fields[0])
    return tri_req_fields

#tri_required_fields = (imp_fields('TRI_required_fields.txt'))

# Import in pieces grabbing main fields plus unique amount and basis of estimate fields
# assigns fields to variables
def concat_req_field(list):
    source_name = ['TRIFID','CHEMICAL NAME','UNIT OF MEASURE'] + list
    return source_name

facility_fields = ['FACILITY NAME','FACILITY STREET','FACILITY CITY','FACILITY COUNTY','FACILITY STATE',
                   'FACILITY ZIP CODE','PRIMARY NAICS CODE','LATITUDE','LONGITUDE']

fug_fields = ['TOTAL FUGITIVE AIR EMISSIONS','FUGITIVE OR NON-POINT AIR EMISSIONS - BASIS OF ESTIMATE']
stack_fields = ['TOTAL STACK AIR EMISSIONS','STACK OR POINT AIR EMISSIONS - BASIS OF ESTIMATE']
streamA_fields = ['TOTAL DISCHARGES TO STREAM A','DISCHARGES TO STREAM A - BASIS OF ESTIMATE']
streamB_fields = ['TOTAL DISCHARGES TO STREAM B','DISCHARGES TO STREAM B - BASIS OF ESTIMATE']
streamC_fields = ['TOTAL DISCHARGES TO STREAM C','DISCHARGES TO STREAM C - BASIS OF ESTIMATE']
streamD_fields = ['TOTAL DISCHARGES TO STREAM D','DISCHARGES TO STREAM D - BASIS OF ESTIMATE']
streamE_fields = ['TOTAL DISCHARGES TO STREAM E','DISCHARGES TO STREAM E - BASIS OF ESTIMATE']
streamF_fields = ['TOTAL DISCHARGES TO STREAM F','DISCHARGES TO STREAM F - BASIS OF ESTIMATE']
onsiteland_fields = ['TOTAL LAND TREATMENT','LAND TRTMT/APPL FARMING - BASIS OF ESTIMATE']
onsiteother_fields = ['TOTAL OTHER DISPOSAL','OTHER DISPOSAL -BASIS OF ESTIMATE']
offsiteland_fields  = ['LAND TREATMENT']
offsiteother_fields  = ['OTHER LAND DISPOSAL']

import_facility = ['TRIFID'] + facility_fields
import_fug = concat_req_field(fug_fields)
import_stack = concat_req_field(stack_fields)
import_streamA = concat_req_field(streamA_fields)
import_streamB = concat_req_field(streamB_fields)
import_streamC = concat_req_field(streamC_fields)
import_streamD = concat_req_field(streamD_fields)
import_streamE = concat_req_field(streamE_fields)
import_streamF = concat_req_field(streamF_fields)
import_onsiteland = concat_req_field(onsiteland_fields)
import_onsiteother = concat_req_field(onsiteother_fields)
# Offsite treatment does not include basis of estimate codes
import_offsiteland = concat_req_field(offsiteland_fields)
import_offsiteother = concat_req_field(offsiteother_fields)

keys = ['fug', 'stack', 'streamA', 'streamB', 'streamC', 'streamD', 'streamE', 'streamF', 'onsiteland', 'onsiteother',
        'offsiteland', 'offsiteother']

values = [import_fug, import_stack, import_streamA, import_streamB,
          import_streamC, import_streamD, import_streamE, import_streamF,
          import_onsiteland, import_onsiteother, import_offsiteland, import_offsiteother]

# Create a dictionary that had the import fields for each release type to use in import process
import_dict = dict_create(keys, values)

dtype_dict = dict_create(values,)

# Import TRI file
tri_csv = '../TRI/US_' + TRIyear + '_v15/US_1_' + TRIyear + '_v15.txt'

tri_release_output_fieldnames = ['FacilityID', 'FlowName', 'Unit', 'FlowAmount','Basis of Estimate','ReleaseType']



# Cycle through file importing by release type, the dictionary key
def import_TRI_by_release_type(d):
    tri = pd.DataFrame()
    for k, v in d.items():
        #create a data type dictionary
        dtype_dict = {'TRIFID':"str", 'CHEMICAL NAME':"str", 'UNIT OF MEASURE':"str"}
        dtype_dict[v[3]] = "float"
        if len(v) > 4:
            dtype_dict[v[4]] = "str"
        tri_part = pd.read_csv(tri_csv, sep='\t', header=0, usecols=v, dtype=dtype_dict, error_bad_lines=False)
        tri_part['ReleaseType'] = k
        if k.startswith('offsite'):
            tri_part['Basis of Estimate'] = 'NA'
        tri_part.columns = tri_release_output_fieldnames
        tri = pd.concat([tri,tri_part])
    return tri



tri = import_TRI_by_release_type(import_dict)
len(tri)