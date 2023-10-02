#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# emissions_other_sources.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
"""
Integration of emissions from data sources that were not included in stewi
"""

import pandas as pd


##############################################################################
# FUNCTIONS
##############################################################################
def integrate_replace_emissions(new_emissions, stewi_emissions):
    """
    Replace and/or add emissions to those compiled in stewi. This is done by
    concatenating the two dataframe and dropping duplicates (keep the new values).

    For reference, the following FlowNames are used in stewi and should be used
    in the new_emissions dataframe:
    - Sulfur dioxide
    - Carbon dioxide
    - Methane
    - Nitrogen oxides
    - Nitrous oxide

    Parameters
    ----------
    new_emissions : dataframe
        Total annual emissions from a facility. Columns must include:
        FlowAmount          float64
        FlowName             object
        DataReliability    float64
        Source               object
        Unit                 object
        Year                  int64
        eGRID_ID             object
    stewi_emissions : dataframe
        Annual facility emissions that have been compiled in stewi. Columns
        include:
        FRS_ID                int64
        FacilityID           object
        FlowAmount          float64
        FlowName             object
        DataReliability    float64
        Source               object
        Unit                 object
        Year                  int64
        eGRID_ID             object

    Returns
    -------
    dataframe
        [description]
    """
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
    stewi_emissions["eGRID_ID"]=stewi_emissions["eGRID_ID"].astype(int)

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
    stewi_emissions.loc[stewi_emissions['FlowName'].isin(flow_list),
                        'FlowName'] = stewi_emissions['FlowName'].str.capitalize()
    #added line below because eGRID_ID got duplicated somewhere causing error in concat
    stewi_emissions = stewi_emissions.loc[:,~stewi_emissions.columns.duplicated()].copy()
    updated_emissions = pd.concat([stewi_emissions, new_emissions])

    subset_cols = [
        'Compartment', 'FlowName',
        'Unit', 'eGRID_ID'
    ]
    # Remove Year from this list, otherwise results in duplciate emissions 
    # by facility if years dont match in specs
    
    updated_emissions.drop_duplicates(subset=subset_cols, keep='last',
                                      inplace=True)
    updated_emissions.reset_index(drop=True, inplace=True)
    
    # Convert NEI flows back to original case for later flow mapping
    updated_emissions.loc[(updated_emissions['Source']=='NEI') &
                          (updated_emissions['FlowName'].isin([x.capitalize() for x in flow_list])),
                          'FlowName'] = updated_emissions['FlowName'].str.title()
    
    drop_columns = ['operator_name', 'net_generation_megawatthours',
                    'Total Fuel Consumption (MMBtu)', 'Net Efficiency',
                    'Compartment_path', 
                    ]
    drop_columns = [c for c in drop_columns 
                    if c in updated_emissions.columns.values.tolist()]
    updated_emissions.drop(columns=drop_columns, inplace=True)
    return updated_emissions
