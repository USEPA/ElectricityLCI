"""
Integration of emissions from data sources that were not included in stewi
"""

import pandas as pd


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
        ReliabilityScore    float64
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
        ReliabilityScore    float64
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
        'ReliabilityScore',
        'Source',
        'Unit',
        'Year',
        'eGRID_ID'
    ]
    assert set(required_cols) == set(updated_emissions.columns)
    
    updated_emissions = pd.concat([stewi_emissions, new_emissions])

    subset_cols = [
        'Compartment', 'FlowName', 'Source',
        'Unit', 'Year', 'eGRID_ID'
    ]
    updated_emissions.drop_duplicates(subset=subset_cols, keep='last',
                                      inplace=True)

    return updated_emissions
