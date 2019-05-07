"""
Integration of emissions from data sources that were not included in stewi
"""

import pandas as pd
from electricitylci.coal_upstream import generate_upstream_coal
from electricitylci.model_config import eia_gen_year

# Import functions for other upstream fuels and include them in this list
# with the appropriate input parameters.
UPSTREAM_EMISSION_GENERATORS = [
    generate_upstream_coal(eia_gen_year),
]


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


def combine_upstream_emissions():
    """
    Combine upstream emissions generated from a list of functions. Each function
    generates the upstream emissions from a single fuel type (e.g. coal, NG, etc.)
    as a dataframe.

    All dataframes should include the columns:
        - [ADD COLUMN NAMES]

    Returns
    -------
    DataFrame
        A single dataframe with emissions upstream of the power plant for all
        fuel types.
    """

    upstream = pd.concat(fn for fn in UPSTREAM_EMISSION_GENERATORS)

    return upstream


def add_upstream_emissions(plant_emissions):
    """
    Add upstream emissions to emissions at the power plant. The upstream emissions
    have extra columns.

    - Need a fuel type column
    - Need a life cycle stage column
    - Only for use when exporting to dataframe (no JSON-LD for now)

    New columns:
        - Fuel type? (coal/gas/nuclear/solar/wind, etc)
        - Fuel index code? (e.g. subbit from prb surface)
        - Stage (e.g. extraction, transportation)
        - Stage index? (maybe add dictionary in globals.py to map stage names
        across fuels)
        -
    Parameters
    ----------
    plant_emissions : DataFrame
        Facility-level emissions from each power plant.

    Returns
    -------
    DataFrame
        The combined emissions from power plant and upstream fuel life cycle.
    """

    upstream_emissions = combine_upstream_emissions()

    combined_emissions = pd.concat([plant_emissions, upstream_emissions])

    return combined_emissions
