#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# generation.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
from datetime import datetime
import logging
import os

import numpy as np
import pandas as pd
from scipy.stats import t
from scipy.special import erfinv
from scipy.optimize import least_squares
from scipy.stats import uniform

# Presence of 'model_specs' indicates that model configuration occurred.
from electricitylci.model_config import model_specs
from electricitylci.aggregation_selector import subregion_col
import electricitylci.ampd_plant_emissions as ampd
from electricitylci.elementaryflows import map_emissions_to_fedelemflows
from electricitylci.dqi import data_collection_lower_bound_to_dqi
from electricitylci.dqi import lookup_score_with_bound_key
from electricitylci.dqi import technological_correlation_lower_bound_to_dqi
from electricitylci.dqi import temporal_correlation_lower_bound_to_dqi
from electricitylci.eia860_facilities import eia860_balancing_authority
from electricitylci.eia923_generation import build_generation_data
from electricitylci.eia923_generation import eia923_primary_fuel
import electricitylci.emissions_other_sources as em_other
from electricitylci.globals import elci_version
from electricitylci.globals import paths
from electricitylci.globals import output_dir
import electricitylci.manual_edits as edits
from electricitylci.process_dictionary_writer import flow_table_creation
from electricitylci.process_dictionary_writer import process_doc_creation
from electricitylci.process_dictionary_writer import ref_exchange_creator
from electricitylci.process_dictionary_writer import uncertainty_table_creation
from electricitylci.process_dictionary_writer import unit
from electricitylci.utils import make_valid_version_num
from electricitylci.utils import check_output_dir
from electricitylci.utils import write_csv_to_output
from electricitylci.egrid_emissions_and_waste_by_facility import (
    get_combined_stewicombo_file,
)
import facilitymatcher.globals as fmglob  # provided by StEWI


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """A core module of electricityLCI, it combines all the data,
performs all the necessary calculations for different eGRID subregions or other
desired regional aggregation categories, and creates the dictionaries (i.e.,
the LCA inventories but in python dictionary format) and stores them in
computer memory.

CHANGELOG

-   Remove module logger.
-   Remove unused imports.
-   Add missing documentation to methods.
-   Clean up formatting towards PEP8.
-   Note: the uncertainty calculations in :func:`aggregate_data` are
    questionable (see doc strings of submodules for details).
-   Fix the outdated pd.DataFrame.append call in :func:`turn_data_to_dict`
-   Remove :func:`add_flow_representativeness_data_quality_scores` because
    unused.
-   Replace .values with .squeeze().values when calling a data frame with
    only one column of data in :func:`olcaschema_genprocess`.
-   Fix groupby for source_db in :func:`calculate_electricity_by_source` to
    match the filter used to find multiple source entries.
-   Add empty database check in :func:`calculate_electricity_by_source`
-   Separate replace egrid function
-   Fix zero division error in aggregate data
-   Implement Hawkins-Young uncertainty
-   Add uncertainty switch
-   Drop NaNs in exchange table
-   Move FRS file download to its own function

Created:
    2019-06-04
Last edited:
    2025-03-14
"""
__all__ = [
    "add_data_collection_score",
    "add_technological_correlation_score",
    "add_temporal_correlation_score",
    "aggregate_data",
    "aggregate_facility_flows",
    "calculate_electricity_by_source",
    "create_generation_process_df",
    "eia_facility_fuel_region",
    "hawkins_young",
    "hawkins_young_sigma",
    "hawkins_young_uncertainty",
    "olcaschema_genprocess",
    "replace_egrid",
    "turn_data_to_dict",
]


##############################################################################
# FUNCTIONS
##############################################################################
def _calc_sigma(p_series):
    """Calculate the standard deviation for a series of facility emission
    factors.

    Parameters
    ----------
    p_series : pandas.Series
        A series object sent during an aggregation or apply call.

    Returns
    -------
    float
        The fitted sigma for a Hawkins-Young uncertainty method.
        Assumes a 90% confidence level (see :param:`alpha`).
    """
    alpha = 0.9
    if model_specs.calculate_uncertainty:
        (is_error, sigma) = hawkins_young_sigma(p_series.values, alpha)
    else:
        return None

    if is_error:
        return None
    else:
        return sigma


def _calc_geom_params(p_series):
    """Location-adjusted geometric mean and standard deviation based on the
    Hawkins-Young uncertainty method.

    Parameters
    ----------
    p_series : pandas.Series
        A data series for aggregated (or disaggregated) emissions,
        including variables for 'uncertaintySigma' (as calculated
        by :func:`_calc_sigma`), 'Emission_factor' (emission amounts
        per MWh),

    Returns
    -------
    tuple
        Geometric mean : float or NaN
        Geometric standard deviation : float or NaN

    """
    sigma = p_series["uncertaintySigma"]
    ef = p_series["Emission_factor"]

    if sigma is None:
        return (float('nan'), float('nan'))

    d = hawkins_young_uncertainty(ef, sigma, False)
    is_error = d['error']
    if is_error:
        return (float('nan'), float('nan'))
    else:
        return (d['mu_g'], d['sigma_g'])


def _wtd_mean(pdser, total_db):
    """The weighted mean method.

    Parameters
    ----------
    pdser : pandas.Series
        A pandas series of numerical values.
        Examples include correlation and data quality values.
    total_db : pandas.DataFrame
        A data frame with the same indices as the pandas series and
        with a column, 'FlowAmount,' that represents the emission
        amount used as the weighting factor (i.e., higher emissions
        means more contribution towards the average).

    Returns
    -------
    float or nan
        The flow-amount-weighted average of values.
    """
    # HOTFIX: averaging with NaNs in the array.
    non_nans = pdser.values[~np.isnan(pdser.values)]
    if len(non_nans) == 0:
        logging.debug("Encountered a NaN array!")
        result = float("nan")
    else:
        try:
            wts = total_db.loc[
                pdser[~np.isnan(pdser)].index, "FlowAmount"].values
            result = np.average(non_nans, weights=wts)
        except:
            logging.debug(
                f"Error calculating weighted mean for {pdser.name}-"
                f"likely from 0 FlowAmounts"
            )
            try:
                with np.errstate(all='raise'):
                    result = np.average(non_nans)
            except (ArithmeticError, ValueError, FloatingPointError):
                result = float("nan")
    return result


def eia_facility_fuel_region(year):
    """Generate a data frame with EIA 860 and EIA 923 facility data.

    Calculates the percent of facility generation from the primary fuel
    category.

    Parameters
    ----------
    year : int
        The year associated with EIA data.

    Returns
    -------
    pandas.DataFrame
        Facility-level data from EIA Forms 860 and 923.
        Columns include:

        - 'FacilityID' : int
        - 'NAICS Code' : str (though they are integers)
        - 'FuelCategory' : str
        - 'PrimaryFuel' : str
        - 'PercentGenerationfromDesignatedFuelCategory' : float
        - 'State' : str
        - 'NERC' : str
        - 'Balancing Authority Code' : str
        - 'Balancing Authority Name' : str
    """
    logging.info(
        "Generating the percent generation from primary fuel category "
        "for each facility")
    primary_fuel = eia923_primary_fuel(year=year)
    ba_match = eia860_balancing_authority(year)
    primary_fuel["Plant Id"] = primary_fuel["Plant Id"].astype(int)
    ba_match["Plant Id"] = ba_match["Plant Id"].astype(int)
    combined = primary_fuel.merge(ba_match, on='Plant Id')
    combined['primary fuel percent gen'] = (
        combined['primary fuel percent gen'] / 100
    )

    combined.rename(
        columns={
            'primary fuel percent gen': 'PercentGenerationfromDesignatedFuelCategory',
            'Plant Id': 'FacilityID',
            'fuel category': 'FuelCategory',
            'NERC Region': 'NERC',
        },
        inplace=True
    )

    return combined


def add_technological_correlation_score(db):
    """Converts the percent generation from primary fuel to a technological data quality indicator where 1 represents >80 percent.

    See 'technological_correlation_lower_bound_to_dqi' in dqi.py for bounds.

    Parameters
    ----------
    db : pandas.DataFrame
        A data frame with 'PercentGenerationfromDesignatedFuelCategory' column
        with floats that represent the percent of plant generation that
        comes from the primary fuel category (i.e., how much of the generation
        is represented by the primary fuel category).

    Returns
    -------
    pandas.DataFrame
        The same data frame received with new column,
        'TechnologicalCorrelation', that represents the data quality based
        on the primary fuel categorization.
    """
    db['TechnologicalCorrelation'] = db[
        'PercentGenerationfromDesignatedFuelCategory'].apply(
            lambda x: lookup_score_with_bound_key(
                x, technological_correlation_lower_bound_to_dqi)
        )
    return db


def add_temporal_correlation_score(years, electricity_lci_target_year):
    """Generates columns in a data frame for data age and its quality score.
    
    3/17/25 MBJ - changing this to take a dataseries containing years
    and return a dataseries that should have the same
    index as the input data series. There's no need to maintain an "Age" column in the
    dataframe, and I think this is also makes the changes to the dataframe
    a bit more transparent. There may also be times where we would prefer
    this temporal correlation to be determined by somthing other than the 
    "Year" column in a complete dataframe.

    Parameters
    ----------
    years : pandas.DataSeries
        A data series representing the data source year.
    electricity_lci_target_year : int
        The year associated with data use (see model_config attribute,
        'electricity_lci_target_year').

    Returns
    -------
    pandas.DataSeries
        - 'TemporalCorrelation' : int, DQI score based on age.
    """
    # Could be more precise here with year
    age_dataseries =  electricity_lci_target_year - pd.to_numeric(years)
    TemporalCorrelation = age_dataseries.apply(
        lambda x: lookup_score_with_bound_key(
            x, temporal_correlation_lower_bound_to_dqi))
    return TemporalCorrelation


def aggregate_facility_flows(df):
    """Aggregate flows from the same source (e.g., netl) within a facility.

    The main problem this solves is that if several emissions are mapped to a
    single federal elementary flow (e.g., CO2 biotic or CO2 land use change)
    then those show up as separate emissions in the inventory and artificially
    inflate the number of emissions for uncertainty calculations.

    This method sums all duplicated emissions together (taking the average of
    their data quality indicators).

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with facility-level emissions that might contain duplicate
        emission species within the facility.

    Returns
    -------
    pandas.DataFrame
        The same data frame sent with duplicated emissions aggregated to a
        single row.
    """
    emission_compartments = [
        "emission/air",
        "emission/water",
        "emission/ground",
        "emission/soil",
        "air",
        "water",
        "soil",
        "ground",
        "waste",
    ]
    groupby_cols = [
        "FuelCategory",
        "Electricity",
        "FlowName",
        "Source",
        "Compartment",
        "stage_code"
    ]
    if "FacilityID" in df.columns:
        groupby_cols=groupby_cols+["FacilityID"]
    elif "eGRID_ID" in df.columns:
        groupby_cols=groupby_cols+["eGRID_ID"]
    else:
        logging.error(
            "generation.aggregate_facility_flows: Missing eGRID_ID and " \
            "FacilityID in dataframe"
        )

    wm = lambda x: _wtd_mean(x, df)
    emissions = df["Compartment"].isin(emission_compartments)
    df_emissions = df[emissions]
    df_nonemissions = df[~emissions]
    df_dupes = df_emissions.duplicated(subset=groupby_cols, keep=False)
    df_red = df_emissions.drop(df_emissions[df_dupes].index)
    group_db = df_emissions.loc[df_dupes, :].groupby(
        groupby_cols, as_index=False
    ).agg({
        "FlowAmount": "sum",
        "DataReliability": wm
    })

    group_db_merge = group_db.merge(
        right=df_emissions.drop_duplicates(subset=groupby_cols),
        on=groupby_cols,
        how="left",
        suffixes=("", "_right"),
    )
    try:
        delete_cols = ["FlowAmount_right", "DataReliability_right"]
        group_db_merge.drop(columns=delete_cols, inplace=True)
    except KeyError:
        logging.debug("Failed to drop columns.")
        pass
    df = pd.concat(
        [df_nonemissions, df_red, group_db_merge],
        ignore_index=True
    )

    return df


def _combine_sources(p_series, df, cols, source_limit=None):
    """Take the sources from a groupby.apply and return a list that
    contains one column containing a list of the sources and another
    that concatenates them into a string. This is all in an effort to find
    another approach for summing electricity for all plants in an aggregation
    that matches the same data sources.

    Parameters
    ----------
    p_series : pandas.Series
        A column of source strings from inventory data frame.
    df: pandas.DataFrame
        Dataframe containing merged generation and emissions data; it includes
        a column for data source (e.g., eGRID, NEI, and RCRAInfo).
    cols : list
        Unused column list, except for debugging statement.
    source_limit : int, optional
        The maximum number of sources allowed to be found.
        Defaults to none.

    Returns
    ----------
    list
        A list of length two.

        1. The first item is a list of all sources or nan.
        2. The second item is a string of concatenated sources or nan.
    """
    logging.debug(
        f"Combining sources for {str(df.loc[p_series.index[0],cols].values)}"
    )
    source_list = list(np.unique(p_series))
    if source_limit is not None:
        if len(source_list) > source_limit:
            result = [float("nan"), float("nan")]
            return result
        else:
            source_list.sort()
            source_list_string = "_".join(source_list)
            result = [source_list, source_list_string]
            return result
    else:
        source_list.sort()
        # HOTFIX: rm redundant calls [2023-11-08; TWD]
        source_list_string = "_".join(source_list)
        result = [source_list, source_list_string]
        return result


def add_data_collection_score(db, elec_df, subregion="BA"):
    """Add the data collection score.

    This is a function of how much of the total electricity generated in a
    subregion is captured by the denominator used in the final emission factor.

    Parameters
    ----------
    db : datafrane
        Dataframe containing facility-level emissions as generated by
        create_generation_process_df.
    elec_df : dataframe
        Dataframe containing the totals for various subregion/source
        combinations. These are used as the denominators in the emissions
        factors
    subregion : str, optional
        The level of subregion that the data will be aggregated to. Choices
        are 'all', 'NERC', 'BA', 'US', by default 'BA'
    """
    logging.info("Adding data collection score")
    region_agg = subregion_col(subregion)
    fuel_agg = ["FuelCategory"]
    if region_agg:
        groupby_cols = region_agg + fuel_agg + ["Year"]
    else:
        groupby_cols = fuel_agg + ["Year"]
    temp_df = db.merge(
        right=elec_df,
        left_on=groupby_cols + ["source_string"],
        right_on=groupby_cols + ["source_string"],
        how="left",
    )
    reduced_db = db.drop_duplicates(subset=groupby_cols + ["eGRID_ID"])
    region_elec = reduced_db.groupby(groupby_cols, as_index=False)[
        "Electricity"
    ].sum()
    region_elec.rename(
        columns={"Electricity": "region_fuel_electricity"}, inplace=True
    )
    temp_df = temp_df.merge(
        right=region_elec,
        left_on=groupby_cols,
        right_on=groupby_cols,
        how="left",
    )
    db["Percent_of_Gen_in_EF_Denominator"] = (
        temp_df["electricity_sum"] / temp_df["region_fuel_electricity"]
    )
    db["DataCollection"] = db["Percent_of_Gen_in_EF_Denominator"].apply(
        lambda x: lookup_score_with_bound_key(
            x, data_collection_lower_bound_to_dqi
        )
    )
    db = db.drop(columns="Percent_of_Gen_in_EF_Denominator")
    return db


def calculate_electricity_by_source(db, subregion="BA"):
    """Calculate the electricity totals by region and source.

    This method uses the same approach as the original generation.py with
    attempts made to speed it up. Each flow will have a source associated
    with it (eGRID, NEI, TRI, RCRAInfo). To develop an emission factor,
    the FlowAmount will need to be divided by electricity generation.
    This routine sums all electricity generation for all source/subregion
    combinations. So if a subregion aggregates FlowAmounts source from NEI and
    TRI then the denominator will be all production from plants that reported
    into NEI or TRI for that subregion.

    Parameters
    ----------
    db : pandas.DataFrame
        Dataframe containing facility-level emissions as generated by
        create_generation_process_df.
    subregion : str, optional
        The level of subregion that the data will be aggregated to. Choices
        are 'all', 'NERC', 'BA', 'US', by default 'BA'

    Returns
    -------
    tuple
        pandas.DataFrame :
            Inventory dataframe with source list and source string fields.
        pandas.DataFrame :
            The calculation of average and total electricity for each source
            along with the facility count for each source.
    """
    all_sources = '_'.join(sorted(list(db["Source"].unique())))

    # HOTFIX: not separating the data frame in hopes of generating electricity
    # amounts for fuel inputs that doesn't make the plants "too efficient"
    # [2024-08-14 MBJ]
    db_powerplant = db.copy()

    region_agg = subregion_col(subregion)

    fuel_agg = ["FuelCategory"]
    if region_agg:
        groupby_cols = (
            region_agg
            + fuel_agg
            + ["Year", "stage_code", "FlowName", "Compartment"]
        )
        elec_groupby_cols = region_agg + fuel_agg + ["Year"]
    else:
        groupby_cols = fuel_agg + [
            "Year",
            "stage_code",
            "FlowName",
            "Compartment",
        ]
        elec_groupby_cols = fuel_agg + ["Year"]

    # HOTFIX: add check for empty power plant data frame [2023-12-19; TWD]
    if len(db_powerplant) == 0:
        db_cols = list(db_powerplant.columns) + ['source_list', 'source_string']
        db_powerplant = pd.DataFrame(columns=db_cols)
    else:
        # This is a pretty expensive process when we have to start looking
        # at each flow generated in each compartment for each balancing
        # authority area. To hopefully speed this up, we'll group by FlowName
        # and Compartment and look and try to eliminate flows where all
        # sources are single entities.
        combine_source_by_flow = lambda x: _combine_sources(
            x, db, ["FlowName", "Compartment"], 1
        )
        # Find all single-source flows (all multiple sources are nans)
        source_df = pd.DataFrame(
            db_powerplant.groupby(["FlowName", "Compartment"])[
                ["Source"]].apply(combine_source_by_flow),
            columns=["source_list"],
        )
        source_df[["source_list", "source_string"]] = pd.DataFrame(
            source_df["source_list"].values.tolist(),
            index=source_df.index
        )
        source_df.reset_index(inplace=True)
        old_index = db_powerplant.index
        db_powerplant = db_powerplant.merge(
            right=source_df,
            left_on=["FlowName", "Compartment"],
            right_on=["FlowName", "Compartment"],
            how="left",
        )
        db_powerplant.index = old_index

        # Filter out single flows; leaving only multi-flows
        db_multiple_sources = db_powerplant.loc[
            db_powerplant["source_string"].isna(), :].copy()
        if len(db_multiple_sources) > 0:
            combine_source_lambda = lambda x: _combine_sources(
                x, db_multiple_sources, groupby_cols
            )
            # HOTFIX: it doesn't make sense to groupby a different group;
            # it gives different results from the first-pass filter;
            # changed to match criteria above. [2023-12-19; TWD]
            # HOTFIX undone [2024-08-13; MBJ]
            source_df = pd.DataFrame(
                db_multiple_sources.groupby(groupby_cols)[
                    ["Source"]].apply(combine_source_lambda),
                columns=["source_list"],
            )
            source_df[["source_list", "source_string"]] = pd.DataFrame(
                source_df["source_list"].values.tolist(),
                index=source_df.index
            )
            source_df.reset_index(inplace=True)
            db_multiple_sources.drop(
                columns=["source_list", "source_string"], inplace=True
            )
            old_index = db_multiple_sources.index
            db_multiple_sources = db_multiple_sources.merge(
                right=source_df,
                left_on=groupby_cols,
                right_on=groupby_cols,
                how="left",
            )
            db_multiple_sources.index = old_index
            db_powerplant.loc[
                db_powerplant["source_string"].isna(),
                ["source_string", "source_list"]
            ] = db_multiple_sources[["source_string", "source_list"]]
    unique_source_lists = list(db_powerplant["source_string"].unique())
    unique_source_lists = [x for x in unique_source_lists if str(x) != "nan"]
    unique_source_lists += [all_sources]
    # One set of emissions passed into this routine may be life cycle emissions
    # used as proxies for Canadian generation. In those cases the electricity
    # generation will be equal to the Electricity already in the dataframe.
    elec_sum_lists = list()
    for src in unique_source_lists:
        logging.info(f"Calculating electricity for {src}")
        db["temp_src"] = src
        src_filter = [
            a in b
            for a, b in zip(
                db["Source"].values.tolist(), db["temp_src"].values.tolist()
            )
        ]
        sub_db = db.loc[src_filter, :].copy()
        sub_db.drop_duplicates(subset=fuel_agg + ["eGRID_ID","Year"], inplace=True)
        # HOTFIX: fix pandas futurewarning syntax [2024-03-08; TWD]
        sub_db_group = sub_db.groupby(elec_groupby_cols, as_index=False).agg(
            {"Electricity": ["sum", "mean"], "eGRID_ID": "count"}
        )
        sub_db_group.columns = elec_groupby_cols + [
            "electricity_sum",
            "electricity_mean",
            "facility_count",
        ]
        sub_db_group["source_string"] = src
        elec_sum_lists.append(sub_db_group)
    elec_sums = pd.concat(elec_sum_lists, ignore_index=True)
    elec_sums.sort_values(by=elec_groupby_cols, inplace=True)
    db = db_powerplant

    return db, elec_sums


def get_generation_years():
    """Create list of generation years based on model configuration.

    Reads the model specs for inventories of interest, generation year,
    and (if renewables are included) the hydro power plant data year.

    Returns
    -------
    list
        A list of years (int)
    """
    generation_years = [model_specs.eia_gen_year]
    # Check to see if hydro power plant data are used (always 2016)
    if model_specs.include_renewable_generation is True:
        generation_years += [2016]
    # Add years of inventories of interest; remove duplicates, and
    # sort chronologically:
    generation_years = sorted(list(set(
        list(model_specs.inventories_of_interest.values())
        + generation_years
    )))

    return generation_years


def get_facilities_w_fuel_region(years=None):
    """Capture all facility fuels and regions for a given set of years.

    Parameters
    ----------
    years : list, optional
        List of years, by default None

    Returns
    -------
    pandas.DataFrame
        A data frame with columns

        - 'FacilityID' (int): plant identifier
        - 'FuelCategory' (str): primary fuel category
        - 'PrimaryFuel' (str): primary fuel code
        - 'PercentGenerationfromDesignatedFuelCategory' (float)
        - 'State' (str): two-character state code
        - 'NERC' (str): NERC region code
        - 'Balancing Authority Code' (str)
        - 'Balancing Authority Name' (str)
    """
    if years is None:
        years = get_generation_years()

    if isinstance(years, (int, float, str)):
        years = [years,]

    for i in range(len(years)):
        year = years[i]
        if i == 0:
            a = eia_facility_fuel_region(year)
        else:
            b = eia_facility_fuel_region(year)
            # This appends a suffix on the old data, and gap fills
            # new data with old data that are not found in the new data.
            # Source: https://stackoverflow.com/a/69504041
            a = a.merge(
                b,
                how='outer',
                on='FacilityID',
                suffixes=('_df1', '')
            )
            for col_name in b.columns:
                new_name = col_name + "_df1"
                if new_name in a.columns:
                    # Fill in new column's NaNs with old data:
                    a[col_name] = a[col_name].fillna(a[new_name])
                    a.drop(columns=new_name, inplace=True)

    return a


def create_generation_process_df():
    """Read emissions and generation data from different sources to provide
    facility-level emissions. Most important inputs to this process come
    from the model configuration file.

    Maps balancing authorities to FERC and EIA regions.

    Returns
    ----------
    pandas.DataFrame
        Data frame includes all facility-level emissions.
    """
    from electricitylci.combinator import BA_CODES

    COMPARTMENT_DICT = {
        "emission/air": "air",
        "emission/water": "water",
        "emission/ground": "ground",
        "input": "input",
        "output": "output",
        "waste": "waste",
        "air": "air",
        "water": "water",
        "ground": "ground",
        "emission/air/troposphere/rural/ground-level": "air",
    }
    if model_specs.replace_egrid:
        # Create data frame with EIA's info on:
        # - 'FacilityID' (int),
        # - 'Electricity' (float), and
        # - 'Year' (int)
        # NOTE: this may return multi-year facilities
        generation_data = build_generation_data().drop_duplicates()

        # Pull list of unique facilities from all generation years of interest
        eia_facilities_to_include = generation_data["FacilityID"].unique()

        # Create the file name for reading/writing facility matcher data.
        inventories_of_interest_list = sorted([
            f"{x}_{model_specs.inventories_of_interest[x]}"
            for x in model_specs.inventories_of_interest.keys()
        ])
        inventories_of_interest_str = "_".join(inventories_of_interest_list)

        # Define the folder, and make sure it exists; and the CSV file
        frs_dir = os.path.join(paths.local_path, "FRS_bridges")
        check_output_dir(frs_dir)
        inventories_of_interest_csv = os.path.join(
            frs_dir,
            inventories_of_interest_str + ".csv"
        )

        # NOTE: data pulled from Facility Register Service (FRS) program
        # provided by USEPA's FacilityMatcher, now a part of StEWI.
        # https://github.com/USEPA/standardizedinventories
        try:
            eia860_FRS = pd.read_csv(inventories_of_interest_csv)
            logging.info(
                "Reading EIA860 to FRS ID matches from existing file")
            eia860_FRS["REGISTRY_ID"] = eia860_FRS["REGISTRY_ID"].astype(str)
        except FileNotFoundError:
            logging.info(
                "Will need to load EIA860 to FRS matches using stewi "
                "facility matcher - it may take a while to download "
                "and read the required data")
            eia860_FRS = read_stewi_frs(inventories_of_interest_csv, True)

        # emissions_and_wastes_by_facility is a StEWICombo inventory based on
        # inventories of interest (e.g., eGRID, RCRAInfo, NEI) and their
        # respective years as defined in the model config.
        # Columns in the emissions_and_wastes_by_facility include
        # FacilityID and FRS_ID (the latter links to REGISTRY_ID in FRS).
        # This effectively adds 'PGM_SYS_ID', which are the EIA facility
        # numbers and maps them to eGRID facility numbers.
        # NOTE: there are unmatched facilities that are found in FRS_bridge,
        # but not in EIA (e.g., EGRID, RCRA).
        emissions_and_wastes_by_facility = get_combined_stewicombo_file(model_specs)
        ewf_df = pd.merge(
            left=emissions_and_wastes_by_facility,
            right=eia860_FRS,
            left_on="FRS_ID",
            right_on="REGISTRY_ID",
            how="left",
        )

        # Effectively removes all non-EIA facilities from StEWICombo inventory.
        #   drops 909 rows in 2022 inventory
        ewf_df.dropna(subset=["PGM_SYS_ID"], inplace=True)

        # Drop unused columns; note legacy column names are still here.
        d_cols = [
            "NEI_ID",
            "FRS_ID",
            "TRI_ID",
            "RCRAInfo_ID",
            "PGM_SYS_ACRNM",
            "REGISTRY_ID"
        ]
        d_cols = [x for x in d_cols if x in ewf_df.columns]
        if len(d_cols) > 0:
            ewf_df.drop(columns=d_cols, inplace=True)

        # Convert facility ID to integer for comparisons.
        ewf_df["FacilityID"] = ewf_df["PGM_SYS_ID"].astype(int)

        # Filter stewi inventory to just (EIA) facilities of interest.
        # HOTFIX: SettingWithCopyWarning [2024-03-12; TWD]
        eaw_for_select_eia_facilities = ewf_df[
            ewf_df["FacilityID"].isin(eia_facilities_to_include)].copy()
        # HOTFIX: "eGRID_ID" column already appears
        if "eGRID_ID" in eaw_for_select_eia_facilities.columns:
            eaw_for_select_eia_facilities.drop(columns="eGRID_ID", inplace=True)
        eaw_for_select_eia_facilities.rename(
            columns={"FacilityID": "eGRID_ID"}, inplace=True)

        # Read in EPA's CEMS state-level data
        # NOTE: reads in all facility data, including those 99999 facilities
        # that were filtered out w/ NAICS code filtering. These facilities
        # are re-filtered later on during a merge with generation data.
        cems_df = ampd.generate_plant_emissions(model_specs.eia_gen_year)

        # Correct StEWI emissions
        emissions_df = em_other.integrate_replace_emissions(
            cems_df, eaw_for_select_eia_facilities
        )

        # Read EIA 860/923 facility info (e.g., PrimaryFuel and percent of
        # generation from designated fuel category).
        # HOTFIX: gather "the best" facility fuel and location data across
        #   all inventory years [240809; TWD].
        facilities_w_fuel_region = get_facilities_w_fuel_region()
        facilities_w_fuel_region.rename(
            columns={'FacilityID': 'eGRID_ID'},
            inplace=True
        )
    else:
        # Load list; only works when not replacing eGRID!
        from electricitylci.generation_mix import egrid_facilities_w_fuel_region
        from electricitylci.egrid_filter import (
            electricity_for_selected_egrid_facilities,
            emissions_and_waste_for_selected_egrid_facilities,
        )

        # HOTFIX: avoid overwriting the global variable by using a copy
        # NOTE: egrid_facilities_with_fuel_region is the same as
        # egrid_facilities
        facilities_w_fuel_region = egrid_facilities_w_fuel_region.copy()
        facilities_w_fuel_region["FacilityID"] = \
            facilities_w_fuel_region["FacilityID"].astype(int)
        facilities_w_fuel_region.rename(
            columns={'FacilityID': 'eGRID_ID'},
            inplace=True)

        generation_data = electricity_for_selected_egrid_facilities.copy()
        generation_data["Year"] = model_specs.egrid_year
        generation_data["FacilityID"] = \
            generation_data["FacilityID"].astype(int)

        emissions_df = emissions_and_waste_for_selected_egrid_facilities.copy()
        emissions_df["eGRID_ID"] = emissions_df["eGRID_ID"].astype(int)

    # HOTFIX: ValueError w/ Year as string and integer [2023-12-22; TWD]
    emissions_df['Year'] = emissions_df['Year'].astype(int)
    generation_data['Year'] = generation_data['Year'].astype(int)
    generation_data.rename(columns={'FacilityID': 'eGRID_ID'}, inplace=True)

    # Match electricity generation data (generation_data) to their facility
    # emissions inventory (emissions_df) by year.
    # HOTFIX: Change how to 'inner' to ensure that plants that have been
    # filtered out are not included (e.g., by NAICS) [3/4/2024; M. Jamieson]
    final_database = pd.merge(
        left=emissions_df,
        right=generation_data,
        on=["eGRID_ID", "Year"],
        how="inner",
    )

    # Add facility-level info to the emissions and generation data.
    # NOTE some failed-to-match facilities with location exist.
    #   This is likely due to 'facilities_w_fuel_region' being associated with
    #   the EIA generation year, whilst the data are from several vintages.
    final_database = pd.merge(
        left=final_database,
        right=facilities_w_fuel_region,
        on="eGRID_ID",
        how="left",
        suffixes=["", "_right"],
    )

    if model_specs.replace_egrid:
        # Get EIA primary fuel categories (and their percent generation);
        # The data are the same as from EIA's `eia_facility_fuel_region`,
        # but with additional facilities.
        primary_fuel_df = eia923_primary_fuel(year=model_specs.eia_gen_year)
        primary_fuel_df.rename(
            columns={'Plant Id': "eGRID_ID"},
            inplace=True
        )
        primary_fuel_df["eGRID_ID"] = primary_fuel_df["eGRID_ID"].astype(int)
        # Produce a data frame of plant ID to fuel category for mapping
        # NOTE: drop duplicates should not be necessary;
        #   passed checks 2016, 2020, 2022 [240809; TWD]
        key_df = (
            primary_fuel_df[["eGRID_ID", "FuelCategory"]]
            .dropna().drop_duplicates().set_index("eGRID_ID")
        )
        # Fills some, but not all.
        final_database["FuelCategory"] = final_database["eGRID_ID"].map(
            key_df["FuelCategory"])
    else:
        # Attempt to use facility data to match NaNs.
        key_df = (
            final_database[["eGRID_ID", "FuelCategory"]]
            .dropna().drop_duplicates().set_index("eGRID_ID")
        )
        final_database.loc[
            final_database["FuelCategory"].isnull(), "FuelCategory"
        ] = final_database.loc[
            final_database["FuelCategory"].isnull(), "eGRID_ID"
            ].map(
                key_df["FuelCategory"]
            )

    final_database["Final_fuel_agg"] = final_database["FuelCategory"]
    if 'Year_x' in final_database.columns:
        year_filter = final_database["Year_x"] == final_database["Year_y"]
        final_database = final_database.loc[year_filter, :]
        final_database.drop(columns="Year_y", inplace=True)
        final_database.rename(columns={"Year_x": "Year"}, inplace=True)

    # Use the Federal Elementary Flow List (FEDEFL) to map flow UUIDs
    # NOTE: 10,000 unmatched flows; mostly wastes and product flows
    final_database = map_emissions_to_fedelemflows(final_database)

    # Sanity check that no duplicated columns exist in the data frame.
    final_database = final_database.loc[
        :, ~final_database.columns.duplicated()
    ]

    # Sanity check that no duplicate emission rows are in the data frame.
    dup_cols_check = [
        "eGRID_ID",
        "FuelCategory",
        "FlowName",
        "FlowAmount",
        "Compartment",
    ]
    final_database = final_database.drop_duplicates(subset=dup_cols_check)

    drop_columns = ['PrimaryFuel_right', 'FuelCategory', 'FuelCategory_right']
    drop_columns = [c for c in drop_columns if c in final_database.columns]
    final_database.drop(columns=drop_columns, inplace=True)
    final_database.rename(
        columns={"Final_fuel_agg": "FuelCategory"},
        inplace=True,
    )

    # Add DQI
    final_database["TemporalCorrelation"] = add_temporal_correlation_score(
        final_database["Year"], model_specs.electricity_lci_target_year)
    final_database = add_technological_correlation_score(final_database)
    final_database["DataCollection"] = 5
    final_database["GeographicalCorrelation"] = 1

    # For surety's sake
    final_database["eGRID_ID"] = final_database["eGRID_ID"].astype(int)

    # Organize database by facility, then by emission compartment
    # (e.g., resource), then by flow name.
    final_database.sort_values(
        by=["eGRID_ID", "Compartment", "FlowName"],
        inplace=True
    )

    # Add more metadata
    final_database["stage_code"] = "Power plant"
    final_database["Compartment_path"] = final_database["Compartment"]
    final_database["Compartment"] = final_database["Compartment_path"].map(
        COMPARTMENT_DICT
    )

    # NOTE: there are fewer BA names than codes in final_database!
    final_database["Balancing Authority Name"] = final_database[
        "Balancing Authority Code"].map(BA_CODES["BA_Name"])
    final_database["EIA_Region"] = final_database[
        "Balancing Authority Code"].map(BA_CODES["EIA_Region"])
    final_database["FERC_Region"] = final_database[
        "Balancing Authority Code"].map(BA_CODES["FERC_Region"])

    # Apply the "manual edits"
    # See GitHub issues #212, #160, #121, and #77.
    # https://github.com/USEPA/ElectricityLCI/issues/
    final_database = edits.check_for_edits(
        final_database, "generation.py", "create_generation_process_df")

    return final_database


def hawkins_young(x, **kwargs):
    """The uncertainty model to be minimized.

    Parameters
    ----------
    x : int or float
        The guessed value of sigma.
    kwargs: dict
        Optional keyword arguments, including:

        - 'alpha' (float): The confidence level (e.g., 0.9 for 90%)
        - 'cui' (float): The confidence upper interval value

    Returns
    -------
    float
        The fitted value of sigma.

    Notes
    -----
    From Young et al. (2019) <https://doi.org/10.1021/acs.est.8b05572>,
    to ensure non-negative releases in Monte Carlo simulations, the error
    is set to a log-normal distribution with the expected value assigned
    to the emission factor (EF) and the 95th percentile of the cumulative
    distribution function (CDF) set to the 90% confidence interval upper
    limit (CIU).

    Based on the CDF for lognormal distribution, D(x), set to 0.95 for
    x = EF*(1+PI), the 90% CIU based on a given emission factor, EF, and
    prediction/confidence interval expressed as a fraction (or percentage);
    hence the 1+CIU. If CIU is undefined, a default value of 50% is used.
    """
    if 'alpha' in kwargs.keys():
        alpha = kwargs['alpha']
    else:
        alpha = 0.9

    if 'ciu' in kwargs.keys():
        ciu = kwargs['ciu']
    else:
        ciu = 0.5

    a = 0.5
    z = erfinv(alpha)
    b = -2**0.5*z
    c = np.log(1 + ciu)
    r = a*x**2 + b*x + c

    return r


def hawkins_young_sigma(data, alpha):
    """Model a log-normal uncertainty distribution to a dataset.

    This method does not fit a log-normal distribution to the given data!

    Parameters
    ----------
    data : numpy.array
        A data array.
    alpha : float
        The confidence level, expressed as a fraction
        (e.g., 90% confidence = 0.9).

    Returns
    -------
    tuple
        A tuple of length two: error boolean and sigma (the standard deviation
        of the normally distributed values of Y = log(X)).

    Notes
    -----
    From Young et al. (2019) <https://doi.org/10.1021/acs.est.8b05572>,
    the prediction interval is expressed as the percentage of the expected
    release factor; Eq 3. expresses it as
    :math:`P = s * sqrt(1 + 1/n)*z/y_hat`
    where:
      s is the standard error of the expected value, SEM;
      n is the sample size;
      z is the critical value for 90% confidence; and
      y_hat is the expected value.
    """
    # Note that there is no assumed log-normal distribution here.
    # HOTFIX nans in z and ciu calcs [2024-05-14; TWD]
    is_error = True
    n = len(data)
    z = 0.0
    if n > 1:
        is_error = False
        z = t.ppf(q=alpha, df=n-1)
    se = np.std(data)/np.sqrt(n)
    y_hat = data.mean()
    ciu = 0.0
    if y_hat != 0:
        ciu = se*np.sqrt(1 + 1/n)*z/y_hat
    if ciu <= -1:
        is_error = True
        ciu = -9.999999e-1  # makes log(0.0000001) in hawkins_young

    # Use least-squares fitting for the quadratic.
    # NOTE: remember, we are fitting sigma, the standard deviation of the
    #       underlying normal distribution. A 'safe' assumption is to
    #       expect sigma to be between 1 and 5. So run a few fits and
    #       get the one that isn't negative (most positive).
    #       Alternatively, we could take std(ddof=1) of the log of the data
    #       to get an estimate of the standard deviation and search across
    #       4x's of it. See snippet code for method:
    #       `s_std = np.round(4*np.log(data).std(ddof=1), 0)`
    all_ans = []
    for i in uniform.rvs(0, 6, size=10):
        ans = least_squares(
            hawkins_young, i, kwargs={'alpha': alpha, 'ciu': ciu})
        all_ans.append(ans['x'][0])

    # Find the minimum of all positive values:
    all_ans = np.array(all_ans)
    sigma = all_ans[np.where(all_ans > 0)].min()

    return (is_error, sigma)


def hawkins_young_uncertainty(ef, sigma, is_error):
    """Compute the log-normal distribution parameters.

    The (geometric) mean and (geometric) standard deviation are fitted to an
    assumed distribution that has an expected value of the emission factor,
    `ef`, and the 95th percentile at the 90% confidence upper interval.

    This modeled log-normal distribution is for use with Monte-Carlo
    simulations to guarantee non-negative emission values with an expected
    value that matches a given emission factor.

    Parameters
    ----------
    ef : float
        Emission factor (emission units/MWh).
    sigma : float
        Fitted standard deviation to emissions data.
    is_error : bool
        The error flag returned from :func:`hawkins_young_sigma`.

    Returns
    -------
    dict
        A dictionary of results. Keys include the following.

        -   'mu' (float): The mean of a normally distributed values of
            Y = log(X)
        -   'sigma' (float): The standard deviation of the normally distributed
            values of Y = log(X)
        -   'mu_g' (float): The geometric mean for the log-normal distribution.
        -   'sigma_g' (float): The geometric standard deviation for the
            log-normal distribution.
        -   'error' (bool): Whether the method failed (e.g., too few data
            points, ci < -1, ef < 0). To be used to quality check results.
    """
    if ef <= 0:
        is_error = True
        mu = np.nan
    else:
        mu = np.log(ef) - 0.5*sigma**2

    mu_g = np.exp(mu)
    sigma_g = np.exp(sigma)

    return {
        'mu': mu,
        'sigma': sigma,
        'mu_g': mu_g,
        'sigma_g': sigma_g,
        'error': is_error,
    }


def aggregate_data(total_db, subregion="BA"):
    """Aggregate facility-level emissions to the specified subregion and
    calculate emission factors based on the total emission and total
    electricity generation.

    Notes
    -----
    1.  This method performs eGRID primary fuel replacement, which is more
        "data correction" than aggregation.
    2.  The DQI for DataCollection is negative where emission factor is
        negative, which does not make sense for a DQI.

    Parameters
    ----------
    total_db : pandas.DataFrame
        Facility-level emissions as generated by create_generation_process_df
    subregion : str, optional
        The level of subregion that the data will be aggregated to. Choices
        are 'all', 'NERC', 'BA', 'US', by default 'BA'.

    Returns
    -------
    pandas.DataFrame
        The dataframe provides emissions aggregated to the specified
        subregion for each technology and stage found in the input data.
        In addition to the aggregated emissions (FlowAmount), the output
        dataframe also contains a facility count (i.e., the number of
        individual facilities contributing to a given emission) and an
        average emission factor with units of emissions in units (Unit)
        per MWh of total electricity generation within the given region
        and for the specified fuel type. Regions that have no net
        electricity generation (i.e., electricity_sum == 0), are assigned
        a zero emission factor, even if there are positive flow amounts.

        Computed columns include:

        - 'source_string' (str): underscore-delimited data sources
        - 'TemporalCorrelation' (float): DQI weighted value between 1-5
        - 'TechnologicalCorrelation' (float): DQI weighted value between 1-5
        - 'GeographicalCorrelation' (float): DQI weighted value between 1-5
        - 'DataCollection' (float): DQI weighted value between 1-5
        - 'DataReliability' (float): DQI weighted value between 1-5
        - 'uncertaintyMin' (float): min of facility-level emission factor
        - 'uncertaintyMax' (float): max of facility-level emission factor
        - 'uncertaintySigma' (float): standard deviation of flow amounts
        - 'electricity_sum' (float): aggregated electricity gen (MWh)
        - 'electricity_mean' (float): mean electricity gen (MWh)
        - 'facility_count' (float): count of facilities for electricity stats
        - 'Emission_factor' (float): emission amount per MWh
        - 'GeomMean' (float): geometric mean of emission factor (units/MWh)
        - 'GeomSD' (float): geometric standard deviation of emission factor
    """
    from electricitylci.combinator import remove_mismatched_inventories
    region_agg = subregion_col(subregion)
    fuel_agg = ["FuelCategory"]
    if region_agg:
        groupby_cols = (
            region_agg
            + fuel_agg
            + ["stage_code", "FlowName", "Compartment", "FlowUUID", "Unit"]
        )
        # NOTE: datatypes should be str, str, int, str
        elec_df_groupby_cols = (
            region_agg + fuel_agg + ["Year", "source_string"]
        )
    else:
        groupby_cols = fuel_agg + [
            "stage_code",
            "FlowName",
            "Compartment",
            "FlowUUID",
            "Unit"
        ]
        # NOTE: datatypes should be str, int, str
        elec_df_groupby_cols = fuel_agg + ["Year", "source_string"]

    # Replace primary fuel categories based on EIA Form 923, if requested
    if model_specs.replace_egrid:
        total_db = replace_egrid(total_db, model_specs.eia_gen_year)

    #USEPA Issue #282. After replacing primary fuel categories with EIA data,
    #in rare instances there will be renewable O&M emissions assigned to plants
    #of conflicting fuel types - like solar O&M emissions assigned to coal plants.
    #This should filter those out, along with remove mismatched construction
    #inputs.
    total_db = remove_mismatched_inventories(total_db)
    # Use a dummy UUID to avoid groupby errors
    total_db["FlowUUID"] = total_db["FlowUUID"].fillna(value="dummy-uuid")

    # Aggregate multiple emissions of the same type
    logging.info("Aggregating multiples of plant emissions")
    sz_tdb = len(total_db)
    total_db = aggregate_facility_flows(total_db)
    logging.debug("Reduce data from %d to %d rows" % (sz_tdb, len(total_db)))

    # Calculate electricity totals by region and source
    total_db, electricity_df = calculate_electricity_by_source(
        total_db, subregion
    )

    # Assign data score based on percent generation
    total_db = add_data_collection_score(total_db, electricity_df, subregion)

    # Calculate the facility-level emission factor (E/MWh)
    # HOTFIX ZeroDivisionError [2024-05-14; TWD]
    crit_zero = total_db["Electricity"] != 0
    total_db.loc[crit_zero, "facility_emission_factor"] = (
        total_db.loc[crit_zero, "FlowAmount"]
        / total_db.loc[crit_zero, "Electricity"]
    )
    # Effectively removes rows with zero Electricity or nan flow amounts.
    #  For 2016 generation, it's all caused by nans in flow amounts.
    #  For 2020 generation, it's all zero electricity.
    #  For 2022 generation, it's a mixed bag.
    total_db.dropna(subset=["facility_emission_factor"], inplace=True)

    # Define the weighted mean function, which relies on the full database
    # for flow amounts (i.e., the flow-amount weighted method)
    wm = lambda x: _wtd_mean(x, total_db)

    info_txt = "Aggregating flow amounts and dqi information"
    if model_specs.calculate_uncertainty:
        info_txt += ", calculating uncertainty"
    logging.info(info_txt)
    database_f3 = total_db.groupby(
        groupby_cols + ["Year", "source_string"], as_index=False
    ).agg({
        "FlowAmount": ["sum", "count"],
        "TemporalCorrelation": wm,
        "TechnologicalCorrelation": wm,
        "GeographicalCorrelation": wm,
        "DataCollection": wm,
        "DataReliability": wm,
        "facility_emission_factor": ["min", "max", _calc_sigma],
    })

    # Reset and define new column names
    database_f3.columns = groupby_cols + [
        "Year",
        "source_string",
        "FlowAmount",
        "FlowAmountCount",
        "TemporalCorrelation",
        "TechnologicalCorrelation",
        "GeographicalCorrelation",
        "DataCollection",
        "DataReliability",
        "uncertaintyMin",
        "uncertaintyMax",
        "uncertaintySigma",
    ]

    logging.info("Removing uncertainty from input flows")
    criteria = database_f3["Compartment"] == "input"
    database_f3.loc[criteria, "uncertaintySigma"] = None

    # Merge electricity_sum, electricity_mean, and facility_count data
    # HOTFIX: 'Year' must be integer in both dataframes [2023-12-18; TWD]
    electricity_df['Year'] = electricity_df['Year'].astype(int)
    database_f3['Year'] = database_f3['Year'].astype(int)
    database_f3 = database_f3.merge(
        right=electricity_df,
        on=elec_df_groupby_cols,
        how="left"
    )

    # Fix Canada by importing 'Electricity' whilst maintaining the indexes
    logging.info("Fixing Canadian electricity amounts")
    canadian_criteria = database_f3["FuelCategory"] == "ALL"
    if region_agg:
        canada_db = pd.merge(
            left=database_f3.loc[canadian_criteria, :],
            right=total_db[groupby_cols + ["Electricity","DataReliability"]],
            left_on=groupby_cols,
            right_on=groupby_cols,
            how="left",
        ).drop_duplicates(subset=groupby_cols)
    else:
        total_grouped = total_db.groupby(
            by=groupby_cols+["DataReliability"], as_index=False)["Electricity"].sum()
        canada_db = pd.merge(
            left=database_f3.loc[canadian_criteria, :],
            right=total_grouped,
            left_on=groupby_cols,
            right_on=groupby_cols,
            how="left",
        )

    # Reverse the dummy UUID assignment
    database_f3.loc[
        database_f3["FlowUUID"] == "dummy-uuid", "FlowUUID"
    ] = float("nan")

    # Create emission factors, adjust accordingly for for Canadian BA's
    logging.info("Creating emission factors")
    canada_db.index = database_f3.loc[canadian_criteria, :].index
    database_f3.loc[canada_db.index, "electricity_sum"] = canada_db[
        "Electricity"
    ]
    # HOTFIX: Address ZeroDivideError [2023-12-18; TWD]
    # NOTE: Set to zero because the units are per net generation;
    #       the `fix_val` is used to search for replacements (there are
    #       no known electricity_sum values less than 0.01, except for
    #       those that are 0).
    fix_val = 1e-4
    database_f3.loc[
        database_f3['electricity_sum'] == 0, 'electricity_sum'] += fix_val
    database_f3["Emission_factor"] = (
        database_f3["FlowAmount"] / database_f3["electricity_sum"]
    )
    database_f3.loc[
        database_f3['electricity_sum'] == fix_val, 'Emission_factor'] = 0

    # Calculate the log-normal parameters for uncertainty; see Hawkins-Young
    # https://github.com/USEPA/ElectricityLCI/discussions/240
    database_f3["GeomMean"], database_f3["GeomSD"] = zip(
        *database_f3[["Emission_factor", "uncertaintySigma"]].apply(
            _calc_geom_params, axis=1
    ))
    database_f3.sort_values(by=groupby_cols, inplace=True)

    return database_f3


def read_stewi_frs(frs_path="FRS_bridge_file.csv", to_save=False):
    """Helper function for reading the Facility Register Service (FRS)
    bridge file from stewi's facilitymatcher.

    Downloads a local copy of the NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV
    (1 GB) from EPA's state files, which take a few minutes.

    Parameters
    ----------
    frs_path : str
        The CSV file path (used when to_save is true), by default
        "FRS_bridge_file.csv"
    to_save : bool, optional
        Whether to save bridge file to CSV, by default False

    Returns
    -------
    pandas.DataFrame
        A data frame with three columns: REGISTRY_ID, PGM_SYS_ACRNM, and PGM_SYS_ID.
    """
    col_dict = {
        'REGISTRY_ID': "str",
        'PGM_SYS_ACRNM': "str",
        'PGM_SYS_ID': "str"
    }

    # Pull the data file name from global config; download if missing
    file_ = fmglob.FRS_config['FRS_bridge_file']
    if not (fmglob.FRSpath / file_).exists():
        fmglob.download_extract_FRS_combined_national(file_)
    FRS_bridge = fmglob.read_FRS_file(file_, col_dict)
    # ^^ these lines could be replaced by a future improved fxn
    # in FacilityMatcher
    eia860_FRS = fmglob.filter_by_program_list(
        df=FRS_bridge, program_list=["EIA-860"]
    )

    # Save a local copy
    if to_save:
        write_csv_to_output(frs_path, eia860_FRS)

    return eia860_FRS


def replace_egrid(total_db, year=None):
    """Replace eGRID primary fuel categories with EIA 923 values.

    Parameters
    ----------
    total_db : pandas.DataFrame
        A data frame with facility-level 'FuelCategory' values for each
        'eGRID_ID' facility.
    year : int, optional
        EIA generation year, by default None.
        If none, uses model_specs.eia_gen_year.

    Returns
    -------
    pandas.DataFrame
        The same data frame as the ``total_db`` parameter, but with
        "FuelCategory" values updated. Notably maintains the "ALL"
        category, which is associated with Canadian balancing authorities.
    """
    if year is None:
        year = model_specs.eia_gen_year

    primary_fuel_df = eia923_primary_fuel(year=year)
    primary_fuel_df.rename(columns={'Plant Id': "eGRID_ID"}, inplace=True)
    primary_fuel_df["eGRID_ID"] = primary_fuel_df["eGRID_ID"].astype(int)
    key_df = primary_fuel_df[
        ["eGRID_ID", "FuelCategory"]].dropna().drop_duplicates(
            subset="eGRID_ID").set_index("eGRID_ID")
    not_all = total_db["FuelCategory"] != "ALL"
    total_db.loc[not_all, "FuelCategory"] = total_db.loc[
        not_all, "eGRID_ID"].map(key_df["FuelCategory"])

    return total_db


def turn_data_to_dict(data, upstream_dict):
    """Turn aggregated emission data into exchange dictionary for openLCA.

    Parameters
    ----------
    data : pandas.DataFrame
        A multi-row data frame containing aggregated emissions to be turned
        into openLCA unit processes. Columns include the follow (as defined by
        `ng_agg_cols` in :func:`olcaschema_genprocess`):

        - stage_code
        - FlowName
        - FlowUUID
        - Compartment
        - Unit
        - Year
        - source_string
        - TemporalCorrelation
        - TechnologicalCorrelation
        - GeographicalCorrelation
        - DataCollection
        - DataReliability
        - uncertaintyMin
        - uncertaintyMax
        - uncertaintySigma
        - Emission_factor
        - GeomMean
        - GeomSD

    upstream_dict : dict
        Dictionary as created by upstream_dict.py, containing the openLCA
        formatted data for all of the fuel inputs.

    Returns
    -------
    list:
        A list of exchange dictionaries.
    """
    logging.debug("Data has %d rows" % len(data))
    logging.debug(f"Turning flows from {data.name} into dictionaries")

    # NOTE: the new olca-schema names are handled in olca_jsonld_writer.py
    cols_for_exchange_dict = [
        "internalId",
        "@type",
        "avoidedProduct",
        "flow",
        "flowProperty",
        "input",
        "quantitativeReference",
        "baseUncertainty",
        "provider",
        "amount",
        "amountFormula",
        "unit",
        "pedigreeUncertainty", # defunct
        "dqEntry",
        "uncertainty",
        "comment",
    ]

    # HOTFIX: remove exchanges that have NaNs for Emission_factor;
    #   they crash openLCA. [240813; TWD]
    #   https://github.com/USEPA/ElectricityLCI/issues/246
    num_nans = data['Emission_factor'].isna().sum()
    if num_nans > 0:
        logging.info("Removing %d nans from exchange table" % num_nans)
    data = data.dropna(subset='Emission_factor')

    data["internalId"] = ""
    data["@type"] = "Exchange"
    data["avoidedProduct"] = False
    data["flowProperty"] = ""
    data["baseUncertainty"] = ""
    data["provider"] = ""
    data["FlowType"] = "ELEMENTARY_FLOW"

    # Effectively rename 'Unit' to 'unit', 'uncertainty Max/Min' to 'Max/Min'
    data["unit"] = data["Unit"]
    data["Maximum"] = data["uncertaintyMax"]
    data["Minimum"] = data["uncertaintyMin"]

    # Define inputs based on compartment label
    data["input"] = False
    input_filter = (
        (data["Compartment"].str.lower().str.contains("input"))
        | (data["Compartment"].str.lower().str.contains("resource"))
        | (data["Compartment"].str.lower().str.contains("technosphere"))
    )
    data.loc[input_filter, "input"] = True

    # Define products based on compartment label
    # HOTFIT: input compartment tends to be technosphere flow
    product_filter=(
        (data["Compartment"].str.lower().str.contains("technosphere"))
        | (data["Compartment"].str.lower().str.contains("valuable"))
        | (data["Compartment"].str.lower().str.contains("input"))
    )
    data.loc[product_filter, "FlowType"] = "PRODUCT_FLOW"

    # Define wastes based on compartment label; NOTE they will all be inputs!
    waste_filter = (
        (data["Compartment"].str.lower().str.contains("technosphere"))
    )
    data.loc[waste_filter, "FlowType"] = "WASTE_FLOW"

    data["flow"] = ""
    data["uncertainty"] = ""
    for index, row in data.iterrows():
        data.at[index, "uncertainty"] = uncertainty_table_creation(
            data.loc[index:index, :]
        )
        data.at[index, "flow"] = flow_table_creation(
            data.loc[index:index, :]
        )

    data["amount"] = data["Emission_factor"]
    data["amountFormula"] = ""
    data["quantitativeReference"] = False

    # Pull pedigree matrix values for DQI
    data["dqEntry"] = (
        "("
        + str(round(data["DataReliability"].iloc[0], 1))
        + ";"
        + str(round(data["TemporalCorrelation"].iloc[0], 1))
        + ";"
        + str(round(data["GeographicalCorrelation"].iloc[0], 1))
        + ";"
        + str(round(data["TechnologicalCorrelation"].iloc[0], 1))
        + ";"
        + str(round(data["DataCollection"].iloc[0], 1))
        + ")"
    )
    data["pedigreeUncertainty"] = ""

    data["comment"] = data["source_string"].str.replace(
        "_", "," , regex=False) + ", " + data["Year"].astype(str)

    # Copy the columns for exchange process list
    data_for_dict = data[cols_for_exchange_dict]

    # Create a list of dictionaries:
    data_dict = data_for_dict.to_dict("records")

    # HOTFIX: append the product flow dictionary to the list [2023-11-13; TWD]
    # NOTE: This is the quantitative reference flow.
    data_dict.append(ref_exchange_creator())

    return data_dict


def olcaschema_genprocess(database, upstream_dict={}, subregion="BA"):
    """Turn a database containing generator facility emissions into a
    dictionary that contains required data for an openLCA-compatible JSON-LD.

    Additionally, default providers for fuel inputs are mapped using the
    information contained in the dictionary containing openLCA-formatted
    data for the fuels.

    Parameters
    ----------
    database : dataframe
        Dataframe containing aggregated emissions to be turned into openLCA
        unit processes
    upstream_dict : dictionary, optional
        Dictionary as created by upstream_dict.py, containing the openLCA
        formatted data for all of the fuel inputs. This function will use the
        names and UUIDs from the entries to assign them as default providers.
    subregion : str, optional
        The subregion level of the aggregated data, by default "BA". See
        aggregation_selector.py for available subregions.

    Returns
    -------
    dict
        Dictionary contaning openLCA-formatted data.
    """
    region_agg = subregion_col(subregion)
    fuel_agg = ["FuelCategory"]
    # Iss150, add stage code to catch renewable construction
    stage_code = ["stage_code"]
    renewables_const_stage_codes = [
        "solar_pv_const",
        "wind_const",
        "solar_thermal_const"
    ]
    if region_agg:
        base_cols = region_agg + fuel_agg + stage_code
    else:
        base_cols = fuel_agg + stage_code

    non_agg_cols = [
        "FlowName",
        "FlowUUID",
        "Compartment",
        "Unit",
        "Year",
        "source_string",
        "TemporalCorrelation",
        "TechnologicalCorrelation",
        "GeographicalCorrelation",
        "DataCollection",
        "DataReliability",
        "uncertaintyMin",
        "uncertaintyMax",
        "uncertaintySigma",
        "Emission_factor",
        "GeomMean",
        "GeomSD",
    ]
    non_agg_cols = [x for x in non_agg_cols if x in database.columns]

    # Create a data frame with one massive column of exchanges
    logging.info("Creating exchanges")
    database_groupby = database.groupby(by=base_cols)
    process_df = pd.DataFrame(
        database_groupby[non_agg_cols].apply(
            turn_data_to_dict,
            (upstream_dict)
        )
    )
    process_df.columns = ["exchanges"]

    # Iss150, The following 18 lines of code are taken from turn_data_to_dict
    # function and modified to reflect that there are now process dictionaries
    # created for technosphere inputs (e.g. coal input from IL-B-U). These
    # flows must have the default provider defined using the existing upstream
    # dictionary and then be "moved" into the Power plant data frame where they
    # should be.
    # First, get indices where upstream process exists.
    provider_filter = [
        x for x in process_df.index.values if x[-1] in upstream_dict.keys()]
    # HOTFIX: only include stage codes found in process_df [241011; TWD]
    sc_list = list(set([x[-1] for x in provider_filter]))
    for index, row in process_df[process_df.index.get_level_values('stage_code').isin(sc_list)].iterrows():
        # New Issue #150, try first to match regional construction. Fall back
        # is US average.
        sc = index[-1]
        fuel_cat = index[-2]
        region = index[0] if region_agg else 'US'
        if "_const" in sc:
            try:
                provider_dict = {
                    "name": upstream_dict[sc + " - " + region]["name"],
                    "categoryPath": upstream_dict[sc + " - " + region]["category"],
                    "processType": "UNIT_PROCESS",
                    "@id": upstream_dict[sc + " - " + region]["uuid"],
                }
            except KeyError:
                provider_dict = {
                    "name": upstream_dict[sc]["name"],
                    "categoryPath": upstream_dict[sc]["category"],
                    "processType": "UNIT_PROCESS",
                    "@id": upstream_dict[sc]["uuid"],
                }
        else:
            provider_dict = {
                "name": upstream_dict[sc]["name"],
                "categoryPath": upstream_dict[sc]["category"],
                "processType": "UNIT_PROCESS",
                "@id": upstream_dict[sc]["uuid"],
            }
        row["exchanges"][0]["provider"] = provider_dict
        row["exchanges"][0]["unit"] = unit(
            upstream_dict[sc]["q_reference_unit"]
        )
        row["exchanges"][0]["FlowType"] = "PRODUCT_FLOW"
        process_df.loc[index[:-1] + ('Power plant',)]["exchanges"].append(
            row["exchanges"][0])

    # These are now only power plant stage codes
    process_df = process_df.drop(provider_filter)
    process_df.reset_index(inplace=True)

    process_df["@type"] = "Process"
    process_df["allocationFactors"] = ""
    process_df["defaultAllocationMethod"] = ""
    process_df["parameters"] = ""
    process_df["processType"] = "UNIT_PROCESS"
    # HOTFIX: add squeeze to force DataFrame to Series [2023-11-13; TWD]
    process_df["category"] = (
        "22: Utilities/2211: Electric Power Generation, "
        "Transmission and Distribution/" + process_df[fuel_agg].squeeze().values
    )
    sc_filter = process_df["stage_code"].isin(renewables_const_stage_codes)
    if region_agg is None:
        process_df["location"] = "US"
        process_df["description"] = (
            "Electricity from "
            + process_df[fuel_agg].squeeze().values
            + " produced at generating facilities in the US."
        )
        process_df["name"] = (
            "Electricity - " + process_df[fuel_agg].squeeze().values + " - US"
        )
        # Iss150, correct construction stage code name and description
        process_df.loc[sc_filter, "description"] = (
            "Construction of "
            + process_df[fuel_agg].values
            + " in the US"
        )
        process_df.loc[sc_filter,"name"] = (
            "Construction - "
            + process_df[fuel_agg].values
            + " - US"
        )
    else:
        # HOTFIX: remove .values, which throws ValueError [2023-11-13; TWD]
        process_df["location"] = process_df[region_agg]
        process_df["description"] = (
            "Electricity from "
            + process_df[fuel_agg].squeeze().values
            + " produced at generating facilities in the "
            + process_df[region_agg].squeeze().values
            + " region."
        )
        process_df["name"] = (
            "Electricity - "
            + process_df[fuel_agg].squeeze().values
            + " - "
            + process_df[region_agg].squeeze().values
        )
        # Iss150, correct construction name and description
        process_df.loc[sc_filter, "description"] = (
            "Construction of "
            + process_df.loc[sc_filter, fuel_agg[0]].squeeze().values
            + " in the "
            + process_df.loc[sc_filter, region_agg[0]].squeeze().values
            + " region."
        )
        process_df.loc[sc_filter, "name"] = (
            "Construction - "
            + process_df.loc[sc_filter, fuel_agg[0]].squeeze().values
            + " - "
            + process_df.loc[sc_filter, region_agg[0]].squeeze().values
        )

    # TODO: use `process_description_creation` from process_disctionary_writer to fill in this portion; note that the default text below is captured in the return string from that method.
    process_df["description"] += (
        " This process was created with ElectricityLCI "
        + "(https://github.com/USEPA/ElectricityLCI) version " + elci_version
        + " using the " + model_specs.model_name + " configuration."
    )
    process_df["version"] = make_valid_version_num(elci_version)
    process_df["processDocumentation"] = [
        process_doc_creation(x) for x in list(
            process_df["FuelCategory"].str.lower())
    ]

    process_cols = [
        "@type",
        "allocationFactors",
        "defaultAllocationMethod",
        "exchanges",
        "location",
        "parameters",
        "processDocumentation",
        "processType",
        "name",
        "version",
        "category",
        "description",
    ]
    result = process_df[process_cols].to_dict("index")

    return result


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    plant_emission_df = create_generation_process_df()
    aggregated_emissions_df = aggregate_data(plant_emission_df, subregion="BA")
    datetimestr = datetime.now().strftime("%Y%m%d_%H%M%S")
    aggregated_emissions_df.to_csv(
        f"{output_dir}/aggregated_emissions_{datetimestr}.csv"
    )
    plant_emission_df.to_csv(f"{output_dir}/plant_emissions_{datetimestr}.csv")
