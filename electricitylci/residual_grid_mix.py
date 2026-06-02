#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# residual_grid_mix.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

import pandas as pd

from electricitylci.globals import GREEN_E
from electricitylci.globals import OVERFLOW_E
from electricitylci.model_config import model_specs
from electricitylci import get_generation_mix_process_df
from electricitylci.eia860_facilities import eia860_balancing_authority
from electricitylci.eia923_generation import build_generation_data
from electricitylci.olca_jsonld_writer import build_residual_processes
from electricitylci.utils import get_nrel_rec
from electricitylci.utils import map_ba_codes
from electricitylci.utils import write_csv_to_output
from electricitylci.globals import NREL_REC_URL


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """A module to calculate residual electricity grid mix (REM) based
on the Energy Information Administration (EIA) Form 923 generation data
(filtered based on :func:`build_generation_data` found in eia923_generation.py)
and mix calculations (e.g., generation ratio and fuel category) based on
:func:`create_generation_mix_process_df_from_model_generation_data` found in
generation_mix.py. The publicly released Excel workbook, Status and Trends in
the U.S. Voluntary Green Power Market,[1] published by the National Laboratory
of the Rockies, provides state-level renewable energy certificate (REC) sales.
Power plant regional information (based on EIA Form 860) is used to map state
REC sales to balancing authority generation (e.g., by facility counts and,
in future releases by facility generation).

To calculate the residual mix, a balancing authority's generation mix is
categorized as either REC (renewable energy sold as a certificate) or non-REC
(the desired residual generation). REC generation is based on the above
aggregation method from the public sales data. Following this division, the
next step is to determine the fuel-based mix of the residual generation. This
is accomplished in :func:`update_mix`, where the original mix is divided into
renewable (REC-compatible generation) and non-renewable (not for REC sale)
generation. Non-renewable generation is by definition non-REC, thus carries
over to the non-REC generation mix.

The renewable generation may be labeled under several fuel categories as
defined in the global variable, ``GREEN_E`` (e.g., hydro, biomass, solar, wind,
geo). REC sales do not (as of writing) distinguish the fuel type used;
therefore, the ratio of renewable generation by fuel category and total
renewable generation is calculated using :func:`calc_relative_ratio`. The
renewable electricity sold as REC is subtracted from the total renewable
generation to determine the non-REC renewable generation. The ratio of renewable
generation by fuel category is used to allocate the non-REC renewable
generation to each fuel category (i.e., assumes the same relative ratio across
renewable fuel categories).

Negative non-REC renewable generation amounts are possible due to the inexact
process of allocating REC sales to balancing authorities. In the case of
negative non-REC renewable generation, the user may elect one of two options
(i.e., 'zero' or 'keep' in the YAML configuration). For the 'keep' option, the
MIXED or OTHER fuel categories are queried in the non-renewable mix fuels. If
found, the non-renewable non-REC generation is reduced by the overage in REC
sales to renewable generation (i.e., the assumption that renewable energy
exists within the MIXED or OTHER fuels); note that this is done
indiscriminantly across all non-renewable fuel categories using the relative
ratio method used for renewable fuels. If the user elects to 'zero' the
overage, the non-REC non-renewable generation remains unchanged. The non-REC
renewable energy is zeroed regardless of selection.

The non-renewable and renewable non-REC generation amounts are summed to
determine the non-REC generation total. The fuel-specific generation amounts
(i.e., based on the renewable and non-renewable non-REC generation amounts
allocated using the relative ratio method) are divided by the new non-REC
generation total to determine the new residual mix.

Methods are based on ``elci_to_rem`` Python tool version 2.[2]

1.  E. O'Shaughnessy, S. Jena, and D. Salyer. 2025. Status and Trends in the
    Voluntary Market (2024 Data). Golden, CO: NLR. Online:
    https://www.nlr.gov/docs/libraries/analysis/nrel-green-power-data-v2024.xlsx
2.  Tyler W. Davis, Matthew Jamieson, Becca Rosen, Joseph Chou, elci_to_rem,
    1/21/2025, https://edx.netl.doe.gov/dataset/elci_to_rem,
    DOI: 10.18141/2503966

Last updated:
    2026-03-27
"""
__all__ = [
    "agg_by_count",
    "calc_relative_ratio",
    "get_elci_mix",
    "get_rec_agg",
    "get_rem",
    "update_mix",
]


##############################################################################
# FUNCTIONS
##############################################################################
def add_residual_mixes(json_path=None):
    """Helper function to generate residual mix processes.

    Takes model configuration parameters (``eia_gen_year``,
    ``rem_weight_method``, and ``neg_rem_method``) to generate residual
    mix data frame (see :func:`get_rem`), which may be saved to CSV
    (depending on config parameter, ``output_residual_mix``), and
    passes the residual mix to olca_jsonld_writer for creating the processes.

    Parameters
    ----------
    json_path : str, optional
        The JSON-LD file path, by default None.
        If none, the current model run's JSON-LD file is referenced.
        (_this optional parameter is to permit REM post-processing_)

    Notes
    -----
    This method will not run if the configuration parameter,
    ``add_residual_mix`` is set to false.
    """
    # Stop process if configuration is not set up for residual mixes.
    if not model_specs.add_residual_mix:
        logging.info("Residual mix processes are not created.")
        return

    # Create the residual process description text.
    # NOTE: This is added to all residual process descriptions.
    rem_text = (
        "Electricity generation mixes updated to reflect residual grid "
        "mix based on NLR's Status and Trends in the Voluntary Market "
        f"for sales in year {model_specs.eia_gen_year} "
        f"({NREL_REC_URL}). "
    )
    if model_specs.rem_weight_method == 'count':
        rem_text += (
            "The balancing authority residual mix is based on a facility "
            "count weighting method of state-level REC sales where excess REC "
        )
    elif model_specs.rem_weight_method == 'gen':
        rem_text += (
            "The balancing authority residual mix is based on a weighting "
            "method of facility-level generation using state-level REC sales "
            "where excess REC "
        )

    if model_specs.neg_rem_method == 'zero':
        rem_text += (
            "generation amounts (MWh) are ignored (i.e., assumed zero; "
            "accounts for all available renewable generation)."
        )
    elif model_specs.neg_rem_method == 'keep':
        rem_text += (
            "generation amounts (MWh) are subtracted from non-renewable "
            "sources, assuming that some renewable energy may be provided from "
            "a non-renewable fuel category (e.g., mixed/other fuels)."
        )

    # Create residual mix for BA by fuel category;
    #   let the user decide to save mix as CSV file in outputs
    df = get_rem(to_save=model_specs.output_residual_mix)

    # Allow user to run this process on an existing baseline JSON-LD;
    # otherwise, default back to the current output file.
    if json_path is None:
        json_path = model_specs.namestr
    else:
        logging.info("Adding residual processes to %s" % json_path)

    # Add residual process to JSON-LD
    build_residual_processes(json_path, df, rem_text)


def agg_by_count():
    """Partition state-based REC electricity generation using the fractional
    weights of electricity generating facility counts found within the shared
    boundaries of each state and balancing authority area.

    Notes
    -----
    This method assumes the state names reported in EIA Form 860 (i.e., this
    does not perform any spatial analysis).

    Returns
    -------
    pandas.Series
        A Pandas series where the index is 'BA_CODE' (balancing authority
        area abbreviation) and the values are 'REC_FRAC' (allocated REC
        generation from states to their BA areas).
    """
    logging.info("Aggregating by state counts")
    ba_df = eia860_balancing_authority(year=model_specs.eia_gen_year)
    ba_df.rename(columns={
        'Balancing Authority Name': "BA_NAME",
        'Balancing Authority Code': "BA_CODE"}, inplace=True)

    # Not every plant has a BA and State, so drop NAs
    ba_df = ba_df.dropna(subset='BA_CODE')

    # Create plant count tables
    table_1 = ba_df.value_counts(subset=['State', 'BA_CODE'])
    table_1.name = "STBA_PLANTS"
    table_2 = ba_df.value_counts(subset=['State'])
    table_2.name = "ST_PLANTS"

    # Join these series together
    t1_df = table_1.reset_index(drop=False)
    t2_df = table_2.reset_index(drop=False)
    t1_df = t1_df.merge(t2_df, how='left', on='State')

    # Calculate state-level fractions
    # these should all add to 1.0 given the NA drop above.
    t1_df["ST_FRAC"] = t1_df['STBA_PLANTS'] / t1_df['ST_PLANTS']

    # Join REC data
    rec_df = get_nrel_rec(model_specs.eia_gen_year)
    rec_df = rec_df[['State', 'Total']].copy()
    jdf = t1_df.merge(rec_df, how='left', on='State')

    # Calculate BA REC amounts using plant count fractions
    jdf['REC_FRAC'] = jdf['ST_FRAC'] * jdf['Total']

    # Allocate RECs to their BA areas using the new relative fractions
    # the sum of REC_FRAC should equal the sum of Total in the REC data frame
    tot_df = jdf.groupby(by='BA_CODE')['REC_FRAC'].agg("sum")
    tot_df.index.names = ['BA_CODE']
    return tot_df


def agg_by_gen():
    """Partition state-based REC electricity generation using the fractional
    weights of electricity generating facilities found within the shared
    boundaries of each state and balancing authority area weighted by
    facility-level annual generation.

    Returns
    -------
    pandas.Series
        A Pandas series where the index is 'BA_CODE' (balancing authority
        abbreviation) and the values are 'REC_FRAC' (allocated REC
        generation from states to their BA areas).
    """
    logging.info("Aggregating by facility-level generation.")

    # Get state and BA info for each facility.
    ba_df = eia860_balancing_authority(year=model_specs.eia_gen_year)
    ba_df.rename(columns={
        'Balancing Authority Name': "BA_NAME",
        'Balancing Authority Code': "BA_CODE",
        "Plant Id": "FacilityID"}, inplace=True)

    # Not every plant has a BA and State, so drop NAs
    ba_df = ba_df.dropna(subset='BA_CODE')

    # Fix column type for merging purposes.
    ba_df['FacilityID'] = ba_df['FacilityID'].astype("int")

    # Get facility-level generation amounts
    fac_gen = build_generation_data(
        generation_years=[model_specs.eia_gen_year,]
    )
    fac_gen = fac_gen.drop(columns='Year')

    # Add annual generation to facility data (i.e., 'Electricity' column).
    ba_df = ba_df.merge(fac_gen, on='FacilityID', how='inner')

    # Create plant generation tables
    table_a = ba_df.groupby(
        by=['State', 'BA_CODE']).agg({'Electricity': 'sum'}).Electricity
    table_b = ba_df.groupby(
        by='State').agg({'Electricity': 'sum'}).Electricity

    table_a.name = "STBA_GEN"
    table_b.name = "ST_GEN"

    # Join these series together
    ta_df = table_a.reset_index(drop=False)
    tb_df = table_b.reset_index(drop=False)
    ta_df = ta_df.merge(tb_df, how='left', on='State')

    # Calculate state-level fractions;
    # these should add to 1.0 for each state given the NA drop above.
    ta_df["ST_FRAC"] = ta_df['STBA_GEN'] / ta_df['ST_GEN']

    # Join REC data
    rec_df = get_nrel_rec(model_specs.eia_gen_year)
    rec_df = rec_df[['State', 'Total']].copy()
    jdf = ta_df.merge(rec_df, how='left', on='State')

    # Calculate BA REC amounts using facility-level generation weights
    jdf['REC_FRAC'] = jdf['ST_FRAC'] * jdf['Total']

    # Allocate RECs to their BA areas using the new relative fractions
    # the sum of REC_FRAC should equal the sum of Total in the REC data frame
    tot_df = jdf.groupby(by='BA_CODE').agg({'REC_FRAC': 'sum'})

    return tot_df


def calc_relative_ratio(df, add_total=False):
    """Calculate the relative electricity generation fractions in a data frame.

    Parameters
    ----------
    df : panda.DataFrame
        A data frame with 'Electricity' field for electricity per fuel category.
        For example, taking a subset of electricityLCI's generation mix for
        a given Balancing Authority area.
    add_total : bool, optional
        Switch to include 'Relative_Total' field, by default False

    Returns
    -------
    pandas.DataFrame
        The same as the argument, but with new fields, "Relative_Ratio"
        and optional "Relative_Total".`

    Raises
    ------
    TypeError
        When argument object is not a data frame.
    IndexError
        When data frame does not have the expected 'Electricity' field.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected data frame, received %s" % type(df))
    if "Electricity" not in df.columns:
        raise IndexError("Data frame missing required 'Electricity' field!")
    total_e = df['Electricity'].sum()
    df['Relative_Ratio'] = df['Electricity'] / total_e
    if add_total:
        df['Relative_Total'] = total_e
    return df


def get_elci_mix():
    """Create data frame of balancing authority electricity generation
    mix amounts by primary fuel category using EIA Form 860 and generation
    from EIA Form 923.

    Returns
    -------
    pandas.DataFrame
        A data frame with fields: "Subregion" (i.e, balancing authority
        names), "FuelCategory" (i.e., primary fuel technology names),
        "Electricity" (i.e., annual generation, MWh), and "Generation_Ratio"
        (i.e., fraction of the total generation accounted by the fuel type
        for the given subregion), and "BA_CODES" with balancing authority
        abbreviations.
    """
    df = get_generation_mix_process_df(regions="BA")
    return map_ba_codes(df)


def get_rec_agg(agg_type, as_series=True):
    """Return REC generation aggregated from states to balancing authority
    areas based on the aggregation type.

    Parameters
    ----------
    agg_type : str
        The aggregation type. Valid options are 'area' and 'count'.

        There are two options to determine the fraction of each state's
        RECs allocated to each balancing authority area.

        -   The 'count' option uses facility counts that fall within the
            shared regions of states and balancing authorities.
        -   The 'gen' option uses facility-level generation as a weighting
            factor to the 'count' method.

    as_series : bool, optional
        Switch to return object as either a Pandas series (if true) or as a
        data frame (if false), by default True.

    Returns
    -------
    pandas.Series or pandas.DataFrame
        A series or data frame with balancing authority codes (BA_CODE) and
        their REC generation amounts (REC_FRAC).

    Raises
    ------
    ValueError
        If aggregation type is not valid.
    """
    if agg_type == 'gen':
        r = agg_by_gen()
    elif agg_type == 'count':
        r = agg_by_count()
    else:
        raise ValueError(
            "Expected agg type to be either 'area' or 'count'; "
            "found '%s'" % agg_type)
    if not as_series:
        r = r.reset_index(drop=False)
    return r


def get_rem(to_save=False):
    """A short-hand method for creating the residual electricity mix data frame.

    Parameters
    ----------
    to_save : bool, optional
        Switch to save results to CSV file, by default false.
        If true, output file is written to DATA_DIR in the format,
        "res-mix_[gen_yr]_rec-[rec_handler]_agg-[agg_handler].csv".

    Returns
    -------
    pandas.DataFrame
        A generation mix data frame with the following columns.

        - 'Subregion', the balancing authority name
        - 'FuelCategory', the primary fuel category (e.g., SOLAR, COAL)
        - 'Electricity', the annual generation for fuel category (MWh)
        - 'Generation_Ratio', the mix fraction for the fuel category
        - 'BA_CODE', the balancing authority abbreviation
        - 'Electricity_new', the REC-free annual generation by fuel (MWh)
        - 'Gen_Ratio_new', the REC-free mix fraction for fuel category
    """
    logging.info("Running residual grid mix calculation tool")
    m_df = get_elci_mix()
    m_df = update_mix(m_df)
    logging.info("Complete!")

    if to_save:
        out_file = (
            f"res-mix_{model_specs.eia_gen_year}"
            f"_rec-{model_specs.neg_rem_method}"
            f"_agg-{model_specs.rem_weight_method}.csv"
        )
        logging.info("Writing %s to file" % out_file)
        write_csv_to_output(out_file, m_df)

    return m_df


def update_mix(df):
    """Update a balancing authority generation mix by removing RECs.

    This methods appends two new columns to the generation mix data frame
    with REC-free electricity generation (MWh) and new mix fractions for
    each fuel category under each balancing authority.

    Notes
    -----
    -   Fixed mis-matched balancing authorities in 'count'-based REC aggregates
        by merging on BA_CODE field.
    -   Added check for non-green energy categories when dealing with overflow
        generation in REC data (as compared to the baseline generation).
        It should be further noted that the presence of one of these overflow
        fuel categories does not preclude non-renewable fuel categories (e.g.,
        coal, oil, or gas) from being reduced by overflowing REC-based
        generation. This would require a third bucket to be introduced to the
        model, such that overflow only comes from MIXED or OTHF categories.
        Currently all non-renewable categories are reduced to make up for the
        difference (when rec_handler is set to 'keep').

    See also
    --------
    The README for this repository includes the pseudocode and visual aids
    regarding this method. Variable names used in this method attempt to
    match the syntax of the pseudocode provided.

    This method relies on the following two model-configured parameters:

    -   rec_handler (str), option for handling REC totals that are greater
        than the renewable energy generated by a balancing authority.
        There are two options:

        -   'keep' will subtract the excess away from the non-renewable fuel
            categories (assuming some renewable generation in the 'mix' or
            'othf' categories) and maintains the math of generation totals
        -   'zero' will floor negative renewable energy values to zero; the
            remainder is unaccounted for

    -   agg_handler (str), Option for state-level to BA REC sales aggregation.
        There is currently one option, 'count', which aggregates based on the
        number of shared facilities.

    Parameters
    ----------
    df : pandas.DataFrame
        Generation mix for balancing authorities.
        Required fields include 'Subregion', 'Electricity', 'Generation_Ratio',
        and 'FuelCategory'.

    Returns
    -------
    pandas.DataFrame
        The same as the generation mix data frame sent, but with two new
        fields.

        -   'Electricity_new' is the REC-free electricity generation amount
            (MWh) for each fuel category under each balancing authority area.
        -   'Gen_Ratio_new' is the REC-free fractional mix for each fuel
            category under each balancing authority area.

    Raises
    ------
    TypeError
        If the method does not receive a pandas data frame for df.
    IndexError
        If the pandas data frame, df, does not have the required fields.
    """
    # Basic error handling
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas data frame, found %s" % type(df))

    r_cols = ['Electricity', 'Generation_Ratio', 'Subregion']
    if not all([i in df.columns for i in r_cols]):
        raise IndexError("Data frame missing required columns!")

    if model_specs.neg_rem_method == 'keep':
        logging.info(
            "Negative renewable energy will be taken from "
            "non-renewable energy generation amounts.")
    elif model_specs.neg_rem_method == 'zero':
        logging.info("Negative renewable energy will be zeroed.")

    # Initialize the new columns with existing electricity amounts and
    # generation ratios (e.g., for regions with no renewables, these values
    # shouldn't change).
    df['Electricity_new'] = df['Electricity']
    df['Gen_Ratio_new'] = df['Generation_Ratio']

    # Define columns used to merge new electricity and generation mix values
    # to our data frame (referenced in the for-loop below).
    m_cols = ['Subregion', 'FuelCategory', 'Electricity_new', 'Gen_Ratio_new']

    # Get the aggregation series and pair to BA area names
    # NOTE: name corrections for geo BA dataframe should fix any mis-matches
    logging.info("Using '%s' weighting method" % model_specs.rem_weight_method)
    agg_df = get_rec_agg(model_specs.rem_weight_method, as_series=False)

    for baa in df['Subregion'].unique():
        # ~~~~~~~~~~~~~~~~
        # NON-GREEN ENERGY
        # ~~~~~~~~~~~~~~~~
        # Find all non-green energy sources and set the non-green energy
        # total (ng) and the non-REC non-green energy total (ngx).
        ng_df = df.query(
            "(FuelCategory not in @GREEN_E) & (Subregion == @baa)").copy()
        ng = 0.0
        ngx = 0.0
        if len(ng_df) > 0:
            ng_df = calc_relative_ratio(ng_df, add_total=True)
            ng_df.drop(
                ['Electricity', 'Generation_Ratio'], axis=1, inplace=True)
            ng = ng_df['Relative_Total'].values[0]
            ngx = ng

        # ~~~~~~~~~~~~
        # GREEN ENERGY
        # ~~~~~~~~~~~~
        # Find only green energy fuels for the given BA area and initialize
        # the non-REC green energy total (gx), to zero.
        g_df = df.query(
            "(FuelCategory in @GREEN_E) & (Subregion == @baa)").copy()
        gx = 0.0

        # Skip BA areas with no green energy; nothing to do!
        if len(g_df) > 0:
            g_df = calc_relative_ratio(g_df, add_total=True)
            g_df.drop(['Electricity', 'Generation_Ratio'], axis=1, inplace=True)

            # Merge and keep index; thanks to Wouter Overmeire (2012)
            # https://stackoverflow.com/a/11982843
            g_df = g_df.reset_index().merge(
                agg_df, how='left', on='BA_CODE').set_index("index")

            # Pull values from data frame for green energy total (big_g)
            # and REC energy total (rec_t) and use them to calculate the
            # non-REC green energy (gx). NOTE: the relative total and
            # rec frac columns are constants, so it's safe to pull just
            # one value from the lot.
            big_g = g_df['Relative_Total'].values[0]
            rec_t = g_df['REC_FRAC'].values[0]
            gx = big_g - rec_t

            # NOTE: due to the categorization of "Green energy," there is a
            # good chance for negative green generation amounts.
            # 1. If we keep the negative amounts, when these are added back to
            #    the generation totals, we can assume that the "mix" or
            #    "other" fuels compensate ('keep' option); or
            # 2. We can zero out the negatives ('zero' option).

            # Check for negative green generation
            if rec_t > big_g:
                logging.info(
                    "Negative renewable energy for %s (%0.2e MWh)" % (baa, gx))
                has_ofe = any(
                    [i in OVERFLOW_E for i in ng_df['FuelCategory'].values])
                if model_specs.neg_rem_method == 'keep' and has_ofe:
                    # Pull the "excess" electricity from non-green
                    gx = 0.0
                    ngx = ng - (rec_t - big_g)
                    # Don't let total generation go negative
                    ngx = max(0.0, ngx)
                else:
                    gx = 0.0

        # Calculate non-REC
        non_rec = gx + ngx

        # Calculate non-REC generation amounts and ratios of each fuel type
        if len(ng_df) > 0:
            ng_df['Electricity_new'] = ng_df['Relative_Ratio'] * ngx
            ng_df['Gen_Ratio_new'] = 0.0
            if non_rec > 0:
                ng_df['Gen_Ratio_new'] = ng_df['Electricity_new'] / non_rec
            df.update(ng_df[m_cols], join='left', overwrite=True)
        if len(g_df) > 0:
            g_df['Electricity_new'] = g_df['Relative_Ratio'] * gx
            g_df['Gen_Ratio_new'] = 0.0
            if non_rec > 0:
                g_df['Gen_Ratio_new'] = g_df['Electricity_new'] / non_rec
            df.update(g_df[m_cols], join='left', overwrite=True)

    return df


#
# SANDBOX
#
if __name__ == '__main__':
    # Setup logging
    from electricitylci.utils import get_logger
    log = get_logger(stream=True, rfh=False)

    # Setup model
    import electricitylci.model_config as config
    config.model_specs = config.build_model_class('ELCI_2023')

    # Test new 'gen' method; six BAs with negative renewable energy.
    from electricitylci.residual_grid_mix import get_rem
    df = get_rem(to_save=True)

    #
    # AGG BY GEN draft
    # See comparison of facility count and generation weights for ELCI_2023:
    # https://edx.netl.doe.gov/resource/af61a48f-a372-458f-a0e5-c37cf387f384
    #

    # Get state and BA info for each facility.
    ba_df = eia860_balancing_authority(year=config.model_specs.eia_gen_year)
    ba_df.rename(columns={
        'Balancing Authority Name': "BA_NAME",
        'Balancing Authority Code': "BA_CODE",
        "Plant Id": "FacilityID"}, inplace=True)

    # Not every plant has a BA and State, so drop NAs
    ba_df = ba_df.dropna(subset='BA_CODE')

    # Fix column type for merging purposes.
    ba_df['FacilityID'] = ba_df['FacilityID'].astype("int")

    # Get facility-level generation amounts
    from electricitylci.eia923_generation import build_generation_data

    # Decision point:
    #   a) If you send the plant IDs, you get 72% of 2023 plants;
    #      --> includes hugely negative plant generations ;(
    #   b) If you don't specify plant IDs, you get 68% of 2023 plants.
    # Probably better to go route (b) and avoid negative generation.
    fac_gen = build_generation_data(
        # egrid_facilities_to_include=ba_df['Plant Id'].to_list(),
        generation_years=[config.model_specs.eia_gen_year,]
    )
    fac_gen = fac_gen.drop(columns='Year')

    # Add annual generation to facility data (i.e., 'Electricity' column).
    ba_df = ba_df.merge(fac_gen, on='FacilityID', how='inner')

    # Create plant count tables
    table_a = ba_df.groupby(
        by=['State', 'BA_CODE']).agg({'Electricity': 'sum'}).Electricity
    table_b = ba_df.groupby(
        by='State').agg({'Electricity': 'sum'}).Electricity

    table_a.name = "STBA_GEN"
    table_b.name = "ST_GEN"

    # Join these series together
    ta_df = table_a.reset_index(drop=False)
    tb_df = table_b.reset_index(drop=False)
    ta_df = ta_df.merge(tb_df, how='left', on='State')

    # Calculate state-level fractions;
    # these should add to 1.0 for each state given the NA drop above.
    ta_df["ST_FRAC"] = ta_df['STBA_GEN'] / ta_df['ST_GEN']

    # Join REC data
    rec_df = get_nrel_rec(config.model_specs.eia_gen_year)
    rec_df = rec_df[['State', 'Total']].copy()
    jdf = ta_df.merge(rec_df, how='left', on='State')

    # Calculate BA REC amounts using facility-level generation weights
    jdf['REC_FRAC'] = jdf['ST_FRAC'] * jdf['Total']

    # Allocate RECs to their BA areas using the new relative fractions
    # the sum of REC_FRAC should equal the sum of Total in the REC data frame
    tot_df = jdf.groupby(by='BA_CODE').agg({'REC_FRAC': 'sum'})
