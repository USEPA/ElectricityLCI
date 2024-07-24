#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# generation.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import ast
from datetime import datetime
import logging

import numpy as np
import pandas as pd
from scipy.stats import t            # in geometric_mean in aggregate_data
from scipy.special import erfinv

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
from electricitylci.utils import set_dir
from electricitylci.egrid_emissions_and_waste_by_facility import (
    emissions_and_wastes_by_facility,
)
import facilitymatcher.globals as fmglob  # package under development


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
-   TODO: implement Hawkins-Young uncertainty

Created:
    2019-06-04
Last edited:
    2024-07-24
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
    "olcaschema_genprocess",
    "turn_data_to_dict",
]


##############################################################################
# FUNCTIONS
##############################################################################
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


def add_temporal_correlation_score(db, electricity_lci_target_year):
    """Generates columns in a data frame for data age and its quality score.

    Parameters
    ----------
    db : pandas.DataFrame
        A data frame with column 'Year' representing the data source year.
    electricity_lci_target_year : int
        The year associated with data use (see model_config attribute,
        'electricity_lci_target_year').

    Returns
    -------
    pandas.DataFrame
        The same data frame received with two new columns:

        - 'Age' : int, difference between target year and data source year.
        - 'TemporalCorrelation' : int, DQI score based on age.
    """
    # Could be more precise here with year
    db['Age'] =  electricity_lci_target_year - pd.to_numeric(db['Year'])
    db['TemporalCorrelation'] = db['Age'].apply(
        lambda x: lookup_score_with_bound_key(
            x, temporal_correlation_lower_bound_to_dqi))
    return db


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
        "FacilityID",
        "Electricity",
        "FlowName",
        "Source",
        "Compartment",
        "stage_code"
    ]

    def wtd_mean(pdser, total_db, cols):
        """Perform a weighted-average of DQI using 'FlowAmount' as the weight.

        Parameters
        ----------
        pdser : pandas.Series
            A numeric series (e.g., DataQuality).
        total_db : pandas.DataFrame
            A data frame with 'FlowAmount' column, numeric, used for weights.
        cols : list
            Unused.

        Returns
        -------
        float
            The weighted average for a given series.
        """
        try:
            wts = total_db.loc[pdser.index, "FlowAmount"]
            result = np.average(pdser, weights=wts)
        except:
            logging.debug(
                f"Error calculating weighted mean for {pdser.name}-"
                f"likely from 0 FlowAmounts"
            )
            try:
                with np.errstate(all='raise'):
                    result = np.average(pdser)
            except ArithmeticError or ValueError or FloatingPointError:
                result = float("nan")
        return result

    wm = lambda x: wtd_mean(x, df, groupby_cols)
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
    power_plant_criteria = db["stage_code"]=="Power plant"
    db_powerplant = db.loc[power_plant_criteria, :].copy()
    db_nonpower = db.loc[~power_plant_criteria, :].copy()
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

    # HOTFIX: add check for empty powerplant data frame [2023-12-19; TWD]
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
            source_df = pd.DataFrame(
                db_multiple_sources.groupby(["FlowName", "Compartment"])[
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
                left_on=["FlowName", "Compartment"],
                right_on=["FlowName", "Compartment"],
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
        sub_db.drop_duplicates(subset=fuel_agg + ["eGRID_ID"], inplace=True)
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
    db_nonpower["source_string"] = all_sources
    db_nonpower["source_list"] = [all_sources]*len(db_nonpower)
    elec_sums = pd.concat(elec_sum_lists, ignore_index=True)
    elec_sums.sort_values(by=elec_groupby_cols, inplace=True)
    db = pd.concat([db_powerplant, db_nonpower])

    return db, elec_sums


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
    }
    if model_specs.replace_egrid:
        # Create data frame with 'FacilityID', 'Electricity', and 'Year'
        generation_data = build_generation_data().drop_duplicates()

        eia_facilities_to_include = generation_data["FacilityID"].unique()
        # Other columns in the emissions_and_wastes_by_Facility
        # FacilityID and FRS_ID (in addition to those above)
        inventories_of_interest_list = sorted([
            f"{x}_{model_specs.inventories_of_interest[x]}"
            for x in model_specs.inventories_of_interest.keys()
        ])
        inventories_of_interest_str = "_".join(inventories_of_interest_list)

        try:
            eia860_FRS = pd.read_csv(
                f"{paths.local_path}/FRS_bridges/"
                f"{inventories_of_interest_str}.csv")
            logging.info(
                "Got EIA860 to FRS ID matches from existing file")
            eia860_FRS["REGISTRY_ID"] = eia860_FRS["REGISTRY_ID"].astype(str)
        except FileNotFoundError:
            logging.info(
                "Will need to load EIA860 to FRS matches using stewi "
                "facility matcher - it may take a while to download "
                "and read the required data")

            file_ = fmglob.FRS_config['FRS_bridge_file']
            col_dict = {
                'REGISTRY_ID': "str",
                'PGM_SYS_ACRNM': "str",
                'PGM_SYS_ID': "str"
            }
            FRS_bridge = fmglob.read_FRS_file(file_, col_dict)
            eia860_FRS = fmglob.filter_by_program_list(
                df=FRS_bridge, program_list=["EIA-860"]
            )
            set_dir(f"{paths.local_path}/FRS_bridges")
            eia860_FRS.to_csv(
                f"{paths.local_path}/FRS_bridges/"
                f"{inventories_of_interest_str}.csv",
                encoding="utf-8-sig",
                index=False
            )
        ewf_df = pd.merge(
            left=emissions_and_wastes_by_facility,
            right=eia860_FRS,
            left_on="FRS_ID",
            right_on="REGISTRY_ID",
            how="left",
        )
        ewf_df.dropna(subset=["PGM_SYS_ID"], inplace=True)
        ewf_df.drop(
            columns=[
                "NEI_ID",
                "FRS_ID",
                "TRI_ID",
                "RCRAInfo_ID",
                "PGM_SYS_ACRNM",
                "REGISTRY_ID"],
            errors="ignore",
            inplace=True
        )
        ewf_df["FacilityID"] = ewf_df["PGM_SYS_ID"].astype(int)
        # HOTFIX: SettingWithCopyWarning [2024-03-12; TWD]
        emissions_and_waste_for_selected_eia_facilities = ewf_df[
            ewf_df["FacilityID"].isin(eia_facilities_to_include)].copy()
        emissions_and_waste_for_selected_eia_facilities.rename(
            columns={"FacilityID": "eGRID_ID"}, inplace=True)
        cems_df = ampd.generate_plant_emissions(model_specs.eia_gen_year)
        emissions_df = em_other.integrate_replace_emissions(
            cems_df, emissions_and_waste_for_selected_eia_facilities
        )
        emissions_df.rename(columns={"FacilityID": "eGRID_ID"}, inplace=True)
        facilities_w_fuel_region = eia_facility_fuel_region(
            model_specs.eia_gen_year)
        facilities_w_fuel_region.rename(
            columns={'FacilityID': 'eGRID_ID'}, inplace=True)
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

    final_database = pd.merge(
        left=emissions_df,
        right=generation_data,
        on=["eGRID_ID", "Year"],
        how="left",
    )
    final_database = pd.merge(
        left=final_database,
        right=facilities_w_fuel_region,
        on="eGRID_ID",
        how="left",
        suffixes=["", "_right"],
    )

    if model_specs.replace_egrid:
        primary_fuel_df = eia923_primary_fuel(year=model_specs.eia_gen_year)
        primary_fuel_df.rename(columns={'Plant Id':"eGRID_ID"},inplace=True)
        primary_fuel_df["eGRID_ID"] = primary_fuel_df["eGRID_ID"].astype(int)
        key_df = (
            primary_fuel_df[["eGRID_ID", "FuelCategory"]]
            .dropna()
            .drop_duplicates(subset="eGRID_ID")
            .set_index("eGRID_ID")
        )
        final_database["FuelCategory"] = final_database["eGRID_ID"].map(
            key_df["FuelCategory"])
    else:
        key_df = (
            final_database[["eGRID_ID", "FuelCategory"]]
            .dropna()
            .drop_duplicates(subset="eGRID_ID")
            .set_index("eGRID_ID")
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
    final_database = map_emissions_to_fedelemflows(final_database)
    dup_cols_check = [
        "eGRID_ID",
        "FuelCategory",
        "FlowName",
        "FlowAmount",
        "Compartment",
    ]
    final_database = final_database.loc[
        :, ~final_database.columns.duplicated()
    ]
    final_database = final_database.drop_duplicates(subset=dup_cols_check)
    drop_columns = ['PrimaryFuel_right', 'FuelCategory', 'FuelCategory_right']
    drop_columns = [
        c for c in drop_columns if c in final_database.columns.values.tolist()]
    final_database.drop(columns=drop_columns, inplace=True)
    final_database.rename(
        columns={"Final_fuel_agg": "FuelCategory",},
        inplace=True,
    )
    final_database = add_temporal_correlation_score(
        final_database, model_specs.electricity_lci_target_year)
    final_database = add_technological_correlation_score(final_database)
    final_database["DataCollection"] = 5
    final_database["GeographicalCorrelation"] = 1

    # For surety sake
    final_database["eGRID_ID"] = final_database["eGRID_ID"].astype(int)

    final_database.sort_values(
        by=["eGRID_ID", "Compartment", "FlowName"], inplace=True
    )
    final_database["stage_code"] = "Power plant"
    final_database["Compartment_path"] = final_database["Compartment"]
    final_database["Compartment"] = final_database["Compartment_path"].map(
        COMPARTMENT_DICT
    )
    final_database["Balancing Authority Name"] = final_database[
        "Balancing Authority Code"].map(BA_CODES["BA_Name"])
    final_database["EIA_Region"] = final_database[
        "Balancing Authority Code"].map(BA_CODES["EIA_Region"])
    final_database["FERC_Region"] = final_database[
        "Balancing Authority Code"].map(BA_CODES["FERC_Region"])

    # Apply the "manual edits"
    # See GitHub issues #212, #121, and #77.
    # https://github.com/USEPA/ElectricityLCI/issues/
    final_database = edits.check_for_edits(
        final_database, "generation.py", "create_generation_process_df")

    return final_database


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
        Facility-level emissions as generated by created by
        create_generation_process_df
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
        - 'uncertaintyLognormParams' (tuple): geo-mean, zero, and CI_max
        - 'electricity_sum' (float): aggregated electricity gen (MWh)
        - 'electricity_mean' (float): mean electricity gen (MWh)
        - 'facility_count' (float): count of facilities for electricity stats
        - 'Emission_factor' (float): emission amount per MWh
        - 'GeomMean' (float): geometric mean of emission factor (units/MWh)
        - 'GeomSD' (float): geometric standard deviation of emission factor
    """
    # """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # SUBMODULES
    # """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    def _geometric_mean(p_series, df, cols):
        """Perform logarithmic calculations on a data series.

        Parameters
        ----------
        p_series : pandas.Series
            A series of numerical data (e.g., emission factors), from which
            the geometric mean and a type of confidence interval are
            computed.
        df : pandas.DataFrame
            The full data frame from which the series was extracted.
            Used only in debugging messages.
        cols : list
            The list of relevant columns (used in the groupby method).
            Used only in debugging messages.

        Returns
        -------
        tuple
            A tuple of length three.

            1. The geometric mean of the series.

            .. math::

                \mu_g = \exp(\frac{\sum(\ln x)}{N})

            2. Zero
            3. A type of upper confidence interval.

            .. math::

                \exp\left[\max\left(
                \bar{d}
                + \frac{\hat{s}^2}{2}
                \pm (\bar{d} + t_{\alpha/2,2} \times se_\bar{d})
                \sqrt{\hat{s}^2 \left[
                \frac{1}{N} + \frac{\hat{s}^2}{2(N-1)}
                \right]}
                \right)\right]

        Notes
        -----
        Alternatively scipy.stats.lognorm may be used to fit a distribution
        and provide the parameters.

        There are three parameters fit to a lognormal distribution, they are:

        - shape: controls the skewness of the distribution (-1 to +1)
        - scale: controls the spread of the distribution
        - location: determines the mean or median of the distribution

        The geometric standard deviation is non-negative, unitless
        multiplicative factor that is a measure of the spread of logarithmic
        values around the mean that acts as an indicator of volatility or risk
        (>=1), which is good for non-normally distribution data.

        Consider this:

        1)  Use scipy.stats.lognorm.fit(data) to get the three-parameter fit
            of the lognormal distribution.

            -   shape, s (i.e., sigma of the normal distribution,
                Y = ln(X - loc) ~ N(mu, sigma))
            -   location, loc
            -   scale (i.e., e^mu, where mu is the mean of the normal
                distribution, Y = ln(X - loc) ~ N(mu, sigma)); scale is 1 when
                mu is zero.

        2) Use scipy.stats.lognorm.interval to calculate the confidence
           interval of the sample.
        """
        # Check series length and data value requirements
        #   Not sure why we care about the median, other than it being the
        #   scale parameter.
        l = len(p_series)
        if (l > 3) & (p_series.quantile(0.5) > 0):
            logging.debug(
                "Calculating confidence interval for: " +
                " - ".join(df.loc[p_series.index[0], cols].values)
            )
            logging.debug(f"Count: {l}")
            with np.errstate(all='raise'):
                try:
                    data = p_series.to_numpy()
                except (ArithmeticError, ValueError, FloatingPointError):
                    logging.debug("Problem with input data")
                    return None

                try:
                    log_data = np.log(data)
                except (ArithmeticError, ValueError, FloatingPointError):
                    # See Duke Energy Carolinas and Duke Energy Florida, Inc.
                    #  - SOLAR - Power plant - Acid compounds
                    logging.debug("Problem with log function")
                    return None

                try:
                    mean = np.mean(log_data)
                except (ArithmeticError, ValueError, FloatingPointError):
                    logging.debug("Problem with mean function")
                    return None

                try:
                    # Simplified geometric standard deviation; scale parameter
                    se = np.std(log_data)/np.sqrt(l)
                    sd2 = se**2
                except (ArithmeticError, ValueError, FloatingPointError):
                    # See Arizona Public Service Company - WIND
                    # and California Independent System Operator - MIXED
                    logging.debug("Problem with std function")
                    return None

                try:
                    # Compute confidence interval for the logarithm of the data
                    # using the mean and standard deviation of the logarithms
                    # of the data.
                    # BUG: why is df = l-2? Only one parameter is being
                    # estimated, so df should be l=1.
                    # Hotfix syntax (alpha==confidence); 2023-11-06 [TWD]
                    _, pi2 = t.interval(0.9, df=l - 2, loc=mean, scale=se)
                except (ArithmeticError, ValueError, FloatingPointError):
                    # See California Independent Operator - GEOTHERMAL
                    logging.debug("Problem with t function")
                    return None

                try:
                    upper_interval = np.max(
                        [
                            mean
                            + sd2/2
                            + pi2*np.sqrt(sd2/l + sd2**2/(2 * (l-1))),
                            mean
                            + sd2/2
                            - pi2*np.sqrt(sd2/l + sd2**2/(2 * (l-1))),
                        ]
                    )
                except:
                    logging.debug("Problem with interval function")
                    return None
                try:
                    # NOTE:
                    # np.exp(mean) is the geometric mean of the series
                    result = (np.exp(mean), 0, np.exp(upper_interval))
                except (ArithmeticError, ValueError, FloatingPointError):
                    logging.debug("Unable to calculate geometric_mean")
                    return None
                if result is not None:
                    return result
                else:
                    logging.debug(
                        f"Problem generating uncertainty parameters \n"
                        f"{df.loc[p_series.index[0], cols].values}\n"
                        f"{p_series.values}"
                        f"{p_series.values+1}"
                    )
                    return None
        else:
            logging.debug(
                "Skipping confidence interval for: " +
                " - ".join(df.loc[p_series.index[0], cols].values)
            )
            logging.debug(f"Count: {l}")
            return None


    def _calc_geom_std(df):
        """Location adjusted geometric mean and standard deviation.

        Parameters
        ----------
        df : pandas.Series
            A data series for aggregated (or disaggregated) emissions,
            including variables for 'uncertaintyLognormParams' (as calculated
            by :func:`_geometric_mean`), 'Emission_factor' (emission amounts
            per MWh),

        Returns
        -------
        tuple
            Geometric mean : string or None
            Geometric standard deviation : string or None

        Notes
        -----
        -   Values are returned as strings, not floats, in order to use
            NoneType filtering.
        -   Depends on global variables (e.g, region_agg) defined within the
            scope of :func:`aggregate_data`.
        """
        if region_agg is not None:
            debug_string = (
                f"{df[region_agg].values[0]} - "
                f"{df['FuelCategory']} - {df['FlowName']}")
        else:
            debug_string = f"{df['FuelCategory']} - {df['FlowName']}"
        logging.debug(debug_string)

        if df["uncertaintyLognormParams"] is None:
            return (None, None)

        # BUG: unused variable, params [2023-11-09; TWD]
        if isinstance(df["uncertaintyLognormParams"], str):
            params = ast.literal_eval(df["uncertaintyLognormParams"])

        try:
            length = len(df["uncertaintyLognormParams"])
        except TypeError:
            logging.warning(
                f"Error calculating length of uncertaintyLognormParams"
                f"{df['uncertaintyLognormParams']}"
            )
            return (None, None)

        if length != 3:
            logging.warning(
                f"Error estimating standard deviation - length: {length}"
            )
        else:
            # In some cases, the final emission factor is far different than
            # the geometric mean of the individual emission factor.
            # Depending on the severity, this could be a clear sign of outliers
            # having a large impact on the final emission factor.
            # When the uncertainty is generated for these cases, the results
            # can be nonsensical - hence we skip them.
            # A more agressive approach would be to re-assign the emission
            # factor as well.
            if df["Emission_factor"] > df["uncertaintyLognormParams"][2]:
                # NOTE: the majority of the values filtered out by this
                # are only fractionally over the confidence interval.
                # Exceptions include, Bonneville Power Administration -
                # GEOTHERMAL - Coal, anthracite, with a value of 10 and
                # a CI limit of 1.3e-5. Carbon dioxide emissions may be
                # +100 over the CI limit (e.g., 869 > 739).
                logging.warning(
                    "Emission factor exceeds confidence limit! "
                    "%s" % debug_string)
                logging.debug("%s > %s" % (
                    df['Emission_factor'], df["uncertaintyLognormParams"][2]))
                return (None, None)
            else:
                # The CDF for a lognormal distribution is:
                # CDF = 0.5*(1 + erf[(ln(x) - mu)/(sqrt(2)*sigma)])
                # substitute: x = EF + (1+PI); mu = ln(EF) - 0.5*sigma**2
                # and set CDF = 0.95:
                # 0.9=erf{[ln(EF*(1+PI)-ln(EF)+0.5*sigma**2)]/(sqrt(2)*sigma)}
                # Know that the erfinv(0.9) = 1.163087, solve for sigma:
                # 1.163 = [ln(EF*(1+PI))-ln(EF)+0.5*sigma**2]/(sqrt(2)*sigma)
                # Rearrange in quadratic form:
                # 0 = 0.5*sigma**2 + (-sqrt(2)*1.163)*sigma + ln(1 + PI),
                # where PI is the upper prediction inverval expressed as a
                # fraction. For example, PI=90% would be 0.9, such that:
                # EF*(1+PI) = EF + 0.9*EF.
                # HOTFIX 'c' parameter based on the simplification above [TWD]
                a = 0.5
                b = -2**0.5*erfinv(2*0.95-1)
                c = np.log(1 + df["uncertaintyLognormParams"][2])
                # HOTFIX: avoid invalid value encountered in scalar power;
                # this happens when a square root is taken of a negative.
                abc = (b**2 - 4*a*c)
                sd1 = float("nan")
                sd2 = float("nan")
                if abc >= 0:
                    sd1 = (-b + abc**0.5)/(2*a)
                    sd2 = (-b - abc**0.5)/(2*a)
                # HOTFIX: correct check for nan [2024-03-28; TWD]
                if sd1 == sd1 and sd2 == sd2:
                    if sd1 < sd2:
                        geostd = np.exp(sd1)
                        geomean = np.exp(
                            np.log(df["Emission_factor"]) - 0.5*sd1**2)
                    else:
                        geostd = np.exp(sd2)
                        geomean = np.exp(
                            np.log(df["Emission_factor"]) - 0.5*sd2**2)
                elif sd1 == sd1 and sd2 != sd2:
                    geostd = np.exp(sd1)
                    geomean = np.exp(np.log(df["Emission_factor"]) - 0.5*sd1**2)
                elif sd2 == sd2 and sd1 != sd1:
                    geostd = np.exp(sd2)
                    geomean = np.exp(np.log(df["Emission_factor"]) - 0.5*sd2**2)
                else:
                    return (None, None)

                if (
                    (geostd is np.inf)
                    or (geostd is np.NINF)
                    or (geostd != geostd)  # HOTFIX: nan check [240328;TWD]
                    or str(geostd) == "nan"
                    or (geostd == 0)
                ):
                    return (None, None)
                return str(geomean), str(geostd)


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
        try:
            wts = total_db.loc[pdser.index, "FlowAmount"]
            result = np.average(pdser, weights=wts)
        except:
            logging.debug(
                f"Error calculating weighted mean for {pdser.name}-"
                f"likely from 0 FlowAmounts"
                # f"{total_db.loc[pdser.index[0],cols]}"
            )
            try:
                with np.errstate(all='raise'):
                    result = np.average(pdser)
            except ArithmeticError or ValueError or FloatingPointError:
                result = float("nan")
        return result


    # """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # MAIN METHOD
    # """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
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

    # Inject a false flow amount for "uncertainty" calculations
    false_gen = 1e-15
    # HOTFIX: pandas futurewarning syntax [2024-03-08; TWD]
    #total_db = total_db.replace({"FlowAmount": 0}, false_gen)

    # Assign data score based on percent generation
    total_db = add_data_collection_score(total_db, electricity_df, subregion)

    # Calculate the emission factor (E/MWh)
    # HOTFIX ZeroDivisionError [2024-05-14; TWD]
    crit_zero = total_db["Electricity"] != 0
    total_db.loc[crit_zero, "facility_emission_factor"] = (
        total_db.loc[crit_zero, "FlowAmount"]
        / total_db.loc[crit_zero, "Electricity"]
    )
    # Effectively removes rows with zero Electricity or nan flow amounts.
    # For 2016 generation, it's all caused by nans in flow amounts.
    # For 2020 generation, it's all zero electricity
    total_db.dropna(subset=["facility_emission_factor"], inplace=True)

    # TODO: replace geo_mean w/ Hawkins-Young variant. It should return
    #       just sigma for use in the mu and mu_g calculations.

    wm = lambda x: _wtd_mean(x, total_db)
    geo_mean = lambda x: _geometric_mean(x, total_db, groupby_cols)
    geo_mean.__name__ = "geo_mean"
    logging.info(
        "Aggregating flow amounts, dqi information, and calculating uncertainty"
    )

    # NOTE: lots of runtime warnings
    database_f3 = total_db.groupby(
        groupby_cols + ["Year", "source_string"], as_index=False
    ).agg({
        "FlowAmount": ["sum", "count"],
        "TemporalCorrelation": wm,
        "TechnologicalCorrelation": wm,
        "GeographicalCorrelation": wm,
        "DataCollection": wm,
        "DataReliability": wm,
        "facility_emission_factor": ["min", "max", geo_mean],
    })

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
        "uncertaintyLognormParams",
    ]

    criteria = database_f3["Compartment"] == "input"
    database_f3.loc[criteria, "uncertaintyLognormParams"] = None

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
    canadian_criteria = database_f3["FuelCategory"] == "ALL"
    if region_agg:
        canada_db = pd.merge(
            left=database_f3.loc[canadian_criteria, :],
            right=total_db[groupby_cols + ["Electricity"]],
            left_on=groupby_cols,
            right_on=groupby_cols,
            how="left",
        ).drop_duplicates(subset=groupby_cols)
    else:
        total_grouped = total_db.groupby(
            by=groupby_cols, as_index=False)["Electricity"].sum()
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

    if region_agg is not None:
        database_f3["GeomMean"], database_f3["GeomSD"] = zip(
            *database_f3[
                [
                    "Emission_factor",
                    "uncertaintyLognormParams",
                    "uncertaintyMin",
                    "uncertaintyMax",
                    "FuelCategory",
                    "FlowName"
                ] + region_agg
            ].apply(_calc_geom_std, axis=1)
        )
    else:
        database_f3["GeomMean"], database_f3["GeomSD"] = zip(
            *database_f3[
                [
                    "Emission_factor",
                    "uncertaintyLognormParams",
                    "uncertaintyMin",
                    "uncertaintyMax",
                    "FuelCategory",
                    "FlowName"
                ]
            ].apply(_calc_geom_std, axis=1)
        )
    database_f3.sort_values(by=groupby_cols, inplace=True)
    return database_f3


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
    """Turn aggregated emission data into a dictionary for openLCA.

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
        - uncertaintyLognormParams
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
        "pedigreeUncertainty",
        "dqEntry",
        "uncertainty",
        "comment",
    ]

    data["internalId"] = ""
    data["@type"] = "Exchange"
    data["avoidedProduct"] = False
    data["flowProperty"] = ""
    data["baseUncertainty"] = ""
    data["provider"] = ""
    data["unit"] = data["Unit"]
    data["FlowType"]="ELEMENTARY_FLOW"

    # Effectively rename 'uncertainty Max/Min' to 'Max/Min'
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
    product_filter=(
        (data["Compartment"].str.lower().str.contains("technosphere"))
        |(data["Compartment"].str.lower().str.contains("valuable"))
    )
    data.loc[product_filter, "FlowType"] = "PRODUCT_FLOW"

    # Define wastes based on compartment label
    waste_filter = (
        (data["Compartment"].str.lower().str.contains("technosphere"))
    )
    data.loc[waste_filter, "FlowType"] = "WASTE_FLOW"

    provider_filter = data["stage_code"].isin(upstream_dict.keys())
    for index, row in data.loc[provider_filter, :].iterrows():
        provider_dict = {
            "name": upstream_dict[getattr(row, "stage_code")]["name"],
            "categoryPath": upstream_dict[getattr(row, "stage_code")][
                "category"
            ],
            "processType": "UNIT_PROCESS",
            "@id": upstream_dict[getattr(row, "stage_code")]["uuid"],
        }
        data.at[index, "provider"] = provider_dict
        data.at[index, "unit"] = unit(
            upstream_dict[getattr(row, "stage_code")]["q_reference_unit"]
        )
        data.at[index, "FlowType"] = "PRODUCT_FLOW"

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
    data_dict = data_for_dict.to_dict("records")
    # HOTFIX: append the product flow dictionary to the list [2023-11-13; TWD]
    data_dict.append(ref_exchange_creator())
    return data_dict


def olcaschema_genprocess(database, upstream_dict={}, subregion="BA"):
    """Turn a database containing generator facility emissions into a
    dictionary that contains required data for an openLCA-compatible JSON-LD.

    Additionally, default providers for fuel inputs are mapped using the information contained in the dictionary containing openLCA-formatted
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
    if region_agg:
        base_cols = region_agg + fuel_agg
    else:
        base_cols = fuel_agg
    non_agg_cols = [
        "stage_code",
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
        "uncertaintyLognormParams",
        "Emission_factor",
        "GeomMean",
        "GeomSD",
    ]

    # Create a data frame with one massive column of exchanges
    database_groupby = database.groupby(by=base_cols)
    process_df = pd.DataFrame(
        database_groupby[non_agg_cols].apply(
            turn_data_to_dict,
            (upstream_dict))
    )
    process_df.columns = ["exchanges"]
    process_df.reset_index(inplace=True)
    process_df["@type"] = "Process"
    process_df["allocationFactors"] = ""
    process_df["defaultAllocationMethod"] = ""
    # HOTFIX: remove .values, which throws ValueError [2023-11-13; TWD]
    process_df["location"] = process_df[region_agg]
    process_df["parameters"] = ""
    process_df["processType"] = "UNIT_PROCESS"
    # HOTFIX: add squeeze to force DataFrame to Series [2023-11-13; TWD]
    process_df["category"] = (
        "22: Utilities/2211: Electric Power Generation, "
        "Transmission and Distribution/" + process_df[fuel_agg].squeeze().values
    )
    if region_agg is None:
        process_df["description"] = (
            "Electricity from "
            + process_df[fuel_agg].squeeze().values
            + " produced at generating facilities in the US."
        )
        process_df["name"] = (
            "Electricity - " + process_df[fuel_agg].squeeze().values + " - US"
        )
    else:
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
