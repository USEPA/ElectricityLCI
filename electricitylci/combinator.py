#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# combinator.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

import pandas as pd

from electricitylci.globals import output_dir, data_dir
from electricitylci.import_impacts import generate_canadian_mixes
from electricitylci.model_config import model_specs
from electricitylci.eia860_facilities import eia860_balancing_authority
import electricitylci.coal_upstream as coal
import electricitylci.natural_gas_upstream as ng
import electricitylci.petroleum_upstream as petro
import electricitylci.geothermal as geo
import electricitylci.solar_upstream as solar
import electricitylci.wind_upstream as wind
import electricitylci.nuclear_upstream as nuke
from electricitylci.generation import add_temporal_correlation_score

import fedelemflowlist as fedefl


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module contains several utility methods for combining
data from different sources, such as power plant-level emissions
provided by Standardized Emission and Waste Inventories (StEWI), NETL
fuel emissions (e.g., coal mining/transport, natural gas extraction,
nuclear fuel cycle), and maps emissions based on the Federal LCA Commons
Elementary Flow List in order to provide life cycle inventory.

Last edited: 2023-12-19
"""


##############################################################################
# GLOBALS
##############################################################################
module_logger = logging.getLogger("combinator.py")

# This was added to populate a ba_codes variable that could be used
# by other modules without having to re-read the excel files. The purpose
# is to try and provide a common source for balancing authority names, as well
# as FERC an EIA region names.
ba_codes = pd.concat([
    pd.read_excel(
        f"{data_dir}/BA_Codes_930.xlsx", header=4, sheet_name="US"
    ),
    pd.read_excel(
        f"{data_dir}/BA_Codes_930.xlsx", header=4, sheet_name="Canada"
    ),
])
ba_codes.rename(
    columns={
        "etag ID": "BA_Acronym",
        "Entity Name": "BA_Name",
        "NCR_ID#": "NRC_ID",
        "Region": "Region",
    },
    inplace=True,
)
ba_codes.set_index("BA_Acronym", inplace=True)


##############################################################################
# FUNCTIONS
##############################################################################
def fill_nans(
        df,
        eia_gen_year,
        key_column="FacilityID",
        target_columns=[],
        dropna=True):
    """Fills nan values for the specified target columns by using the data from
    other rows, using the key_column for matches. There is an extra step
    to fill remaining nans for the state column because the module to calculate
    transmission and distribution losses needs values in the state column to
    work.

    Parameters
    ----------
    df : dataframe
        Dataframe containing nans and at a minimum the columns key_column and
        target_columns
    key_column : str, optional
        The column to match for the data to fill target_columns, by default "FacilityID"
    target_columns : list, optional
        A list of columns with nans to fill, by default []. If empty, the function
        will use a pre-defined set of columns.
    dropna : bool, optional
        After nans are filled, drop rows that still contain nans in the
        target columns, by default True

    Returns
    -------
    dataframe: hopefully with all of the nans filled.
    """
    if not target_columns:
        target_columns = [
            "Balancing Authority Code",
            "Balancing Authority Name",
            "FuelCategory",
            "NERC",
            "PercentGenerationfromDesignatedFuelCategory",
            "eGRID_ID",
            "Subregion",
            "FERC_Region",
            "EIA_Region",
            "State",
            "Electricity",
        ]
    confirmed_target = []
    for x in target_columns:
        if x in df.columns:
            confirmed_target.append(x)
        else:
            module_logger.debug(f"Column {x} is not in the dataframe")
    if key_column not in df.columns:
        module_logger.debug(
            f"Key column '{key_column}' is not in the dataframe"
        )
        raise KeyError
    for col in confirmed_target:
        key_df = (
            df[[key_column, col]]
            .dropna()
            .drop_duplicates(subset=key_column)
            .set_index(key_column)
        )
        df.loc[df[col].isnull(), col] = df.loc[
            df[col].isnull(), key_column
        ].map(key_df[col])
    plant_ba = eia860_balancing_authority(eia_gen_year).set_index("Plant Id")
    plant_ba.index = plant_ba.index.astype(int)
    if "State" not in df.columns:
        df["State"] = float("nan")
        confirmed_target.append("State")
    df.loc[df["State"].isna(), "State"] = df.loc[
        df["State"].isna(), "eGRID_ID"].map(plant_ba["State"])
    if dropna:
        df.dropna(subset=confirmed_target, inplace=True, how="all")
        df.dropna(subset=["Electricity"],inplace=True)
    return df


def concat_map_upstream_databases(eia_gen_year, *arg, **kwargs):
    """Concatenate and map all of the databases given as args.

    All of the emissions in the combined database are mapped to the
    federal elementary flows list based on the mapping file 'eLCI' in
    preparation for being turned into openLCA processes and combined with
    the generation emissions.

    Parameters
    ----------
    eia_gen_year : int
        Becomes the 'Year' column in the returned data frame.
    *arg : tuple
        Tuple of pandas.DataFrame objects to be combined, generated by the
        upstream modules or renewables modules (e.g., .nuclear_upstream,
        .petroleum_upstream, and .solar_upstream).
    **kwargs : dict
        A dictionary of named arguments. The key 'group_name' triggers a
        tuple to be returned, rather than just a data frame (see notes).

    Returns
    -------
    pandas.DataFrame or tuple
        The data frame contains the columns: 'plant_id', 'FuelCategory','stage_code', 'FlowName', 'Compartment', 'Compartment_path', 'FlowUUID', 'Unit', 'ElementaryFlowPrimeContext', 'FlowAmount', 'quantity', 'Source', 'Year', 'Electricity', and 'input'.

    Notes
    -----
    If 'group_name' is provided in kwargs, then the function will return a
    tuple containing the mapped dataframe and lists of tuples for the unique
    mapped and unmapped flows, as well as write the results to text file.

    For EIA generation year 2016, there is a reported 2375 unmatched and
    2036 matched flows for renewable energy power plants.

    Examples
    --------
    >>> import electricitylci.geothermal as geo
    >>> import electricitylci.solar_upstream as solar
    >>> import electricitylci.wind_upstream as wind
    >>> import electricitylci.solar_thermal_upstream as solartherm
    >>> eia_gen_year = config.model_specs.eia_gen_year
    >>> geo_df = geo.generate_upstream_geo(eia_gen_year)
    >>> solar_df = solar.generate_upstream_solar(eia_gen_year)
    >>> wind_df = wind.generate_upstream_wind(eia_gen_year)
    >>> solartherm_df = solartherm.generate_upstream_solarthermal(eia_gen_year)
    >>> netl_gen, u_list, m_list = combine.concat_map_upstream_databases(
    ...    eia_gen_year, geo_df, solar_df, wind_df, solartherm_df,
    ...    group_name='renewable')
    """
    mapped_column_dict = {
        "TargetFlowName": "FlowName",
        "TargetFlowUUID": "FlowUUID",
        "TargetFlowContext": "Compartment",
        "TargetUnit": "Unit",
    }
    # NOTE: input has no compartment
    compartment_mapping = {
        "air": "emission/air",
        "water": "emission/water",
        "ground": "emission/ground",
        "soil": "emission/ground",
        "resource": "resource",
        "NETL database/emissions": "NETL database/emissions",
        "NETL database/resources": "NETL database/resources",
    }
    module_logger.info(
        f"Concatenating and flow-mapping {len(arg)} upstream databases.")
    upstream_df_list = list()
    # HOTFIX: index out data frames from tuple before mutate [2023-12-21; TWD]
    for i in range(len(arg)):
        df = arg[i]
        if isinstance(df, pd.DataFrame):
            if "Compartment_path" not in df.columns:
                # HOTFIX: simplify new column definition [2023-12-21; TWD]
                df["Compartment_path"] = df.loc[:, "Compartment"].map(
                    compartment_mapping)
            upstream_df_list.append(df)
    upstream_df = pd.concat(upstream_df_list, ignore_index=True, sort=False)

    module_logger.info("Creating flow mapping database")
    flow_mapping = fedefl.get_flowmapping('eLCI')
    flow_mapping["SourceFlowName"] = flow_mapping["SourceFlowName"].str.lower()

    module_logger.info("Preparing upstream df for merge")
    upstream_df["FlowName_orig"] = upstream_df["FlowName"]
    upstream_df["Compartment_orig"] = upstream_df["Compartment"]
    upstream_df["Compartment_path_orig"] = upstream_df["Compartment_path"]
    upstream_df["Unit_orig"] = upstream_df["Unit"]
    upstream_df["FlowName"] = upstream_df["FlowName"].str.lower().str.rstrip()
    upstream_df["Compartment"] = (
        upstream_df["Compartment"].str.lower().str.rstrip()
    )
    upstream_df["Compartment_path"] = (
        upstream_df["Compartment_path"].str.lower().str.rstrip()
    )
    upstream_df["Unit"].fillna("<blank>", inplace=True)
    upstream_columns = upstream_df.columns

    module_logger.info("Grouping upstream database")
    groupby_cols = [
        "fuel_type",
        "stage_code",
        "FlowName",
        "Compartment",
        "input",
        "plant_id",
        "Compartment_path",
        "Unit",
        "FlowName_orig",
        "Compartment_path_orig",
        "Unit_orig",
    ]
    upstream_df["FlowAmount"] = upstream_df["FlowAmount"].astype(float)
    if "Electricity" in upstream_df.columns:
        upstream_df_grp = upstream_df.groupby(
            groupby_cols, as_index=False
        ).agg({"FlowAmount": "sum", "quantity": "mean", "Electricity": "mean"})
    else:
        upstream_df_grp = upstream_df.groupby(
            groupby_cols, as_index=False
        ).agg({"FlowAmount": "sum", "quantity": "mean"})
    upstream_df = upstream_df[
        ["FlowName_orig", "Compartment_path_orig", "stage_code"]
    ]
    module_logger.info("Merging upstream database and flow mapping")
    upstream_mapped_df = pd.merge(
        left=upstream_df_grp,
        right=flow_mapping,
        left_on=["FlowName", "Compartment_path"],
        right_on=["SourceFlowName", "SourceFlowContext"],
        how="left",
    )
    del(upstream_df_grp, flow_mapping)
    upstream_mapped_df.drop(
        columns={"FlowName", "Compartment", "Unit"}, inplace=True
    )
    upstream_mapped_df = upstream_mapped_df.rename(
        columns=mapped_column_dict, copy=False
    )
    upstream_mapped_df.drop_duplicates(
        subset=["plant_id", "FlowName", "Compartment_path", "FlowAmount"],
        inplace=True,
    )
    upstream_mapped_df.dropna(subset=["FlowName"], inplace=True)

    module_logger.info("Applying conversion factors")
    upstream_mapped_df["FlowAmount"] = (
        upstream_mapped_df["FlowAmount"]
        * upstream_mapped_df["ConversionFactor"]
    )

    upstream_mapped_df.rename(
        columns={"fuel_type": "FuelCategory"}, inplace=True
    )
    upstream_mapped_df["FuelCategory"] = upstream_mapped_df[
        "FuelCategory"
    ].str.upper()
    upstream_mapped_df["ElementaryFlowPrimeContext"] = "emission"
    upstream_mapped_df.loc[
        upstream_mapped_df["Compartment"].str.contains("resource"),
        "ElementaryFlowPrimeContext",
    ] = "resource"
    upstream_mapped_df["Source"] = "netl"
    upstream_mapped_df["Year"] = eia_gen_year
    final_columns = [
        "plant_id",
        "FuelCategory",
        "stage_code",
        "FlowName",
        "Compartment",
        "Compartment_path",
        "FlowUUID",
        "Unit",
        "ElementaryFlowPrimeContext",
        "FlowAmount",
        "quantity",
        "Source",
        "Year",
    ]
    if "Electricity" in upstream_columns:
        final_columns = final_columns + ["Electricity"]
    if "input" in upstream_columns:
        final_columns = final_columns + ["input"]

    # I added the section below to help generate lists of matched and unmatched
    # flows. Because of the groupby, it's expensive enough not to run everytime.
    # I didn't want to get rid of it in case it comes in handy later.
    if kwargs != {}:
        if "group_name" in kwargs:
            module_logger.info("kwarg group_name used: generating flows lists")
            unique_orig = upstream_df.groupby(
                by=["FlowName_orig", "Compartment_path_orig"]
            ).groups
            unique_mapped = upstream_mapped_df.groupby(
                by=[
                    "FlowName_orig",
                    "Compartment_path_orig",
                    "Unit_orig",
                    "FlowName",
                    "Compartment",
                    "Unit",
                ]
            ).groups
            unique_mapped_set = set(unique_mapped.keys())
            unique_orig_set = set(unique_orig.keys())
            unmatched_list = sorted(list(unique_orig_set - unique_mapped_set))
            matched_list = sorted(list(unique_mapped_set))
            fname_append = f"_{kwargs['group_name']}"
            out_path = f"{output_dir}/flowmapping_lists{fname_append}.txt"
            with open(out_path, "w") as f:
                f.write("Unmatched flows\n")
                if kwargs is not None:
                    if kwargs["group_name"] is not None:
                        f.write(f"From the group: {kwargs['group_name']}\n")
                for x in unmatched_list:
                    f.write(f"{x}\n")
                f.write("\nMatched flows\n")
                for x in matched_list:
                    f.write(f"{x}\n")
                f.close()
            module_logger.info("Flow mapping results written to %s" % out_path)
            upstream_mapped_df = upstream_mapped_df[final_columns]
            return upstream_mapped_df, unmatched_list, matched_list
    upstream_mapped_df = upstream_mapped_df[final_columns]
    return upstream_mapped_df


def concat_clean_upstream_and_plant(pl_df, up_df):
    """Combine the upstream and the generator (power plant) databases.

    Includes some database cleanup.

    Parameters
    ----------
    pl_df : dataframe
        The generator dataframe, generated by electricitylci.generation

    up_df : dataframe
        The combined upstream dataframe.

    Returns
    -------
    dataframe
    """
    region_cols = [
        "NERC",
        "Balancing Authority Code",
        "Balancing Authority Name",
        "Subregion",
    ]
    existing_region_cols=[x for x in pl_df.columns if x in region_cols]
    up_df = up_df.drop(columns=["eGRID_ID"], errors="ignore").merge(
        right=pl_df[["eGRID_ID"] + existing_region_cols].drop_duplicates(),
        left_on="plant_id",
        right_on="eGRID_ID",
        how="left",
    )
    combined_df = pd.concat([pl_df, up_df], ignore_index=True)
    combined_df["Balancing Authority Name"] = combined_df[
        "Balancing Authority Code"
    ].map(ba_codes["BA_Name"])
    combined_df["FERC_Region"] = combined_df["Balancing Authority Code"].map(
        ba_codes["FERC_Region"]
    )
    combined_df["EIA_Region"] = combined_df["Balancing Authority Code"].map(
        ba_codes["EIA_Region"]
    )
    categories_to_delete = [
        "plant_id",
        "FuelCategory_right",
        "Net Generation (MWh)",
        "PrimaryFuel_right",
    ]
    for x in categories_to_delete:
        try:
            combined_df.drop(columns=[x], inplace=True)
        except KeyError:
            module_logger.debug(f"Error deleting column {x}")
    combined_df["FacilityID"] = combined_df["eGRID_ID"]

    # I think without the following, given the way the data is created for
    # fuels, there are too many instances where fuel demand can be created
    # when no emissions are reported for the power plant. This should force
    # the presence of a power plant in the dataset for a fuel input to be
    # counted.
    combined_df.loc[
        ~(combined_df["stage_code"] == "Power plant"), "FuelCategory"
    ] = float("nan")
    # This allows construction impacts to be aligned to a power plant type -
    # not as import in openLCA but for analyzing results outside of openLCA.
    combined_df.loc[
        combined_df["FuelCategory"] == "CONSTRUCTION", "FuelCategory"
    ] = float("nan")
    combined_df = fill_nans(combined_df, model_specs.eia_gen_year)
    # The hard-coded cutoff is a workaround for now. Changing the parameter
    # to 0 in the config file allowed the inventory to be kept for generators
    # that are now being tagged as mixed.
    generation_filter = (
        combined_df["PercentGenerationfromDesignatedFuelCategory"]
        < model_specs.min_plant_percent_generation_from_primary_fuel_category / 100
    )
    if model_specs.keep_mixed_plant_category:
        combined_df.loc[generation_filter, "FuelCategory"] = "MIXED"
        combined_df.loc[generation_filter, "PrimaryFuel"] = "Mixed Fuel Type"
    else:
        combined_df = combined_df.loc[~generation_filter]
    return combined_df


def add_fuel_inputs(gen_df, upstream_df, upstream_dict):
    """Convert the upstream emissions database to fuel inputs and add them
    to the generator dataframe.

    This is in preparation of generating unit processes for openLCA.

    Parameters
    ----------
    gen_df : pandas.DataFrame
        The generator data frame containing power plant emissions (e.g., from
        :module:`generation`.\ :func:`create_generation_process_df`).
    upstream_df : pandas.DataFrame
        The combined upstream data frame (e.g., from
        :func:`get_upstream_process_df`).
    upstream_dict : dict
        This is the dictionary of upstream "unit processes" as generated by
        :func:`write_upstream_process_database_to_dict` after upstream_dict
        has been written to JSON-LD (e.g., with
        :func:`write_process_dicts_to_jsonld`).
        This is important because the UUIDs for the upstream "unit processes"
        are only generated when written to JSON-LD.

    Returns
    -------
    pandas.DataFrame
    """
    upstream_reduced = upstream_df.drop_duplicates(
        subset=["plant_id", "stage_code", "quantity"]
    )
    fuel_df = pd.DataFrame(columns=gen_df.columns)

    # The upstream reduced should only have one instance of each plant/stage
    # code combination. We'll first map the upstream dictionary to each plant
    # and then expand that dictionary into columns we can use. The goal is
    # to generate the fuels and associated metadata with each plant. That will
    # then be merged with the generation database.
    fuel_df["flowdict"] = upstream_reduced["stage_code"].map(upstream_dict)

    expand_fuel_df = fuel_df["flowdict"].apply(pd.Series)
    fuel_df.drop(columns=["flowdict"], inplace=True)

    fuel_df["Compartment"] = "input"
    fuel_df["FlowName"] = expand_fuel_df["q_reference_name"]
    fuel_df["stage_code"] = upstream_reduced["stage_code"]
    fuel_df["FlowAmount"] = upstream_reduced["quantity"]
    fuel_df["FlowUUID"] = expand_fuel_df["q_reference_id"]
    fuel_df["Unit"] = expand_fuel_df["q_reference_unit"]
    fuel_df["eGRID_ID"] = upstream_reduced["plant_id"]
    fuel_df["FacilityID"] = upstream_reduced["plant_id"]
    fuel_df["FuelCategory"] = upstream_reduced["FuelCategory"]
    fuel_df["Year"] = upstream_reduced["Year"]
    merge_cols = [
        "Age",
        "Balancing Authority Code",
        "Balancing Authority Name",
        "Electricity",
        "NERC",
        "Subregion",
    ]
    merge_cols = [x for x in merge_cols if x in fuel_df.columns]
    fuel_df.drop(columns=merge_cols, inplace=True)
    gen_df_reduced = gen_df[merge_cols + ["eGRID_ID"]].drop_duplicates(
        subset=["eGRID_ID"]
    )

    fuel_df = fuel_df.merge(
        right=gen_df_reduced,
        left_on="eGRID_ID",
        right_on="eGRID_ID",
        how="left",
    )
    fuel_df.dropna(subset=["Electricity"], inplace=True)
    fuel_df["Source"] = "eia"
    fuel_df = add_temporal_correlation_score(
        fuel_df, model_specs.electricity_lci_target_year)
    fuel_df["DataCollection"] = 5
    fuel_df["GeographicalCorrelation"] = 1
    fuel_df["TechnologicalCorrelation"] = 1
    fuel_df["DataReliability"] = 1
    fuel_df["ElementaryFlowPrimeContext"] = "input"
    fuel_cat_key = (
        gen_df[["FacilityID", "FuelCategory"]].drop_duplicates(
            subset="FacilityID").set_index("FacilityID")
    )
    fuel_df["FuelCategory"] = fuel_df["FacilityID"].map(
        fuel_cat_key["FuelCategory"]
    )
    gen_plus_up_df = pd.concat([gen_df, fuel_df], ignore_index=True)
    gen_plus_up_df = fill_nans(gen_plus_up_df, model_specs.eia_gen_year)
    # Taking out anything with New Brunswick System Operator so that
    # these fuel inputs (for a very small US portion of NBSO) don't get mapped
    # to the Canadian import rollup (i.e., double-counted)
    _ba_col = "Balancing Authority Name"
    _ba_name = "New Brunswick System Operator"
    gen_plus_up_df = gen_plus_up_df.loc[
        gen_plus_up_df[_ba_col] != _ba_name, :].reset_index(drop=True)

    return gen_plus_up_df


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    from electricitylci.generation import create_generation_process_df

    coal_df = coal.generate_upstream_coal(2016)
    ng_df = ng.generate_upstream_ng(2016)
    petro_df = petro.generate_petroleum_upstream(2016)
    geo_df = geo.generate_upstream_geo(2016)
    solar_df = solar.generate_upstream_solar(2016)
    wind_df = wind.generate_upstream_wind(2016)
    nuke_df = nuke.generate_upstream_nuc(2016)
    upstream_df = concat_map_upstream_databases(
        petro_df, geo_df, solar_df, wind_df, nuke_df
    )
    plant_df = create_generation_process_df()
    plant_df["stage_code"] = "Power plant"
    module_logger.info(plant_df.columns)
    module_logger.info(upstream_df.columns)
    combined_df = concat_clean_upstream_and_plant(plant_df, upstream_df)
    canadian_inventory = generate_canadian_mixes(combined_df)
    combined_df = pd.concat([combined_df, canadian_inventory])
    combined_df.sort_values(
        by=["eGRID_ID", "Compartment", "FlowName", "stage_code"], inplace=True
    )
    combined_df.to_csv(f"{output_dir}/combined_df.csv")
