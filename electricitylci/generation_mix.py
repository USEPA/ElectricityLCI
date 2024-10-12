#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# generation_mix.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging

import numpy as np
import pandas as pd

from electricitylci.globals import data_dir
from electricitylci.model_config import model_specs
from electricitylci.process_dictionary_writer import (
    exchange,
    exchange_table_creation_ref,
    exchange_table_creation_input_genmix,
    process_table_creation_genmix,
    exchange_table_creation_input_usaverage,
    process_table_creation_usaverage,
    exchange_table_creation_input_international_mix,
)


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """Get a subset of the egrid_facilities dataset.

The functions in this module calculate the fraction of each generating source
(either from generation data or straight from eGRID).

Last edited: 2024-10-11
"""


##############################################################################
# GLOBALS
##############################################################################
if not model_specs.replace_egrid:
    from electricitylci.egrid_facilities import egrid_facilities
    from electricitylci.egrid_facilities import egrid_subregions
    from electricitylci.egrid_energy import (
        ref_egrid_subregion_generation_by_fuelcategory,
    )

    # HOTFIX: make copy rather than slice [2023-12-21; TWD]
    egrid_facilities_w_fuel_region = egrid_facilities[[
        "FacilityID",
        "Subregion",
        "PrimaryFuel",
        "FuelCategory",
        "NERC",
        "PercentGenerationfromDesignatedFuelCategory",
        "Balancing Authority Name",
        "Balancing Authority Code",
    ]]

    # Get reference regional generation data by fuel type, add in NERC
    egrid_subregions_NERC = egrid_facilities[
        ["Subregion", "FuelCategory", "NERC"]]
    egrid_subregions_NERC = egrid_subregions_NERC.drop_duplicates()
    egrid_subregions_NERC = egrid_subregions_NERC[
        egrid_subregions_NERC["NERC"].notnull()
    ]
    ref_egrid_subregion_generation_by_fuelcategory_with_NERC = pd.merge(
        ref_egrid_subregion_generation_by_fuelcategory,
        egrid_subregions_NERC,
        on=["Subregion", "FuelCategory"],
    )

    ref_egrid_subregion_generation_by_fuelcategory_with_NERC = ref_egrid_subregion_generation_by_fuelcategory_with_NERC.rename(
        columns={"Ref_Electricity_Subregion_FuelCategory": "Electricity"}
    )


##############################################################################
# FUNCTIONS
##############################################################################
def create_generation_mix_process_df_from_model_generation_data(
        generation_data, subregion=None):
    """Create a fuel generation mix by subregion.

    Currently uses a dataframe 'egrid_facilities_w_fuel_region' that is not an
    input. This should be changed to an input so that the function can
    accommodate another data source.

    Parameters
    ----------
    generation_data : DataFrame
        [description]
    subregion : str
        Description of single region or group of regions.
        Options include 'all' for all eGRID subregions, 'NERC' for all NERC
        regions, 'BA' for all balancing authorities, or a single region
        (unclear if single region will work).

    Returns
    -------
    pandas.DataFrame
    """
    from electricitylci.combinator import BA_CODES
    from electricitylci.generation import eia_facility_fuel_region

    if subregion is None:
        subregion = model_specs.regional_aggregation

    # Converting to numeric for better stability and merging
    generation_data["FacilityID"] = generation_data["FacilityID"].astype(int)

    if model_specs.replace_egrid:
        year = model_specs.eia_gen_year
        # This will only add BA labels, not eGRID subregions
        fuel_region = eia_facility_fuel_region(year)
        fuel_region["FacilityID"] = fuel_region["FacilityID"].astype(int)
        generation_data = generation_data.loc[
            generation_data["Year"] == year, :
        ]
        database_for_genmix_final = pd.merge(
            generation_data, fuel_region, on="FacilityID"
        )
    else:
        egrid_facilities_w_fuel_region["FacilityID"] = egrid_facilities_w_fuel_region[
            "FacilityID"].astype(int)
        database_for_genmix_final = pd.merge(
            generation_data, egrid_facilities_w_fuel_region, on="FacilityID"
        )
    database_for_genmix_final["Balancing Authority Name"] = database_for_genmix_final[
        "Balancing Authority Code"].map(BA_CODES["BA_Name"])
    database_for_genmix_final["FERC_Region"] = database_for_genmix_final[
        "Balancing Authority Code"].map(BA_CODES["FERC_Region"])
    database_for_genmix_final["EIA_Region"] = database_for_genmix_final[
        "Balancing Authority Code"].map(BA_CODES["EIA_Region"])

    # Changing the structure of this function so that it uses pandas groupby
    if subregion == "NERC":
        database_for_genmix_final["Subregion"] = database_for_genmix_final[
            "NERC"
        ]
    elif subregion == "BA":
        database_for_genmix_final["Subregion"] = database_for_genmix_final[
            "Balancing Authority Name"
        ]
    elif subregion == "eGRID":
        database_for_genmix_final["Subregion"] = database_for_genmix_final[
            "eGRID"  # Value was "NERC", which I think is wrong
        ]
    elif subregion == "FERC":
        database_for_genmix_final["Subregion"] = database_for_genmix_final["FERC_Region"]

    if model_specs.keep_mixed_plant_category:
        mixed_criteria = (
                database_for_genmix_final[
                    "PercentGenerationfromDesignatedFuelCategory"]
                < model_specs.min_plant_percent_generation_from_primary_fuel_category/100
        )
        database_for_genmix_final.loc[mixed_criteria, "FuelCategory"] = "MIXED"
    if subregion == "US":
        group_cols = ["FuelCategory"]
    else:
        group_cols = ["Subregion", "FuelCategory"]
    if model_specs.keep_mixed_plant_category:
        pass
    subregion_fuel_gen = database_for_genmix_final.groupby(
        group_cols, as_index=False
    )["Electricity"].sum()

    # Groupby .transform method returns a dataframe of the same length as the
    # original
    if subregion == "US":
        subregion_total_gen = subregion_fuel_gen["Electricity"].sum()
    else:
        subregion_total_gen = subregion_fuel_gen.groupby("Subregion")[
            "Electricity"
        ].transform("sum")
    subregion_fuel_gen["Generation_Ratio"] = (
        subregion_fuel_gen["Electricity"] / subregion_total_gen
    )
    if subregion == "BA":
        # Dropping US generation data on New Brunswick System Operator (NBSO).
        # There are several US plants in NBSO and as a result, there is a
        # generation mix for the US side and the routines below generate one
        # for the Canada side. This manifests itself as a generation mix that
        # pulls 2 MWh for every 1MWh generated. For simplicity and because
        # NBSO is an imported BA, we'll remove the US-side and assume it's
        # covered under the Canadian imports.
        subregion_fuel_gen = subregion_fuel_gen.loc[
            subregion_fuel_gen["Subregion"] != "New Brunswick System Operator",
            :
        ]

    canada_list=[]
    #  NOTE: 'B.C. Hydro & Power Authority' shows up as
    # 'British Columbia Hydro and Power Authority' in ELCI_1
    canada_subregions = [
        "BCHA", # "B.C. Hydro & Power Authority"
        "HQT",  # "Hydro-Quebec TransEnergie"
        "MHEB", # "Manitoba Hydro"
        "NBSO", # "New Brunswick System Operator"
        "IESO", # "Ontario IESO"
    ]
    for bc in canada_subregions:
        reg = BA_CODES.loc[bc, "BA_Name"]
        canada_list.append((reg,"ALL",1.0,1.0))
    canada_df = pd.DataFrame(
        canada_list,
        columns=["Subregion","FuelCategory","Electricity","Generation_Ratio"]
    )
    subregion_fuel_gen = pd.concat(
        [subregion_fuel_gen,canada_df],
        ignore_index=True)
    return subregion_fuel_gen


def create_generation_mix_process_df_from_egrid_ref_data(subregion=None):
    """Create a fuel generation mix by subregion using egrid reference data.

    This is only possible for a subregion, NERC region, or total US.

    Parameters
    ----------
    generation_data : pandas.DataFrame
    subregion : str
        Description of single region or group of regions.

    Returns
    -------
    DataFrame
        Dataframe contains the fraction of various generation technologies
        to produce 1 MWh of electricity.
    """
    if subregion is None:
        subregion = model_specs.regional_aggregation
    # Converting to numeric for better stability and merging
    if subregion == "eGRID":
        regions = egrid_subregions
    elif subregion == "NERC":
        regions = list(
            pd.unique(
                ref_egrid_subregion_generation_by_fuelcategory_with_NERC[
                    "NERC"
                ]
            )
        )
    else:
        regions = [subregion]

    result_database = pd.DataFrame()

    for reg in regions:
        if subregion == "NERC":
            # This makes sure that the dictionary writer works fine because it only works with the subregion column.
            database = ref_egrid_subregion_generation_by_fuelcategory_with_NERC[
                ref_egrid_subregion_generation_by_fuelcategory_with_NERC[
                    "NERC"
                ]
                == reg
            ]
            database["Subregion"] = database["NERC"]
        elif subregion == "US":
            # for all US use entire database
            database = ref_egrid_subregion_generation_by_fuelcategory_with_NERC
        else:
            # assume the region is an egrid subregion
            database = ref_egrid_subregion_generation_by_fuelcategory_with_NERC[
                ref_egrid_subregion_generation_by_fuelcategory_with_NERC[
                    "Subregion"
                ]
                == reg
            ]

        # Get fuel typoes for each region
        # region_fuel_categories = list(pd.unique(database['FuelCategory']))
        total_gen_reg = np.sum(database["Electricity"])
        database["Generation_Ratio"] = database["Electricity"] / total_gen_reg
        # for index,row in database.iterrows():
        #     cropping the database according to the current fuel being considered
        #     row['Generation_Ratio'] = row['Electricity']/total_gen_reg
        result_database = pd.concat([result_database, database])

    return result_database


def olcaschema_genmix(database, gen_dict, subregion=None):
    """Generate an olca-schema process for each region-fuel pairing.

    Parameters
    ----------
    database : pandas.DataFrame
        A generation mix data frame (e.g., from `get_generation_mix_process_df`)
    gen_dict : dict
        A dictionary of olca-schema-formatted processes, used for reference.
    subregion : str, optional
        The aggregation level (e.g., 'BA'), by default None

    Returns
    -------
    dict
        An olca-schema-formatted process dictionary.
    """
    if subregion is None:
        subregion = model_specs.regional_aggregation
    generation_mix_dict = {}

    # Define the list of subregions (e.g., BA names)
    if "Subregion" in database.columns:
        region = list(pd.unique(database["Subregion"]))
    else:
        region = ["US"]
        database["Subregion"] = "US"
    logging.debug("Processing %d regions" % len(region))

    # Grab this list once and reuse it.
    f_list = list(database["FuelCategory"].unique())

    for reg in region:
        database_reg = database[database["Subregion"] == reg]
        exchanges_list = []

        # Creating the reference output
        exchanges_list = exchange(
            exchange_table_creation_ref(database_reg), exchanges_list)

        for fuelname in f_list:
            # Cropping the database according to the current fuel being
            # considered.
            database_f1 = database_reg[
                database_reg["FuelCategory"] == fuelname
            ]
            if database_f1.empty != True:
                matching_dict = {
                    'Electricity': None,
                    'Construction': None
                }
                # Iss150, need to search for both electricity and construction
                m_str1 = "Electricity - " + fuelname + " - " + reg
                m_str2 = "Construction - " + fuelname + " - " + reg
                for generator in gen_dict:
                    if gen_dict[generator]["name"] == m_str1:
                        logging.debug(
                            "Found matching dictionary for '%s'" % m_str1)
                        matching_dict['Electricity'] = gen_dict[generator]
                    elif gen_dict[generator]["name"] == m_str2:
                        logging.debug(
                            "Found matching dictionary for '%s'" % m_str2)
                        matching_dict['Construction'] = gen_dict[generator]
                    # Still allow breaking if we've found both dicts.
                    if (matching_dict['Construction'] is not None) and (
                            matching_dict['Electricity'] is not None):
                        logging.debug("Found both!")
                        break

                for k, match in matching_dict.items():
                    if match is not None:
                        ra = exchange_table_creation_input_genmix(
                            database_f1, fuelname
                        )
                        ra["quantitativeReference"] = False
                        # HOTFIX: make category string, not list
                        # [2023-11-29; TWD]
                        ra["provider"] = {
                            "name": match["name"],
                            "@id": match["uuid"],
                            "category": match["category"],
                        }
                        exchanges_list = exchange(ra, exchanges_list)
                    else:
                        logging.warning(
                            "Trouble matching dictionary for generation "
                            f"mix {k} - {fuelname} - {reg}. "
                            "Skipping this flow for now"
                        )

        final = process_table_creation_genmix(reg, exchanges_list)
        generation_mix_dict[reg] = final

    return generation_mix_dict


def olcaschema_usaverage(
        database,
        gen_dict,
        subregion=None,
        excluded_regions=['HIMS','HIOA','AKGD','AKMS']):
    """Add docstring."""
    if subregion is None:
        subregion = model_specs.regional_aggregation
    generation_mix_dict = {}
    # Cropping the database according to the current fuel being considered
    us_database = create_generation_mix_process_df_from_egrid_ref_data(
        subregion='US')
    # Not choosing the Hawaiian and Alaskan regions.
    us_database = us_database.loc[
        ~us_database["Subregion"].isin(excluded_regions), :]
    df2 = us_database.groupby(
        ['FuelCategory']
    )['Electricity'].agg('sum').reset_index()
    df2['Electricity_fuel_total'] = df2['Electricity']
    del df2['Electricity']

    df3 = us_database.merge(
        df2,
        left_on = 'FuelCategory',
        right_on = 'FuelCategory')
    df3['Generation_Ratio'] = df3['Electricity'] / df3['Electricity_fuel_total']
    del df3['Electricity_fuel_total']

    us_database = df3
    if "FuelCategory" in us_database.columns:
        fuels = list(pd.unique(us_database["FuelCategory"]))

    for fuel in fuels:
        database_reg = us_database[us_database["FuelCategory"] == fuel]
        exchanges_list = []

        # Creating the reference output
        exchange(exchange_table_creation_ref(database_reg), exchanges_list)
        for reg in list(us_database["Subregion"].unique()):
            # Reading complete fuel name and heat content information
            # fuelname = row['Fuelname']
            # Cropping the database according to the current fuel being
            # considered.
            # Not choosing the Hawaiian and Alaskan regions.
            if reg in excluded_regions:
                continue
            else:
                database_f1 = database_reg[database_reg["Subregion"] == reg]
                if database_f1.empty != True:
                    matching_dict = None
                    for generator in gen_dict:
                        if (
                            gen_dict[generator]["name"]
                            == "Electricity - " + fuel + " - " + reg
                        ):
                            matching_dict = gen_dict[generator]
                            break
                    if matching_dict is None:
                        logging.warning(
                            "Trouble matching dictionary for creating fuel "
                            f"mix {fuel} - {reg}. "
                            "Skipping this flow for now."
                        )
                    else:
                        ra = exchange_table_creation_input_usaverage(
                            database_f1, fuel
                        )
                        ra["quantitativeReference"] = False
                        ra["provider"] = {
                            "name": matching_dict["name"],
                            "@id": matching_dict["uuid"],
                            "category": matching_dict["category"].split("/"),
                        }
                        exchange(ra, exchanges_list)
                        # Writing final file

        final = process_table_creation_usaverage(fuel, exchanges_list)
        generation_mix_dict[fuel] = final

    return generation_mix_dict


def olcaschema_international(database, gen_dict, subregion=None):
    """Add docstring."""
    intl_database = pd.read_csv(data_dir+'/International_Electricity_Mix.csv')
    database = intl_database
    generation_mix_dict = {}
    if "Subregion" in database.columns:
        region = list(pd.unique(database["Subregion"]))
    else:
        region = ["US"]
        database["Subregion"] = "US"
    for reg in region:

        database_reg = database[database["Subregion"] == reg]
        exchanges_list = []

        # Creating the reference output
        exchange(exchange_table_creation_ref(database_reg), exchanges_list)
        for fuelname in list(database["FuelCategory"].unique()):
            # Reading complete fuel name and heat content information
            # fuelname = row['Fuelname']
            # Cropping the database according to the current fuel being
            # considered.
            database_f1 = database_reg[
                database_reg["FuelCategory"] == fuelname
            ]
            if database_f1.empty != True:
                matching_dict = None
                for generator in gen_dict:
                    if (
                        gen_dict[generator]["name"]
                        == "Electricity; at grid; USaverage - " + fuelname
                       ):
                        matching_dict = gen_dict[generator]
                        break
                if matching_dict is None:
                    logging.warning(
                        f"Trouble matching dictionary for us average mix {fuelname} - USaverage. Skipping this flow for now"
                    )
                else:
                    ra = exchange_table_creation_input_international_mix(
                    database_f1, fuelname
                    )
                    ra["quantitativeReference"] = False
                    ra["provider"] = {
                       "name": matching_dict["name"],
                       "@id": matching_dict["uuid"],
                       "category": matching_dict["category"].split("/"),
                    }
                    #if matching_dict is None:
                    exchange(ra, exchanges_list)
                    # Writing final file

        final = process_table_creation_genmix(reg, exchanges_list)
        # print(reg +' Process Created')
        generation_mix_dict[reg] = final
    return generation_mix_dict
