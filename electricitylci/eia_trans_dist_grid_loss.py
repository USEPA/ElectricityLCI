#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# eia_trans_dist_grid_loss.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
from functools import lru_cache
import logging
import os
from zipfile import BadZipFile

import pandas as pd
import numpy as np
import requests
from xlrd import XLRDError

from electricitylci.globals import output_dir
from electricitylci.globals import paths
from electricitylci.globals import STATE_ABBREV
from electricitylci.eia923_generation import build_generation_data
from electricitylci.combinator import BA_CODES
import electricitylci.model_config as config
from electricitylci.generation import eia_facility_fuel_region
from electricitylci.egrid_facilities import egrid_facilities
from electricitylci.aggregation_selector import subregion_col
from electricitylci.process_dictionary_writer import (
    exchange,
    ref_exchange_creator,
    electricity_at_user_flow,
    electricity_at_grid_flow,
    process_table_creation_distribution,
)


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """
Define function to extract EIA state-wide electricity profiles and calculate
state-wide transmission and distribution losses for the user-specified year.

See also: https://www.eia.gov/tools/faqs/faq.php?id=105&t=3

Last updated:
    2024-03-11
"""
__all__ = [
    "eia_trans_dist_download_extract",
    "generate_regional_grid_loss",
    "olca_schema_distribution_mix",
]


##############################################################################
# FUNCTIONS
##############################################################################
@lru_cache(maxsize=10)
def eia_trans_dist_download_extract(year):
    """Calculate state-level transmission and distribution losses.

    This function (1) downloads EIA state-level electricity profiles for all
    50 states in the U.S. for a specified year to the working directory, and (2)
    calculates the transmission and distribution gross grid loss for each state
    based on statewide 'estimated losses', 'total disposition', and 'direct
    use'.

    The final output from this function is a [50x1] dimensional dataframe that
    contains transmission and distribution gross grid losses for each U.S. state
    for the specified year. Additional information on the calculation for gross
    grid loss is provided on the EIA website and can be accessed here:
    https://www.eia.gov/tools/faqs/faq.php?id=105&t=3]

    Parameters
    ----------
    year : str
        Analysis year

    Returns
    -------
    pandas.DataFrame
    """
    eia_trans_dist_loss = pd.DataFrame()
    old_path = os.getcwd()
    if os.path.exists(f"{paths.local_path}/t_and_d_{year}"):
        logging.info("Found TD folder for year, %s" % year)
        os.chdir(f"{paths.local_path}/t_and_d_{year}")
    else:
        logging.info("Creating new TD folder for year, %s" % year)
        os.mkdir(f"{paths.local_path}/t_and_d_{year}")
        os.chdir(f"{paths.local_path}/t_and_d_{year}")

    state_df_list = []
    for key in STATE_ABBREV:
        filename = f"{STATE_ABBREV[key]}.xlsx"
        if not os.path.exists(filename):
            logging.info(f"Downloading archive data for {STATE_ABBREV[key]}")
            # HOTFIX: URLs for two-word states have space omitted.
            url_a = (
                "https://www.eia.gov/electricity/state/archive/"
                + year
                + "/"
                + key.replace(" ", "")
                + "/xls/"
                + filename
            )
            url_b = (
                "https://www.eia.gov/electricity/state/"
                + key.replace(" ", "")
                + "/xls/"
                + filename
            )
            # HOTFIX: https://github.com/USEPA/ElectricityLCI/issues/235
            r = requests.get(url_a)
            r_head = r.headers.get("Content-Type", "")
            if not r.ok or r_head.startswith("text"):
                logging.info(f"Trying alternative site {STATE_ABBREV[key]}")
                r = requests.get(url_b)
                r_head = r.headers.get("Content-Type", "")

            if r.ok and not r_head.startswith("text"):
                with open(filename, 'wb') as f:
                    f.write(r.content)
            else:
                logging.error(
                    f"No TD loss data for {STATE_ABBREV[key]} {year}")

        try:
            df = pd.read_excel(
                filename,
                sheet_name="10. Source-Disposition",
                header=3,
                index_col=0,
                engine="openpyxl"
            )
        except (XLRDError, BadZipFile):
            logging.error("Failed to read TD data from '%s'" % filename)
        else:
            logging.debug("Read %s" % filename)
            df.columns = df.columns.str.replace("Year\n", "",regex=True)
            df = df.loc["Estimated losses"] / (
                df.loc["Total disposition"] - df.loc["Direct use"]
            )
            df = df.to_frame(name=STATE_ABBREV[key])
            state_df_list.append(df)

    eia_trans_dist_loss = pd.concat(state_df_list, axis=1, sort=True)
    max_year = max(eia_trans_dist_loss.index.astype(int))
    if max_year < int(year):
        logging.info(f'The most recent T&D loss data is from {max_year}')
        year = str(max_year)

    eia_trans_dist_loss.columns = eia_trans_dist_loss.columns.str.upper()
    eia_trans_dist_loss = eia_trans_dist_loss.transpose()
    eia_trans_dist_loss = eia_trans_dist_loss[[year]]
    eia_trans_dist_loss.columns = ["t_d_losses"]
    os.chdir(old_path)

    return eia_trans_dist_loss


def generate_regional_grid_loss(year, subregion="all"):
    """Generates transmission and distribution losses for the
    given year, aggregated by subregion.

    Parameters
    ----------
    year : int
        Analysis year for the transmission and distribution loss data.
        Ideally this should match the year of your final_database.
    subregion : string
        Any subregion as defined in aggregation_selector.subregion_col.
        Default value "all" will use eGRID subregions.

    Returns
    -------
    pandas.DataFrame
        A dataframe of transmission and distribution loss rates as a
        fraction. This dataframe can be used to generate unit processes
        for transmission and distribution to match the regionally-
        aggregated emissions unit processes.
    """
    logging.info(
        "Generating %d factors for transmission and distribution losses" % year)
    plant_generation = build_generation_data(generation_years=[year])
    plant_generation["FacilityID"] = plant_generation["FacilityID"].astype(int)
    if config.model_specs.replace_egrid:
        plant_data = eia_facility_fuel_region(year)
        plant_data["FacilityID"] = plant_data["FacilityID"].astype(int)
        plant_generation = pd.merge(
            left=plant_generation,
            right=plant_data,
            on="FacilityID",
            how="left",
        )
    else:
        egrid_facilities_w_fuel_region = egrid_facilities[[
            "FacilityID",
            "Subregion",
            "PrimaryFuel",
            "FuelCategory",
            "NERC",
            "PercentGenerationfromDesignatedFuelCategory",
            "Balancing Authority Name",
            "Balancing Authority Code",
            "State"
        ]]
        egrid_facilities_w_fuel_region["FacilityID"] = (
            egrid_facilities_w_fuel_region["FacilityID"].astype(int)
        )
        plant_generation = plant_generation.merge(
            egrid_facilities_w_fuel_region,
            on=["FacilityID"],
            how="left"
        )

    plant_generation["Balancing Authority Name"] = plant_generation[
        "Balancing Authority Code"].map(BA_CODES["BA_Name"])
    plant_generation["FERC_Region"] = plant_generation[
        "Balancing Authority Code"].map(BA_CODES["FERC_Region"])
    plant_generation["EIA_Region"] = plant_generation[
        "Balancing Authority Code"].map(BA_CODES["EIA_Region"])

    td_rates = eia_trans_dist_download_extract(f"{year}")
    td_by_plant = pd.merge(
        left=plant_generation,
        right=td_rates,
        left_on="State",
        right_index=True,
        how="left",
    )
    td_by_plant.dropna(subset=["t_d_losses"], inplace=True)
    td_by_plant["t_d_losses"] = td_by_plant["t_d_losses"].astype(float)

    aggregation_column = subregion_col(subregion)
    wm = lambda x: np.average(
        x, weights=td_by_plant.loc[x.index, "Electricity"]
    )
    if aggregation_column is not None:
        td_by_region = td_by_plant.groupby(
            aggregation_column, as_index=False
        ).agg({"t_d_losses": wm})
    else:
        td_by_region = pd.DataFrame(
            td_by_plant.agg({"t_d_losses": wm}), columns=["t_d_losses"]
        )
        td_by_region["Region"] = "US"

    return td_by_region


def olca_schema_distribution_mix(td_by_region, cons_mix_dict, subregion="BA"):
    """Create dictionaries for openLCA"""
    distribution_mix_dict = {}
    if subregion == "all":
        aggregation_column = "Subregion"
        region = list(pd.unique(td_by_region[aggregation_column]))
    elif subregion == "NERC":
        aggregation_column = "NERC"
        region = list(pd.unique(td_by_region[aggregation_column]))
    elif subregion == "BA":
        aggregation_column = "Balancing Authority Name"
        region = list(pd.unique(td_by_region[aggregation_column]))
    elif subregion == "FERC":
        aggregation_column = "FERC_Region"
        region = list(pd.unique(td_by_region[aggregation_column]))
    else:
        aggregation_column = None
        region = ["US"]
    for reg in region:
        if aggregation_column is None:
            database_reg = td_by_region
        else:
            database_reg = td_by_region.loc[
                td_by_region[aggregation_column] == reg, :
            ]
        exchanges_list = []
        # Creating the reference output
        exchange(
            ref_exchange_creator(electricity_flow=electricity_at_user_flow),
            exchanges_list,
        )
        exchange(
            ref_exchange_creator(electricity_flow=electricity_at_grid_flow),
            exchanges_list,
        )
        exchanges_list[1]["input"] = True
        exchanges_list[1]["quantitativeReference"] = False
        exchanges_list[1]["amount"] = 1 + database_reg["t_d_losses"].values[0]
        matching_dict = None
        for cons_mix in cons_mix_dict:
            if (
                cons_mix_dict[cons_mix]["name"]
                == f"Electricity; at grid; consumption mix - {reg} - {subregion}"
            ):
                matching_dict = cons_mix_dict[cons_mix]
                break
        if matching_dict is None:
            logging.warning(
                f"Trouble matching dictionary for {reg}. "
                f"Consumption mix at user will not be created."
                )
        else:
            exchanges_list[1]["provider"] = {
                "name": matching_dict["name"],
                "@id": matching_dict["uuid"],
                "category": matching_dict["category"].split("/"),
            }
            # Writing final file
            final = process_table_creation_distribution(reg, exchanges_list)
            final["name"] = f"Electricity; at user; consumption mix - {reg} - {subregion}"
            distribution_mix_dict[f"{reg} - {subregion}"] = final

    return distribution_mix_dict


##############################################################################
# MAIN
##############################################################################
if __name__ == "__main__":
    config.model_specs=config.build_model_class("ELCI_2_2020")
    year = 2016
    trans_dist_grid_loss = generate_regional_grid_loss(year, "BA")
    trans_dist_grid_loss.to_csv(f"{output_dir}/trans_dist_loss_{year}.csv")
