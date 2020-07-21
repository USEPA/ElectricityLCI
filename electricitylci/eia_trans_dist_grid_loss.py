# %%
# Import python modules

import pandas as pd
import numpy as np
import os
import urllib.request
from electricitylci.globals import output_dir, data_dir
import logging
from xlrd import XLRDError
from functools import lru_cache
from electricitylci.model_config import model_specs
# %%
# Set working directory, files downloaded from EIA will be saved to this location
# os.chdir = 'N:/eLCI/Transmission and Distribution'

# %%
# Define function to extract EIA state-wide electricity profiles and calculate
# state-wide transmission and distribution losses for the user-specified year


@lru_cache(maxsize=10)
def eia_trans_dist_download_extract(year):

    """[This function (1) downloads EIA state-level electricity profiles for all
    50 states in the U.S. for a specified year to the working directory, and (2)
    calculates the transmission and distribution gross grid loss for each state
    based on statewide 'estimated losses', 'total disposition', and 'direct use'.
    The final output from this function is a [50x1] dimensional dataframe that
    contains transmission and distribution gross grid losses for each U.S. state
    for the specified year. Additional information on the calculation for gross
    grid loss is provided on the EIA website and can be accessed via
    URL: https://www.eia.gov/tools/faqs/faq.php?id=105&t=3]

    Arguments:
        year {[str]} -- [Analysis year]
    """

    eia_trans_dist_loss = pd.DataFrame()

    state_abbrev = {
        "alabama": "al",
        "alaska": "ak",
        "arizona": "az",
        "arkansas": "ar",
        "california": "ca",
        "colorado": "co",
        "connecticut": "ct",
        "delaware": "de",
        "florida": "fl",
        "georgia": "ga",
        "hawaii": "hi",
        "idaho": "id",
        "illinois": "il",
        "indiana": "in",
        "iowa": "ia",
        "kansas": "ks",
        "kentucky": "ky",
        "louisiana": "la",
        "maine": "me",
        "maryland": "md",
        "massachusetts": "ma",
        "michigan": "mi",
        "minnesota": "mn",
        "mississippi": "ms",
        "missouri": "mo",
        "montana": "mt",
        "nebraska": "ne",
        "nevada": "nv",
        "newhampshire": "nh",
        "newjersey": "nj",
        "newmexico": "nm",
        "newyork": "ny",
        "northcarolina": "nc",
        "northdakota": "nd",
        "ohio": "oh",
        "oklahoma": "ok",
        "oregon": "or",
        "pennsylvania": "pa",
        "rhodeisland": "ri",
        "southcarolina": "sc",
        "southdakota": "sd",
        "tennessee": "tn",
        "texas": "tx",
        "utah": "ut",
        "vermont": "vt",
        "virginia": "va",
        "washington": "wa",
        "westvirginia": "wv",
        "wisconsin": "wi",
        "wyoming": "wy",
    }
    old_path = os.getcwd()
    if os.path.exists(f"{data_dir}/t_and_d_{year}"):
        os.chdir(f"{data_dir}/t_and_d_{year}")
    else:
        os.mkdir(f"{data_dir}/t_and_d_{year}")
        os.chdir(f"{data_dir}/t_and_d_{year}")
    state_df_list = list()
    for key in state_abbrev:
        filename = f"{state_abbrev[key]}.xlsx"
        if not os.path.exists(filename):
            url = (
                "https://www.eia.gov/electricity/state/archive/"
                + year
                + "/"
                + key
                + "/xls/"
                + filename
            )
            print(f"Downloading data for {state_abbrev[key]}")

            try:
                urllib.request.urlretrieve(url, filename)
                df = pd.read_excel(
                    filename,
                    sheet_name="10. Source-Disposition",
                    header=3,
                    index_col=0,
                )
            except XLRDError:
                # The most current year has a different url - no "archive/year"
                url = (
                    "https://www.eia.gov/electricity/state/"
                    + key
                    + "/xls/"
                    + filename
                )
                urllib.request.urlretrieve(url, filename)
                df = pd.read_excel(
                    filename,
                    sheet_name="10. Source-Disposition",
                    header=3,
                    index_col=0,
                )
        else:
            logging.info(
                f"Using previously downloaded data for {state_abbrev[key]}"
            )
            df = pd.read_excel(
                filename,
                sheet_name="10. Source-Disposition",
                header=3,
                index_col=0,
            )

        df.columns = df.columns.str.replace("Year\n", "")
        df = df.loc["Estimated losses"] / (
            df.loc["Total disposition"] - df.loc["Direct use"]
        )
        df = df.to_frame(name=state_abbrev[key])
        state_df_list.append(df)
    eia_trans_dist_loss = pd.concat(state_df_list, axis=1, sort=True)
    max_year = max(eia_trans_dist_loss.index.astype(int))
    if max_year < int(year):
        print(f'The most recent T&D loss data is from {max_year}')
        year = str(max_year)

    eia_trans_dist_loss.columns = eia_trans_dist_loss.columns.str.upper()
    eia_trans_dist_loss = eia_trans_dist_loss.transpose()
    eia_trans_dist_loss = eia_trans_dist_loss[[year]]
    eia_trans_dist_loss.columns = ["t_d_losses"]
    os.chdir(old_path)
    return eia_trans_dist_loss


def generate_regional_grid_loss(final_database, year, subregion="all"):
    """This function generates transmission and distribution losses for the
    provided generation data and given year, aggregated by subregion.

    Arguments:
        final_database: dataframe
            The database containing plant-level emissions.
        year: int
            Analysis year for the transmission and distribution loss data.
            Ideally this should match the year of your final_database.
    Returns:
        td_by_region: dataframe
            A dataframe of transmission and distribution loss rates as a
            fraction. This dataframe can be used to generate unit processes
            for transmission and distribution to match the regionally-
            aggregated emissions unit processes.
    """
    print("Generating factors for transmission and distribution losses")
    from electricitylci.eia923_generation import build_generation_data
    from electricitylci.combinator import ba_codes
    from electricitylci.egrid_facilities import get_egrid_facilities
    td_calc_columns = [
        "State",
        "NERC",
        "FuelCategory",
        "PrimaryFuel",
        "NERC",
        "Balancing Authority Name",
        "Electricity",
        "Year",
        "Subregion",
        "FRS_ID",
        "eGRID_ID",
    ]
#    plant_generation = final_database[td_calc_columns].drop_duplicates()
    egrid_facilities = get_egrid_facilities(model_specs.egrid_year)
    egrid_facilities_w_fuel_region = egrid_facilities[
        [
        "FacilityID",
        "Subregion",
        "PrimaryFuel",
        "FuelCategory",
        "NERC",
        "PercentGenerationfromDesignatedFuelCategory",
        "Balancing Authority Name",
        "Balancing Authority Code",
        "State"
        ]
    ]
    egrid_facilities_w_fuel_region["FacilityID"]=egrid_facilities_w_fuel_region["FacilityID"].astype(int)
    plant_generation = build_generation_data(generation_years=[year])
    plant_generation["FacilityID"]=plant_generation["FacilityID"].astype(int)
    plant_generation = plant_generation.merge(egrid_facilities_w_fuel_region,on=["FacilityID"],how="left")
    plant_generation["Balancing Authority Name"]=plant_generation["Balancing Authority Code"].map(ba_codes["BA_Name"])
    plant_generation["FERC_Region"]=plant_generation["Balancing Authority Code"].map(ba_codes["FERC_Region"])
    plant_generation["EIA_Region"]=plant_generation["Balancing Authority Code"].map(ba_codes["EIA_Region"])
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

    from electricitylci.aggregation_selector import subregion_col
    aggregation_column=subregion_col(subregion)
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
    from electricitylci.process_dictionary_writer import (
        exchange_table_creation_ref,
        exchange,
        ref_exchange_creator,
        electricity_at_user_flow,
        electricity_at_grid_flow,
        process_table_creation_distribution,
    )

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
                == "Electricity; at grid; consumption mix - " + reg
            ):
                matching_dict = cons_mix_dict[cons_mix]
                break
        if matching_dict is None:
            logging.warning(f"Trouble matching dictionary for {reg}")
        else:
            exchanges_list[1]["provider"] = {
                "name": matching_dict["name"],
                "@id": matching_dict["uuid"],
                "category": matching_dict["category"].split("/"),
            }
            # Writing final file
        final = process_table_creation_distribution(reg, exchanges_list)
        final["name"] = "Electricity; at user; consumption mix - " + reg
        distribution_mix_dict[reg] = final
    return distribution_mix_dict


if __name__ == "__main__":
    year = 2016
    final_database=pd.DataFrame()
    trans_dist_grid_loss = generate_regional_grid_loss(
        final_database, year, "BA"
    )
    trans_dist_grid_loss.to_csv(f"{output_dir}/trans_dist_loss_{year}.csv")
