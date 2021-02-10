import pandas as pd
import numpy as np
from electricitylci.globals import data_dir, output_dir
import electricitylci.PhysicalQuantities as pq
import electricitylci.cems_data as cems
import electricitylci.eia923_generation as eia923
import electricitylci.eia860_facilities as eia860
import fedelemflowlist
from electricitylci.model_config import model_specs

import logging


def generate_plant_emissions(year):
    """
    Reads data from EPA air markets program data and fuel use from EIA 923 Page 1
    or Page 5 data (generator vs boiler-level data). Emissions factors from AP42
    are used to calculate emissions from the plant if the fuel input from EPA
    air markets program data does not matche EIA 923 data. This data is meant
    to replace the eGRID-sourced data provided by STEWi.

    Parameters
    ----------
    year : int
        Year of data to use (Air Markets Program Data, EIA 923, etc.)

    Returns
    -------
    dataframe
        Returns a dataframe with emissions for all power plants reporting to
        AMPD or EIA923. Emissions are either actual measured emissions (marked
        as Source = "cems") or from ap42 emission factors applied at either
        the boiler or generator fuel type level (marked as Source = "ap42").
    """
    COMPARTMENT_MAP = {"emission/air": "air"}
    FUELCAT_MAP = {
        "AB": "BIOMASS",
        #            "BFG",
        "BIT": "COAL",
        #            "BLQ",
        "DFO": "OIL",
        "GEO": "GEOTHERMAL",
        #            "JF",
        #            "KER",
        #            "LFG",
        "LIG": "COAL",
        #            "MSB",
        #            "MSN",
        #            "MWH",
        "NG": "GAS",
        "NUC": "NUCLEAR",
        "OBG": "BIOMASS",
        "OBL": "BIOMASS",
        "OBS": "BIOMASS",
        #            "OG",
        #            "OTH",
        #            "PC",
        #            "PG",
        #            "PUR",
        "RC": "COAL",
        "RFO": "OIL",
        #            "SC",
        #            "SGC",
        #            "SGP",
        #            "SLW",
        "SUB": "COAL",
        "SUN": "SOLAR",
        #            "TDF",
        "WAT": "HYDRO",
        "WC": "COAL",
        "WDL": "BIOMASS",
        "WDS": "BIOMASS",
        #            "WH",
        "WND": "WIND",
        "WO": "OIL",
        "Mixed Fuel Type": "MIXED",
    }

    def emissions_check_gen_fuel(df):
        emissions_check = eia923_gen_fuel_sub_agg.merge(
            df, on="plant_id", how="left"
        )
        emissions_check["Check Heat Input MMBtu"] = emissions_check[
            "total_fuel_consumption_mmbtu"
        ].fillna(0) - emissions_check[
            "Sheet 1_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input Quantity"] = emissions_check[
            "total_fuel_consumption_quantity"
        ].fillna(0) - emissions_check[
            "Sheet 1_total_fuel_consumption_quantity"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input MMBtu Ratio"] = emissions_check[
            "total_fuel_consumption_mmbtu"
        ].fillna(0) / emissions_check[
            "Sheet 1_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input Quantity Ratio"] = emissions_check[
            "total_fuel_consumption_quantity"
        ].fillna(0) / emissions_check[
            "Sheet 1_total_fuel_consumption_quantity"
        ].fillna(
            0
        )

        return emissions_check

    def emissions_check_boiler(df):
        df_list = [
            eia923_gen_fuel_boiler_agg,
            df,
            eia923_boiler_sub_agg,
            eia923_gen_fuel_union_boiler_agg,
        ]
        emissions_check = df_list[0]
        for df_ in df_list[1:]:
            emissions_check = emissions_check.merge(
                df_, on=["plant_id"], how="left"
            )
        emissions_check["Check Heat Input MMBtu_Boiler"] = emissions_check[
            "total_fuel_consumption_mmbtu"
        ].fillna(0) - emissions_check[
            "Sheet 3_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input Quantity_Boiler"] = emissions_check[
            "total_fuel_consumption_quantity"
        ].fillna(0) - emissions_check[
            "Sheet 3_total_fuel_consumption_quantity"
        ].fillna(
            0
        )
        emissions_check[
            "Check Heat Input MMBtu_Boiler Ratio"
        ] = emissions_check["total_fuel_consumption_mmbtu"].fillna(
            0
        ) / emissions_check[
            "Sheet 3_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check[
            "Check Heat Input Quantity_Boiler Ratio"
        ] = emissions_check["total_fuel_consumption_quantity"].fillna(
            0
        ) / emissions_check[
            "Sheet 3_total_fuel_consumption_quantity"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input MMBtu_Boiler_Gen"] = (
            emissions_check["total_fuel_consumption_mmbtu"].fillna(0)
            + emissions_check[
                "Sheet 1_Union Total Fuel Consumption (MMBtu)"
            ].fillna(0)
        ) - emissions_check["Sheet 1_Total Fuel Consumption (MMBtu)"].fillna(0)
        emissions_check["Check Heat Input Quantity_Boiler_Gen"] = (
            emissions_check["total_fuel_consumption_quantity"].fillna(0)
            + emissions_check[
                "Sheet 1_Union total_fuel_consumption_quantity"
            ].fillna(0)
        ) - emissions_check["Sheet 1_total_fuel_consumption_quantity"].fillna(
            0
        )
        emissions_check["Check Heat Input MMBtu_Boiler_Gen Ratio"] = (
            emissions_check["total_fuel_consumption_mmbtu"].fillna(0)
            + emissions_check[
                "Sheet 1_Union Total Fuel Consumption (MMBtu)"
            ].fillna(0)
        ) / emissions_check["Sheet 1_Total Fuel Consumption (MMBtu)"].fillna(0)
        emissions_check["Check Heat Input Quantity_Boiler_Gen Ratio"] = (
            emissions_check["total_fuel_consumption_quantity"].fillna(0)
            + emissions_check[
                "Sheet 1_Union total_fuel_consumption_quantity"
            ].fillna(0)
        ) / emissions_check["Sheet 1_total_fuel_consumption_quantity"].fillna(
            0
        )

        return emissions_check

    def eia_gen_fuel_co2_ch4_n2o_emissions(eia923_gen_fuel):

        emissions = pd.DataFrame()

        for row in ef_co2_ch4_n2o.itertuples():

            fuel_type = eia923_gen_fuel_sub.loc[
                eia923_gen_fuel_sub["reported_fuel_type_code"].astype(str)
                == str(row.EIA_Fuel_Type_Code)
            ].copy()

            fuel_type["CO2 (Tons)"] = (row.ton_CO2_mmBtu) * fuel_type[
                "total_fuel_consumption_mmbtu"
            ].astype(float, errors="ignore")
            fuel_type["CH4 (lbs)"] = (row.pound_methane_per_mmbtu) * fuel_type[
                "total_fuel_consumption_mmbtu"
            ].astype(float, errors="ignore")
            fuel_type["N2O (lbs)"] = (row.pound_n2o_per_mmBtu) * fuel_type[
                "total_fuel_consumption_mmbtu"
            ].astype(float, errors="ignore")

            emissions = pd.concat([emissions, fuel_type])

        emissions_agg = emissions.groupby(
            ["plant_id", "plant_name", "operator_name"]
        )[
            "CO2 (Tons)",
            "CH4 (lbs)",
            "N2O (lbs)",
            "total_fuel_consumption_mmbtu",
            "total_fuel_consumption_quantity",
        ].sum()
        emissions_agg = emissions_agg.reset_index()
        emissions_agg["plant_id"] = emissions_agg["plant_id"].astype(str)

        return emissions_agg

    def eia_boiler_co2_ch4_n2o_emissions(eia923_boiler):

        emissions = pd.DataFrame()

        for row in ef_co2_ch4_n2o.itertuples():

            fuel_type = eia923_boiler_sub.loc[
                eia923_boiler_sub["reported_fuel_type_code"].astype(str)
                == str(row.EIA_Fuel_Type_Code)
            ].copy()

            fuel_heating_value_monthly = [
                "mmbtu_per_unit_january",
                "mmbtu_per_unit_february",
                "mmbtu_per_unit_march",
                "mmbtu_per_unit_april",
                "mmbtu_per_unit_may",
                "mmbtu_per_unit_june",
                "mmbtu_per_unit_july",
                "mmbtu_per_unit_august",
                "mmbtu_per_unit_september",
                "mmbtu_per_unit_october",
                "mmbtu_per_unit_november",
                "mmbtu_per_unit_december",
            ]
            fuel_quantity_monthly = [
                "quantity_of_fuel_consumed_january",
                "quantity_of_fuel_consumed_february",
                "quantity_of_fuel_consumed_march",
                "quantity_of_fuel_consumed_april",
                "quantity_of_fuel_consumed_may",
                "quantity_of_fuel_consumed_june",
                "quantity_of_fuel_consumed_july",
                "quantity_of_fuel_consumed_august",
                "quantity_of_fuel_consumed_september",
                "quantity_of_fuel_consumed_october",
                "quantity_of_fuel_consumed_november",
                "quantity_of_fuel_consumed_december",
            ]

            fuel_type["total_fuel_consumption_mmbtu"] = (
                np.multiply(
                    fuel_type[fuel_heating_value_monthly],
                    fuel_type[fuel_quantity_monthly],
                )
            ).sum(axis=1, skipna=True)

            fuel_type["CO2 (Tons)"] = (row.ton_CO2_mmBtu) * fuel_type[
                "total_fuel_consumption_mmbtu"
            ].astype(float, errors="ignore")
            fuel_type["CH4 (lbs)"] = (row.pound_methane_per_mmbtu) * fuel_type[
                "total_fuel_consumption_mmbtu"
            ].astype(float, errors="ignore")
            fuel_type["N2O (lbs)"] = (row.pound_n2o_per_mmBtu) * fuel_type[
                "total_fuel_consumption_mmbtu"
            ].astype(float, errors="ignore")

            emissions = pd.concat([emissions, fuel_type])

        emissions_agg = emissions.groupby(
            ["plant_id", "plant_name", "operator_name"], as_index=False
        )[
            "CH4 (lbs)",
            "N2O (lbs)",
            "CO2 (Tons)",
            "total_fuel_consumption_mmbtu",
            "total_fuel_consumption_quantity",
        ].sum()
        emissions_agg["plant_id"] = emissions_agg["plant_id"].astype(str)

        return emissions_agg

    def eia_gen_fuel_net_gen(eia923_gen_fuel):

        net_gen_monthly = [
            "netgen_january",
            "netgen_february",
            "netgen_march",
            "netgen_april",
            "netgen_may",
            "netgen_june",
            "netgen_july",
            "netgen_august",
            "netgen_september",
            "netgen_october",
            "netgen_november",
            "netgen_december",
        ]
        eia923_gen_fuel["Annual Net Generation (MWh)"] = eia923_gen_fuel[
            net_gen_monthly
        ].sum(axis=1, skipna=True)
        eia_923_gen_fuel_agg = eia923_gen_fuel.groupby(
            ["plant_id", "plant_name", "operator_name"]
        )["Annual Net Generation (MWh)"].sum()
        eia_923_gen_fuel_agg = eia_923_gen_fuel_agg.reset_index()
        eia_923_gen_fuel_agg_fuel_type = eia923_gen_fuel.groupby(
            [
                "plant_id",
                "plant_name",
                "operator_name",
                "reported_fuel_type_code",
            ]
        )["Annual Net Generation (MWh)"].sum()
        eia_923_gen_fuel_agg_fuel_type = (
            eia_923_gen_fuel_agg_fuel_type.reset_index()
        )
        eia_923_gen_fuel_agg_fuel_type_pivot = eia_923_gen_fuel_agg_fuel_type.pivot(
            index="plant_id",
            columns="reported_fuel_type_code",
            values="Annual Net Generation (MWh)",
        )
        eia_923_gen_fuel_agg_fuel_type_pivot = (
            eia_923_gen_fuel_agg_fuel_type_pivot.reset_index()
        )
        eia_923_gen_fuel_agg = eia_923_gen_fuel_agg.merge(
            eia_923_gen_fuel_agg_fuel_type_pivot, on="plant_id", how="left"
        )
        eia_923_gen_fuel_agg["plant_id"] = eia_923_gen_fuel_agg[
            "plant_id"
        ].astype(str)

        return eia_923_gen_fuel_agg

    def eia_gen_fuel_so2_emissions(eia923_gen_fuel_sub):

        #        emissions = pd.DataFrame()
        emissions = eia923_gen_fuel_sub.merge(
            ef_so2.loc[ef_so2["Boiler_Firing_Type_Code"] == "None", :],
            left_on=["reported_prime_mover", "reported_fuel_type_code"],
            right_on=["Reported_Prime_Mover", "Reported_Fuel_Type_Code"],
            how="left",
        )
        emissions = emissions.merge(
            wtd_sulfur_content_fuel,
            left_on=["Reported_Fuel_Type_Code"],
            right_index=True,
            how="left",
        )
        emissions["SO2_Emissions"] = None
        criteria = (emissions["Emission_Factor_Denominator"] != "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "No"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "total_fuel_consumption_quantity"]
            * emissions.loc[criteria, "Emission_Factor"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] == "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "No"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "total_fuel_consumption_mmbtu"]
            * emissions.loc[criteria, "Emission_Factor"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] == "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "Yes"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "Avg Sulfur Content (%)"]
            * emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "total_fuel_consumption_mmbtu"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] != "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "Yes"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "Avg Sulfur Content (%)"]
            * emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "total_fuel_consumption_quantity"]
        )

        emissions_agg = emissions.groupby(
            ["plant_id", "plant_name", "operator_name"], as_index=False
        )[
            "SO2 (lbs)",
            "total_fuel_consumption_quantity",
            "total_fuel_consumption_mmbtu",
        ].sum()
        emissions_agg["plant_id"] = emissions_agg["plant_id"].astype(str)

        return emissions_agg

    def eia_boiler_so2_emissions(eia923_boiler_firing_type):

        fuel_heating_value_monthly = [
            "mmbtu_per_unit_january",
            "mmbtu_per_unit_february",
            "mmbtu_per_unit_march",
            "mmbtu_per_unit_april",
            "mmbtu_per_unit_may",
            "mmbtu_per_unit_june",
            "mmbtu_per_unit_july",
            "mmbtu_per_unit_august",
            "mmbtu_per_unit_september",
            "mmbtu_per_unit_october",
            "mmbtu_per_unit_november",
            "mmbtu_per_unit_december",
        ]
        fuel_quantity_monthly = [
            "quantity_of_fuel_consumed_january",
            "quantity_of_fuel_consumed_february",
            "quantity_of_fuel_consumed_march",
            "quantity_of_fuel_consumed_april",
            "quantity_of_fuel_consumed_may",
            "quantity_of_fuel_consumed_june",
            "quantity_of_fuel_consumed_july",
            "quantity_of_fuel_consumed_august",
            "quantity_of_fuel_consumed_september",
            "quantity_of_fuel_consumed_october",
            "quantity_of_fuel_consumed_november",
            "quantity_of_fuel_consumed_december",
        ]
        so2_emissions_monthly = [
            "SO2 (lbs) January",
            "SO2 (lbs) February",
            "SO2 (lbs) March",
            "SO2 (lbs) April",
            "SO2 (lbs) May",
            "SO2 (lbs) June",
            "SO2 (lbs) July",
            "SO2 (lbs) August",
            "SO2 (lbs) September",
            "SO2 (lbs) October",
            "SO2 (lbs) November",
            "SO2 (lbs) December",
        ]
        sulfur_content_monthly = [
            "sulfur_content_january",
            "sulfur_content_february",
            "sulfur_content_march",
            "sulfur_content_april",
            "sulfur_content_may",
            "sulfur_content_june",
            "sulfur_content_july",
            "sulfur_content_august",
            "sulfur_content_september",
            "sulfur_content_october",
            "sulfur_content_november",
            "sulfur_content_december",
        ]
        fuel_heat_quantity_monthly = [
            "MMBtu January",
            "MMBtu February",
            "MMBtu March",
            "MMBtu April",
            "MMBtu May",
            "MMBtu June",
            "MMBtu July",
            "MMBtu August",
            "MMBtu September",
            "MMBtu October",
            "MMBtu November",
            "MMBtu December",
        ]

        eia923_boiler_firing_type[
            fuel_heating_value_monthly
        ] = eia923_boiler_firing_type[fuel_heating_value_monthly].apply(
            pd.to_numeric, errors="coerce"
        )
        eia923_boiler_firing_type[
            fuel_quantity_monthly
        ] = eia923_boiler_firing_type[fuel_quantity_monthly].apply(
            pd.to_numeric, errors="coerce"
        )
        eia923_boiler_firing_type[
            sulfur_content_monthly
        ] = eia923_boiler_firing_type[sulfur_content_monthly].apply(
            pd.to_numeric, errors="coerce"
        )
        eia923_boiler_firing_type[fuel_heat_quantity_monthly] = np.multiply(
            eia923_boiler_firing_type[fuel_heating_value_monthly],
            eia923_boiler_firing_type[fuel_quantity_monthly],
        )
        emissions = eia923_boiler_firing_type.merge(
            ef_so2,
            left_on=[
                "reported_prime_mover",
                "reported_fuel_type_code",
                "firing_type_1",
            ],
            right_on=[
                "Reported_Prime_Mover",
                "Reported_Fuel_Type_Code",
                "Boiler_Firing_Type_Code",
            ],
            how="left",
        )

        emissions["SO2_Emissions"] = None
        criteria = (emissions["Emission_Factor_Denominator"] != "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "No"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, fuel_quantity_monthly].sum(axis=1)
            * emissions.loc[criteria, "Emission_Factor"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] == "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "No"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, fuel_heat_quantity_monthly].sum(axis=1)
            * emissions.loc[criteria, "Emission_Factor"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] == "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "Yes"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = np.multiply(
            np.diagonal(
                np.dot(
                    emissions.loc[criteria, fuel_heat_quantity_monthly].fillna(
                        0
                    ),
                    emissions.loc[criteria, sulfur_content_monthly]
                    .fillna(0)
                    .T,
                )
            ),
            emissions.loc[criteria, "Emission_Factor"],
        )
        criteria = (emissions["Emission_Factor_Denominator"] != "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "Yes"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = np.multiply(
            np.diagonal(
                np.dot(
                    emissions.loc[criteria, fuel_quantity_monthly].fillna(0),
                    emissions.loc[criteria, sulfur_content_monthly]
                    .fillna(0)
                    .T,
                )
            ),
            emissions.loc[criteria, "Emission_Factor"],
        )
        #        emissions["SO2 (lbs)"] = (emissions[so2_emissions_monthly]).sum(
        #            axis=1, skipna=True
        #        )
        emissions["total_fuel_consumption_mmbtu"] = emissions[
            fuel_heat_quantity_monthly
        ].sum(axis=1)
        emissions_merge = emissions.merge(
            eia_so2_rem_eff, on=["plant_id", "boiler_id"], how="left"
        )
        emissions_merge[
            "so2_removal_efficiency_rate_at_annual_operating_factor"
        ] = emissions_merge[
            "so2_removal_efficiency_rate_at_annual_operating_factor"
        ].fillna(
            0
        )
        emissions_merge["SO2 (lbs) with AEC"] = emissions_merge[
            "SO2 (lbs)"
        ] * (
            1
            - emissions_merge[
                "so2_removal_efficiency_rate_at_annual_operating_factor"
            ]
        )
        emissions_agg = emissions_merge.groupby(
            ["plant_id", "plant_name", "operator_name"], as_index=False
        )[
            "SO2 (lbs) with AEC",
            "total_fuel_consumption_quantity",
            "total_fuel_consumption_mmbtu",
        ].sum()
        emissions_agg["plant_id"] = emissions_agg["plant_id"].astype(str)
        emissions_agg = emissions_agg.rename(
            columns={"SO2 (lbs) with AEC": "SO2 (lbs)"}
        )

        return emissions_agg

    def eia_gen_fuel_nox_emissions(eia923_gen_fuel_sub):

        #        emissions = pd.DataFrame()
        emissions = eia923_gen_fuel_sub.merge(
            ef_nox,
            left_on=["reported_fuel_type_code", "reported_prime_mover"],
            right_on=["Reported_Fuel_Type_Code", "Reported_Prime_Mover"],
            how="left",
        )
        emissions["NOx (lbs)"] = None
        criteria = emissions["Emission_Factor_Denominator"] == "MMBtu"
        emissions.loc[criteria, "NOx (lbs)"] = (
            emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "total_fuel_consumption_mmbtu"]
        )
        criteria = emissions["Emission_Factor_Denominator"] != "MMBtu"
        emissions.loc[criteria, "NOx (lbs)"] = (
            emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "total_fuel_consumption_quantity"]
        )
        emissions_agg = emissions.groupby(
            ["plant_id", "plant_name", "operator_name"], as_index=False
        )[
            "NOx (lbs)",
            "total_fuel_consumption_quantity",
            "total_fuel_consumption_mmbtu",
        ].sum()
        emissions_agg["plant_id"] = emissions_agg["plant_id"].astype(str)

        return emissions_agg

    def eia_boiler_nox(row):
        if row["nox_emission_rate_entire_year_lbs_mmbtu"] > 0:
            return row["NOx Based on Annual Rate (lbs)"]
        else:
            return row["NOx (lbs)"]

    def eia_boiler_nox_emissions(eia923_boiler_firing_type):
        fuel_heat_quantity_monthly = [
            "MMBtu January",
            "MMBtu February",
            "MMBtu March",
            "MMBtu April",
            "MMBtu May",
            "MMBtu June",
            "MMBtu July",
            "MMBtu August",
            "MMBtu September",
            "MMBtu October",
            "MMBtu November",
            "MMBtu December",
        ]
        emissions = pd.DataFrame()
        emissions = eia923_boiler_firing_type.merge(
            ef_nox,
            left_on=[
                "reported_fuel_type_code",
                "reported_prime_mover",
                "firing_type_1",
            ],
            right_on=[
                "Reported_Fuel_Type_Code",
                "Reported_Prime_Mover",
                "Boiler_Firing_Type_Code",
            ],
            how="left",
        )
        emissions["NOx (lbs)"] = emissions["Emission_Factor"] * emissions[
            "total_fuel_consumption_quantity"
        ].astype(float, errors="ignore")

        emissions.dropna(subset=["NOx (lbs)"], inplace=True)
        emissions["total_fuel_consumption_mmbtu"] = emissions[
            fuel_heat_quantity_monthly
        ].sum(axis=1)
        emissions_boiler = emissions.merge(
            eia_nox_rate, on=["plant_id", "boiler_id"], how="left"
        )
        emissions_boiler["NOx Based on Annual Rate (lbs)"] = (
            emissions_boiler["total_fuel_consumption_mmbtu"]
            * emissions_boiler["nox_emission_rate_entire_year_lbs_mmbtu"]
        )
        emissions_boiler = emissions_boiler.assign(
            NOx_lbs=emissions_boiler.apply(eia_boiler_nox, axis=1)
        )
        emissions_agg = emissions_boiler.groupby(
            ["plant_id", "plant_name", "operator_name"], as_index=False
        )[
            "NOx_lbs",
            "total_fuel_consumption_quantity",
            "total_fuel_consumption_mmbtu",
        ].sum()
        emissions_agg["plant_id"] = emissions_agg["plant_id"].astype(str)
        emissions_agg = emissions_agg.rename(columns={"NOx_lbs": "NOx (lbs)"})
        return emissions_agg

    def eia_wtd_sulfur_content(eia923_boiler):
        """This function determines the weighted average sulfur content of all reported fuel types
    reported in EIA-923 Monthly Boiler Fuel Consumption and Emissions Time Series File.
    Weighted average fuel sulfur content is derived via monthly fuel quantities and sulfur content reported
    in 'EIA-923 Monthly Boiler Fuel Consumption and Emissions Time Series File'. This approach implicitly
    assumes that the composition of fuels consumed in steam boilers are representative of their respective fuel class,
    and can be applied to thermal generation without loss of generality. For example, the sulfur content of bitumiunous coal
    consumed for steam generators is assumed to be representative of bituminious coal consumed across other prime movers technologies
    and/or thermal generation technologies.

    Arguments:
        eia923_boiler {[Dataframe]} -- This dataframe contains all information in 'EIA-923 Monthly Boiler Fuel
        Consumption and Emissions Time Series File'

    Returns:
        [sulfur_content_agg] -- A 39x1 dataframe, the index represents all unqiue EIA reported fuel
        code types in the 'EIA-923 Monthly Boiler Fuel Consumption and Emissions Time Series File'.
        The rows represent the weigthed average sulfur fuel content for the select fuel.
        """

        sulfur_content = pd.DataFrame()
        eia923_boiler_drop_na = eia923_boiler.dropna(
            subset=["reported_fuel_type_code"]
        )
        eia923_boiler_unique_fuel_codes = (
            eia923_boiler[["reported_fuel_type_code"]]
            .drop_duplicates()
            .dropna()
        )
        eia923_boiler_unique_fuel_codes.columns = ["reported_fuel_type_code"]

        for row in eia923_boiler_unique_fuel_codes.itertuples():
            fuel_type = eia923_boiler_drop_na.loc[
                eia923_boiler_drop_na["reported_fuel_type_code"].astype(str)
                == str(row.reported_fuel_type_code)
            ].copy()
            fuel_quantity_monthly = [
                "quantity_of_fuel_consumed_january",
                "quantity_of_fuel_consumed_february",
                "quantity_of_fuel_consumed_march",
                "quantity_of_fuel_consumed_april",
                "quantity_of_fuel_consumed_may",
                "quantity_of_fuel_consumed_june",
                "quantity_of_fuel_consumed_july",
                "quantity_of_fuel_consumed_august",
                "quantity_of_fuel_consumed_september",
                "quantity_of_fuel_consumed_october",
                "quantity_of_fuel_consumed_november",
                "quantity_of_fuel_consumed_december",
            ]
            sulfur_content_monthly = [
                "sulfur_content_january",
                "sulfur_content_february",
                "sulfur_content_march",
                "sulfur_content_april",
                "sulfur_content_may",
                "sulfur_content_june",
                "sulfur_content_july",
                "sulfur_content_august",
                "sulfur_content_september",
                "sulfur_content_october",
                "sulfur_content_november",
                "sulfur_content_december",
            ]
            fuel_type["Sulfur Weighted"] = (
                np.multiply(
                    fuel_type[fuel_quantity_monthly],
                    fuel_type[sulfur_content_monthly],
                )
            ).sum(axis=1, skipna=True)
            frames = [sulfur_content, fuel_type]
            sulfur_content = pd.concat(frames)
        sulfur_content_agg = sulfur_content.groupby(
            ["reported_fuel_type_code"], as_index=False
        )["Sulfur Weighted", "total_fuel_consumption_quantity"].sum()
        sulfur_content_agg["Avg Sulfur Content (%)"] = (
            sulfur_content_agg["Sulfur Weighted"]
            / sulfur_content_agg["total_fuel_consumption_quantity"]
        )
        sulfur_content_agg = sulfur_content_agg[
            ["reported_fuel_type_code", "Avg Sulfur Content (%)"]
        ]

        return sulfur_content_agg

    def eia_primary_fuel(row):
        if row["Primary Fuel %"] < model_specs.min_plant_percent_generation_from_primary_fuel_category/100:
            return "Mixed Fuel Type"
        else:
            return row["Primary Fuel"]

    def emissions_logic_CO2(row):
        if (
            (
                row["ampd Heat Input (MMBtu)"]
                < row["total_fuel_consumption_mmbtu"] * 1.2
            )
            | (
                row["ampd Heat Input (MMBtu)"]
                > row["total_fuel_consumption_mmbtu"] * 0.8
            )
        ) & (
            (row["ampd CO2 (Tons)"] < row["CO2 (Tons)"] * 100)
            | (row["ampd CO2 (Tons)"] > row["CO2 (Tons)"] * (1 / 100))
        ):
            row["Source"] = "ampd"
            return row["ampd CO2 (Tons)"], row["Source"]
        else:
            row["Source"] = "ap42"
            return row["CO2 (Tons)"], row["Source"]

    def emissions_logic_SO2(row):
        if (
            (
                row["ampd Heat Input (MMBtu)"]
                < row["total_fuel_consumption_mmbtu"] * 1.2
            )
            | (
                row["ampd Heat Input (MMBtu)"]
                > row["total_fuel_consumption_mmbtu"] * 0.8
            )
        ) & (
            (row["ampd SO2 (lbs)"] < row["SO2 (lbs)"] * 100)
            | (row["ampd SO2 (lbs)"] > row["SO2 (lbs)"] * (1 / 100))
        ):
            row["Source"] = "ampd"
            return row["ampd SO2 (lbs)"], row["Source"]
        else:
            row["Source"] = "ap42"
            return row["SO2 (lbs)"], row["Source"]

    def emissions_logic_NOx(row):
        if (
            (
                row["ampd Heat Input (MMBtu)"]
                < row["total_fuel_consumption_mmbtu"] * 1.2
            )
            | (
                row["ampd Heat Input (MMBtu)"]
                > row["total_fuel_consumption_mmbtu"] * 0.8
            )
        ) & (
            (row["ampd NOX (lbs)"] < row["NOx (lbs)"] * 100)
            | (row["ampd NOX (lbs)"] > row["NOx (lbs)"] * (1 / 100))
        ):
            row["Source"] = "ampd"
            return row["ampd NOX (lbs)"], row["Source"]
        else:
            row["Source"] = "ap42"
            return row["NOx (lbs)"], row["Source"]

    print(
        "Generating power plant emissions from CEMS data or emission factors..."
    )
    logging.info("Loading data")
    ampd = cems.build_cems_df(year)
    eia923_gen_fuel = eia923.eia923_generation_and_fuel(year)
    #    eia923_gen_fuel = pd.read_pickle(
    #        f"{data_dir}/EIA 923/Pickle Files/Generation and Fuel/EIA 923 Generation and Fuel {year}.pkl"
    #    )
    eia923_boiler = eia923.eia923_boiler_fuel(year)
    #    eia923_boiler = pd.read_pickle(
    #        f"{data_dir}/EIA 923/Pickle Files/Boiler Fuel/EIA 923 Boiler Fuel {year}.pkl"
    #    )
    eia923_aec = eia923.eia923_sched8_aec(year)
    #    eia923_aec = pd.read_pickle(
    #        f"{data_dir}/EIA 923/Pickle Files/Air Emissions Control/EIA 923 AEC {year}.pkl"
    #    )
    eia860_env_assoc_boiler_NOx = eia860.eia860_EnviroAssoc_nox(year)
    #    eia860_env_assoc_boiler_NOx = pd.read_pickle(
    #        f"{data_dir}/EIA 860/Pickle Files/Environmental Associations/EIA 860 Boiler NOx {year}.pkl"
    #    )
    eia860_env_assoc_boiler_SO2 = eia860.eia860_EnviroAssoc_so2(year)
    #    eia860_env_assoc_boiler_SO2 = pd.read_pickle(
    #        f"{data_dir}/EIA 860/Pickle Files/Environmental Associations/EIA 860 Boiler SO2 {year}.pkl"
    #    )
    eia860_boiler_design = eia860.eia860_boiler_info_design(year)
    #    eia860_boiler_design = pd.read_pickle(
    #        f"{data_dir}/EIA 860/Pickle Files/Boiler Info & Design Parameters/EIA 860 Boiler Design {year}.pkl"
    #    )
    ef_co2_ch4_n2o = pd.read_excel(
        f"{data_dir}/EFs/eLCI EFs.xlsx", sheet_name="CO2,CH4,N2O"
    )
    ef_so2 = pd.read_csv(f"{data_dir}/EFs/eLCI EFs_SO2.csv", index_col=0)
    ef_nox = pd.read_csv(f"{data_dir}/EFs/eLCI EFs_NOx.csv", index_col=0)
    eia_nox_rate = eia923_aec[
        [
            "plant_id",
            "nox_control_id",
            "nox_emission_rate_entire_year_lbs_mmbtu",
        ]
    ].copy()

    eia_nox_rate["nox_emission_rate_entire_year_lbs_mmbtu"] = eia_nox_rate[
        "nox_emission_rate_entire_year_lbs_mmbtu"
    ].apply(pd.to_numeric, errors="coerce")
    eia_nox_rate = eia_nox_rate.dropna().drop_duplicates()
    eia_nox_rate["nox_control_id"] = eia_nox_rate["nox_control_id"].astype(str)
    eia_nox_rate["plant_id"] = eia_nox_rate["plant_id"].astype(str)
    eia_nox_rate = eia_nox_rate.merge(
        eia860_env_assoc_boiler_NOx[
            ["plant_id", "nox_control_id", "boiler_id"]
        ],
        on=["plant_id", "nox_control_id"],
        how="left",
    )
    eia_nox_rate = eia_nox_rate.dropna()
    eia_nox_rate = eia_nox_rate[
        ["plant_id", "nox_emission_rate_entire_year_lbs_mmbtu", "boiler_id"]
    ].drop_duplicates(["plant_id", "boiler_id"])

    eia_so2_rem_eff = eia923_aec[
        [
            "plant_id",
            "so2_control_id",
            "so2_removal_efficiency_rate_at_annual_operating_factor",
        ]
    ].copy()
    eia_so2_rem_eff[
        "so2_removal_efficiency_rate_at_annual_operating_factor"
    ] = eia_so2_rem_eff[
        "so2_removal_efficiency_rate_at_annual_operating_factor"
    ].apply(
        pd.to_numeric, errors="coerce"
    )
    eia_so2_rem_eff = eia_so2_rem_eff.dropna().drop_duplicates()
    eia_so2_rem_eff["so2_control_id"] = eia_so2_rem_eff[
        "so2_control_id"
    ].astype(str)
    eia_so2_rem_eff["plant_id"] = eia_so2_rem_eff["plant_id"].astype(str)
    eia_so2_rem_eff = eia_so2_rem_eff.merge(
        eia860_env_assoc_boiler_SO2[
            ["plant_id", "so2_control_id", "boiler_id"]
        ],
        on=["plant_id", "so2_control_id"],
        how="left",
    )
    eia_so2_rem_eff = eia_so2_rem_eff.dropna()
    eia_so2_rem_eff = eia_so2_rem_eff[
        [
            "plant_id",
            "so2_removal_efficiency_rate_at_annual_operating_factor",
            "boiler_id",
        ]
    ].drop_duplicates(["plant_id", "boiler_id"])

    eia923_gen_fuel_unique_fuel_codes = (
        eia923_gen_fuel[["reported_fuel_type_code"]].drop_duplicates().dropna()
    )
    wtd_sulfur_content_fuel = eia923_gen_fuel_unique_fuel_codes.merge(
        # Check this routine
        eia_wtd_sulfur_content(eia923_boiler),
        on=["reported_fuel_type_code"],
        how="outer",
    ).fillna(0)
    wtd_sulfur_content_fuel.set_index("reported_fuel_type_code", inplace=True)

    #    eia923_gen_fuel = eia923_gen_fuel.rename(
    #        columns={"prime_mover": "Reported Prime Mover"}
    #    )
    index1 = pd.MultiIndex.from_arrays(
        [
            eia923_gen_fuel[col]
            for col in [
                "plant_id",
                "reported_prime_mover",
                "reported_fuel_type_code",
            ]
        ]
    )
    index2 = pd.MultiIndex.from_arrays(
        [
            eia923_boiler[col]
            for col in [
                "plant_id",
                "reported_prime_mover",
                "reported_fuel_type_code",
            ]
        ]
    )
    eia923_gen_fuel_sub = eia923_gen_fuel.loc[~index1.isin(index2)]
    eia923_boiler_sub = eia923_boiler.loc[index2.isin(index1)]

    del index1, index2

    index1 = pd.MultiIndex.from_arrays(
        [
            eia923_gen_fuel[col]
            for col in ["reported_prime_mover", "reported_fuel_type_code"]
        ]
    )
    index2 = pd.MultiIndex.from_arrays(
        [
            eia923_boiler[col]
            for col in ["reported_prime_mover", "reported_fuel_type_code"]
        ]
    )
    eia923_gen_fuel_union_boiler = eia923_gen_fuel.loc[~index1.isin(index2)]
    eia923_gen_fuel_union_boiler = eia923_gen_fuel_union_boiler.loc[
        eia923_gen_fuel_union_boiler["plant_id"].isin(
            eia923_boiler["plant_id"]
        )
    ]

    del index1, index2
    logging.info("Summing eia923 fuel generation")
    eia923_gen_fuel_sub_agg = eia923_gen_fuel_sub.groupby(
        ["plant_id"], as_index=False
    )["total_fuel_consumption_mmbtu", "total_fuel_consumption_quantity"].sum()
    eia923_gen_fuel_sub_agg.columns = [
        "plant_id",
        "Sheet 1_Total Fuel Consumption (MMBtu)",
        "Sheet 1_total_fuel_consumption_quantity",
    ]
    eia923_gen_fuel_sub_agg["plant_id"] = eia923_gen_fuel_sub_agg[
        "plant_id"
    ].astype(str)

    eia923_boiler_sub_agg = eia923_boiler_sub.copy()
    fuel_heating_value_monthly = [
        "mmbtu_per_unit_january",
        "mmbtu_per_unit_february",
        "mmbtu_per_unit_march",
        "mmbtu_per_unit_april",
        "mmbtu_per_unit_may",
        "mmbtu_per_unit_june",
        "mmbtu_per_unit_july",
        "mmbtu_per_unit_august",
        "mmbtu_per_unit_september",
        "mmbtu_per_unit_october",
        "mmbtu_per_unit_november",
        "mmbtu_per_unit_december",
    ]
    fuel_quantity_monthly = [
        "quantity_of_fuel_consumed_january",
        "quantity_of_fuel_consumed_february",
        "quantity_of_fuel_consumed_march",
        "quantity_of_fuel_consumed_april",
        "quantity_of_fuel_consumed_may",
        "quantity_of_fuel_consumed_june",
        "quantity_of_fuel_consumed_july",
        "quantity_of_fuel_consumed_august",
        "quantity_of_fuel_consumed_september",
        "quantity_of_fuel_consumed_october",
        "quantity_of_fuel_consumed_november",
        "quantity_of_fuel_consumed_december",
    ]
    logging.info("Summing eia923 boiler data")
    eia923_boiler_sub_agg["total_fuel_consumption_mmbtu"] = (
        np.multiply(
            eia923_boiler_sub_agg[fuel_heating_value_monthly],
            eia923_boiler_sub_agg[fuel_quantity_monthly],
        )
    ).sum(axis=1, skipna=True)
    eia923_boiler_sub_agg = eia923_boiler_sub_agg.groupby(
        ["plant_id"], as_index=False
    )["total_fuel_consumption_mmbtu", "total_fuel_consumption_quantity"].sum()
    eia923_boiler_sub_agg.columns = [
        "plant_id",
        "Sheet 3_Total Fuel Consumption (MMBtu)",
        "Sheet 3_total_fuel_consumption_quantity",
    ]
    eia923_boiler_sub_agg["plant_id"] = eia923_boiler_sub_agg[
        "plant_id"
    ].astype(str)

    eia923_gen_fuel_union_boiler_agg = eia923_gen_fuel_union_boiler.groupby(
        ["plant_id"], as_index=False
    )["total_fuel_consumption_mmbtu", "total_fuel_consumption_quantity"].sum()
    eia923_gen_fuel_union_boiler_agg.columns = [
        "plant_id",
        "Sheet 1_Union Total Fuel Consumption (MMBtu)",
        "Sheet 1_Union total_fuel_consumption_quantity",
    ]
    eia923_gen_fuel_union_boiler_agg[
        "plant_id"
    ] = eia923_gen_fuel_union_boiler_agg["plant_id"].astype(str)

    eia923_gen_fuel_boiler_agg = eia923_gen_fuel.loc[
        eia923_gen_fuel["plant_id"].isin(eia923_boiler["plant_id"])
    ]
    eia923_gen_fuel_boiler_agg = eia923_gen_fuel_boiler_agg.groupby(
        ["plant_id"], as_index=False
    )["total_fuel_consumption_mmbtu", "total_fuel_consumption_quantity"].sum()
    eia923_gen_fuel_boiler_agg.columns = [
        "plant_id",
        "Sheet 1_Total Fuel Consumption (MMBtu)",
        "Sheet 1_total_fuel_consumption_quantity",
    ]
    eia923_gen_fuel_boiler_agg["plant_id"] = eia923_gen_fuel_boiler_agg[
        "plant_id"
    ].astype(str)

    del fuel_heating_value_monthly, fuel_quantity_monthly

    eia_860_boiler_firing_type = eia860_boiler_design[
        ["plant_id", "boiler_id", "firing_type_1"]
    ].copy()
    eia_860_boiler_firing_type["plant_id"] = eia_860_boiler_firing_type[
        "plant_id"
    ].astype(str, errors="ignore")
    eia923_boiler_firing_type = eia923_boiler_sub.merge(
        eia_860_boiler_firing_type, on=["plant_id", "boiler_id"], how="left"
    )
    eia923_boiler_firing_type["firing_type_1"] = eia923_boiler_firing_type[
        "firing_type_1"
    ].fillna("None")
    eia923_boiler_firing_type["plant_id"] = eia923_boiler_firing_type[
        "plant_id"
    ].astype(str)

    eia_gen_fuel_net_gen_output = eia_gen_fuel_net_gen(eia923_gen_fuel)
    eia_gen_fuel_net_gen_output["Primary Fuel"] = eia_gen_fuel_net_gen_output[
        [
            "AB",
            "BFG",
            "BIT",
            "BLQ",
            "DFO",
            "GEO",
            "JF",
            "KER",
            "LFG",
            "LIG",
            "MSB",
            "MSN",
            "MWH",
            "NG",
            "NUC",
            "OBG",
            "OBL",
            "OBS",
            "OG",
            "OTH",
            "PC",
            "PG",
            "PUR",
            "RC",
            "RFO",
            "SC",
            "SGC",
            "SGP",
            "SLW",
            "SUB",
            "SUN",
            "TDF",
            "WAT",
            "WC",
            "WDL",
            "WDS",
            "WH",
            "WND",
            "WO",
        ]
    ].idxmax(axis=1)
    eia_gen_fuel_net_gen_output[
        "Primary Fuel Net Generation (MWh)"
    ] = eia_gen_fuel_net_gen_output[
        [
            "AB",
            "BFG",
            "BIT",
            "BLQ",
            "DFO",
            "GEO",
            "JF",
            "KER",
            "LFG",
            "LIG",
            "MSB",
            "MSN",
            "MWH",
            "NG",
            "NUC",
            "OBG",
            "OBL",
            "OBS",
            "OG",
            "OTH",
            "PC",
            "PG",
            "PUR",
            "RC",
            "RFO",
            "SC",
            "SGC",
            "SGP",
            "SLW",
            "SUB",
            "SUN",
            "TDF",
            "WAT",
            "WC",
            "WDL",
            "WDS",
            "WH",
            "WND",
            "WO",
        ]
    ].max(
        axis=1
    )
    eia_gen_fuel_net_gen_output["Primary Fuel %"] = (
        eia_gen_fuel_net_gen_output["Primary Fuel Net Generation (MWh)"]
        / eia_gen_fuel_net_gen_output["Annual Net Generation (MWh)"]
    )

    eia_gen_fuel_net_gen_output = eia_gen_fuel_net_gen_output.assign(
        Primary_Fuel=eia_gen_fuel_net_gen_output.apply(
            eia_primary_fuel, axis=1
        )
    )
    if not model_specs.keep_mixed_plant_category:
        eia_gen_fuel_net_gen_output = eia_gen_fuel_net_gen_output.loc[
                eia_gen_fuel_net_gen_output["Primary_Fuel"]!="Mixed Fuel Type", :
                ]
    plant_fuel_class = eia_gen_fuel_net_gen_output[
        ["plant_id", "Primary_Fuel", "Primary Fuel %"]
    ].copy()
    plant_fuel_class["plant_id"] = plant_fuel_class["plant_id"].astype(str)
    logging.info("Generating co2, ch4, n2o from gen fuel")
    eia_gen_fuel_co2_ch4_n2o_output = eia_gen_fuel_co2_ch4_n2o_emissions(
        eia923_gen_fuel
    )
    logging.info("Generating so2 emissions from gen fuel")
    eia_gen_fuel_so2_output = eia_gen_fuel_so2_emissions(eia923_gen_fuel_sub)
    logging.info("Generating nox emissions from gen fuel")
    eia_gen_fuel_nox_output = eia_gen_fuel_nox_emissions(eia923_gen_fuel_sub)
    logging.info("Generating co2, ch4, n2o emissions from boiler")
    eia_boiler_co2_ch4_n2o_output = eia_boiler_co2_ch4_n2o_emissions(
        eia923_boiler
    )
    # This seems to be the long one.
    logging.info("Generating so2 emissions from boiler fuel")
    eia_boiler_so2_output = eia_boiler_so2_emissions(eia923_boiler_firing_type)
    logging.info("Generating nox emissions from boiler fuel")
    eia_boiler_nox_output = eia_boiler_nox_emissions(eia923_boiler_firing_type)

    ampd_rev = ampd[
        (ampd["co2_mass_tons"] > 0)
        & (ampd["so2_mass_tons"] > 0)
        & (ampd["nox_mass_tons"] > 0)
        & (ampd["heat_content_mmbtu"] > 0)
    ].copy()
    ampd_rev["ampd CO2 (Tons)"] = ampd_rev["co2_mass_tons"] * pq.convert(
        1, "ton", "ton"
    )
    ampd_rev["ampd SO2 (lbs)"] = ampd_rev["so2_mass_tons"] * pq.convert(
        1, "ton", "lb"
    )
    ampd_rev["ampd NOX (lbs)"] = ampd_rev["nox_mass_tons"] * pq.convert(
        1, "ton", "lb"
    )
    ampd_rev = ampd_rev.rename(
        columns={
            "gross_load_mwh": "ampd Gross Generation (MWh)",
            "heat_content_mmbtu": "ampd Heat Input (MMBtu)",
        }
    )
    ampd_rev = ampd_rev[
        [
            "plant_id_eia",
            "ampd CO2 (Tons)",
            "ampd SO2 (lbs)",
            "ampd NOX (lbs)",
            "ampd Gross Generation (MWh)",
            "ampd Heat Input (MMBtu)",
        ]
    ]
    ampd_rev["plant_id"] = ampd_rev["plant_id_eia"].astype(str)

    eia_923_gen_fuel_plant = eia923_gen_fuel.groupby(
        ["plant_id", "plant_name", "operator_name"], as_index=False
    )["net_generation_megawatthours", "total_fuel_consumption_mmbtu"].sum()
    eia_923_gen_fuel_plant["plant_id"] = eia_923_gen_fuel_plant[
        "plant_id"
    ].astype(str)

    df_list = [
        eia_gen_fuel_co2_ch4_n2o_output,
        eia_gen_fuel_so2_output,
        eia_gen_fuel_nox_output,
        eia_boiler_co2_ch4_n2o_output,
        eia_boiler_so2_output,
        eia_boiler_nox_output,
    ]
    logging.info("Choosing emission sources")
    emissions_comparer = pd.concat(df_list, sort=True)
    eia_plant = emissions_comparer.groupby(
        ["plant_id", "plant_name", "operator_name"], as_index=False
    )["CO2 (Tons)", "CH4 (lbs)", "N2O (lbs)", "SO2 (lbs)", "NOx (lbs)"].sum()
    eia_plant = eia_plant.merge(
        eia_923_gen_fuel_plant,
        on=["plant_id", "plant_name", "operator_name"],
        how="left",
    )
    result_agg = eia_plant.merge(ampd_rev, on=["plant_id"], how="left")
    result_agg = result_agg.merge(
        plant_fuel_class, on=["plant_id"], how="left"
    )
    result_agg["plant_id"] = result_agg["plant_id"].astype(int)

    result_agg_final = result_agg.copy()
    result_agg_final["CO2_emissions_tons"] = result_agg_final["CO2 (Tons)"]
    result_agg_final["CO2_Source"] = "ap42"
    fuel_input_criteria = result_agg_final["ampd Heat Input (MMBtu)"].between(
        result_agg_final["total_fuel_consumption_mmbtu"] * 0.8,
        result_agg_final["total_fuel_consumption_mmbtu"] * 1.2,
    )
    emission_criteria = result_agg_final["ampd CO2 (Tons)"].between(
        result_agg_final["CO2 (Tons)"] * (1 / 100),
        result_agg_final["CO2 (Tons)"] * 100,
    )
    total_criteria = (fuel_input_criteria) & (emission_criteria)
    result_agg_final.loc[
        total_criteria, "CO2_emissions_tons"
    ] = result_agg_final.loc[total_criteria, "ampd CO2 (Tons)"]
    result_agg_final.loc[total_criteria, "CO2_Source"] = "ampd"

    result_agg_final["SO2_emissions_lbs"] = result_agg_final["SO2 (lbs)"]
    result_agg_final["SO2_Source"] = "ap42"
    emission_criteria = result_agg_final["ampd SO2 (lbs)"].between(
        result_agg_final["SO2 (lbs)"] * (1 / 100),
        result_agg_final["SO2 (lbs)"] * 100,
    )
    total_criteria = (fuel_input_criteria) & (emission_criteria)
    result_agg_final.loc[
        total_criteria, "SO2_emissions_lbs"
    ] = result_agg_final.loc[total_criteria, "ampd SO2 (lbs)"]
    result_agg_final.loc[total_criteria, "SO2_Source"] = "ampd"

    result_agg_final["NOx_emissions_lbs"] = result_agg_final["NOx (lbs)"]
    result_agg_final["NOx_Source"] = "ap42"
    emission_criteria = result_agg_final["ampd NOX (lbs)"].between(
        result_agg_final["NOx (lbs)"] * (1 / 100),
        result_agg_final["NOx (lbs)"] * 100,
    )
    total_criteria = (fuel_input_criteria) & (emission_criteria)
    result_agg_final.loc[
        total_criteria, "NOx_emissions_lbs"
    ] = result_agg_final.loc[total_criteria, "ampd NOX (lbs)"]
    result_agg_final.loc[
        total_criteria, "NOx_Source"] = "ampd"

    result_agg_final["Net Efficiency"] = (
        result_agg_final["net_generation_megawatthours"]
        * pq.convert(1, "MW*h", "Btu")
        / 10 ** 6
        / result_agg_final["total_fuel_consumption_mmbtu"]
    )
    result_agg_final["Gross Efficiency"] = (
        result_agg_final["ampd Gross Generation (MWh)"]
        * pq.convert(1, "MW*h", "Btu")
        / 10 ** 6
        / result_agg_final["ampd Heat Input (MMBtu)"]
    )

    netl_harmonized = result_agg_final[
        [
            "plant_id",
            "plant_name",
            "operator_name",
            "Primary_Fuel",
            "net_generation_megawatthours",
            "total_fuel_consumption_mmbtu",
            "Net Efficiency",
            "CO2_emissions_tons",
            "CH4 (lbs)",
            "N2O (lbs)",
            "SO2_emissions_lbs",
            "NOx_emissions_lbs",
            "CO2_Source",
            "SO2_Source",
            "NOx_Source",
        ]
    ]
    netl_harmonized = netl_harmonized.rename(
        columns={
            "CO2_emissions_tons": "CO2 (Tons)",
            "SO2_emissions_lbs": "SO2 (lbs)",
            "NOx_emissions_lbs": "NOx (lbs)",
            "total_fuel_consumption_mmbtu": "Total Fuel Consumption (MMBtu)",
        }
    )
    logging.info("Melting and mapping")
    netl_harmonized_melt = netl_harmonized.melt(
        id_vars=[
            "plant_id",
            "plant_name",
            "operator_name",
            "Primary_Fuel",
            "net_generation_megawatthours",
            "Total Fuel Consumption (MMBtu)",
            "Net Efficiency",
            "CO2_Source",
            "SO2_Source",
            "NOx_Source",
        ],
        var_name="Flow",
    )
    conversion_dict = {
        "CO2 (Tons)": pq.convert(1, "ton", "kg"),
        "CH4 (lbs)": pq.convert(1, "lb", "kg"),
        "N2O (lbs)": pq.convert(1, "lb", "kg"),
        "SO2 (lbs)": pq.convert(1, "lb", "kg"),
        "NOx (lbs)": pq.convert(1, "lb", "kg"),
    }
    netl_harmonized_melt["Conv_factor"] = netl_harmonized_melt["Flow"].map(
        conversion_dict
    )
    netl_harmonized_melt["value"] = (
        netl_harmonized_melt["value"] * netl_harmonized_melt["Conv_factor"]
    )
    netl_harmonized_melt.drop(columns=["Conv_factor"], inplace=True)
    netl_harmonized_melt["Unit"] = "kg"
    netl_harmonized_melt["Year"] = int(year)
    netl_harmonized_melt["Source"] = "ap42"
    netl_harmonized_melt.loc[
        netl_harmonized_melt["Flow"] == "CO2 (Tons)", "Source"
    ] = netl_harmonized_melt["CO2_Source"]
    netl_harmonized_melt.loc[
        netl_harmonized_melt["Flow"] == "SO2 (lbs)", "Source"
    ] = netl_harmonized_melt["SO2_Source"]
    netl_harmonized_melt.loc[
        netl_harmonized_melt["Flow"] == "NOx (lbs)", "Source"
    ] = netl_harmonized_melt["NOx_Source"]
    netl_harmonized_melt.drop(
        columns=["CO2_Source", "SO2_Source", "NOx_Source"], inplace=True
    )
    flowlist = fedelemflowlist.get_flows()
    co2_flow = flowlist.loc[
        (
            (flowlist["Flowable"] == "Carbon dioxide")
            & (flowlist["Context"] == "emission/air")
        ),
        :,
    ]
    so2_flow = flowlist.loc[
        (
            (flowlist["Flowable"] == "Sulfur dioxide")
            & (flowlist["Context"] == "emission/air")
        ),
        :,
    ]
    nox_flow = flowlist.loc[
        (
            (flowlist["Flowable"] == "Nitrogen oxides")
            & (flowlist["Context"] == "emission/air")
        ),
        :,
    ]
    n2o_flow = flowlist.loc[
        (
            (flowlist["Flowable"] == "Nitrous oxide")
            & (flowlist["Context"] == "emission/air")
        ),
        :,
    ]
    ch4_flow = flowlist.loc[
        (
            (flowlist["Flowable"] == "Methane")
            & (flowlist["Context"] == "emission/air")
        ),
        :,
    ]
    flow_df = pd.concat([co2_flow, ch4_flow, n2o_flow, so2_flow, nox_flow])
    flow_df["Flow"] = [
        "CO2 (Tons)",
        "CH4 (lbs)",
        "N2O (lbs)",
        "SO2 (lbs)",
        "NOx (lbs)",
    ]
    netl_harmonized_melt = netl_harmonized_melt.merge(
        flow_df[["Flow", "Flowable", "Context", "Flow UUID"]],
        on=["Flow"],
        how="left",
    )
    netl_harmonized_melt.drop(columns=["Flow"], inplace=True)
    netl_harmonized_melt.rename(
        columns={
            "value": "FlowAmount",
            "Flowable": "FlowName",
            "plant_id": "eGRID_ID",
            "Context": "Compartment_path",
            "Flow UUID": "FlowUUID",
            "Primary_Fuel": "PrimaryFuel",
        },
        inplace=True,
    )
    netl_harmonized_melt["Compartment"] = netl_harmonized_melt[
        "Compartment_path"
    ].map(COMPARTMENT_MAP)
    netl_harmonized_melt["FuelCategory"] = netl_harmonized_melt[
        "PrimaryFuel"
    ].map(FUELCAT_MAP)
    # if model_specs.use_primaryfuel_for_coal:
    #     netl_harmonized_melt.loc[
    #         netl_harmonized_melt["FuelCategory"] == "COAL", "FuelCategory"
    #     ] = netl_harmonized_melt.loc[
    #         netl_harmonized_melt["FuelCategory"] == "COAL", "PrimaryFuel"
    #     ]
    netl_harmonized_melt["DataCollection"] = 5
    netl_harmonized_melt["GeographicalCorrelation"] = 1
    netl_harmonized_melt["TechnologicalCorrelation"] = 1
    netl_harmonized_melt["DataReliability"] = 1
    netl_harmonized_melt.sort_values(by=["eGRID_ID", "FlowName"], inplace=True)
    netl_harmonized_melt["eGRID_ID"] = netl_harmonized_melt["eGRID_ID"].astype(
        int
    )
    netl_harmonized_melt.reset_index(inplace=True, drop=True)
    return netl_harmonized_melt


if __name__ == "__main__":
    netl_harmonized_melt = generate_plant_emissions(2016)
    netl_harmonized_melt.to_csv(f"{output_dir}/netl_harmonized.csv")
