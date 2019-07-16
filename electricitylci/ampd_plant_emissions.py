import pandas as pd
import numpy as np
from electricitylci.globals import data_dir, output_dir
import electricitylci.PhysicalQuantities as pq
import electricitylci.cems_data as cems
import electricitylci.eia923_generation as eia923
import fedelemflowlist
from electricitylci.model_config import use_primaryfuel_for_coal
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
        the boiler or generator fuel type level (market as Source = "ap42").
    """
    COMPARTMENT_MAP = {"emission/air": "air"}
    FUELCAT_MAP = {
        "AB": "BIOMASS",
        #            "BFG",
        "BIT": "COAL",
        #            "BLQ",
        "DFO": "OIL",
        "GEO": "GEO",
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
            df, on="Plant Id", how="left"
        )
        emissions_check["Check Heat Input MMBtu"] = emissions_check[
            "Total Fuel Consumption MMBtu"
        ].fillna(0) - emissions_check[
            "Sheet 1_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input Quantity"] = emissions_check[
            "Total Fuel Consumption Quantity"
        ].fillna(0) - emissions_check[
            "Sheet 1_Total Fuel Consumption Quantity"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input MMBtu Ratio"] = emissions_check[
            "Total Fuel Consumption MMBtu"
        ].fillna(0) / emissions_check[
            "Sheet 1_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input Quantity Ratio"] = emissions_check[
            "Total Fuel Consumption Quantity"
        ].fillna(0) / emissions_check[
            "Sheet 1_Total Fuel Consumption Quantity"
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
                df_, on=["Plant Id"], how="left"
            )
        emissions_check["Check Heat Input MMBtu_Boiler"] = emissions_check[
            "Total Fuel Consumption MMBtu"
        ].fillna(0) - emissions_check[
            "Sheet 3_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input Quantity_Boiler"] = emissions_check[
            "Total Fuel Consumption Quantity"
        ].fillna(0) - emissions_check[
            "Sheet 3_Total Fuel Consumption Quantity"
        ].fillna(
            0
        )
        emissions_check[
            "Check Heat Input MMBtu_Boiler Ratio"
        ] = emissions_check["Total Fuel Consumption MMBtu"].fillna(
            0
        ) / emissions_check[
            "Sheet 3_Total Fuel Consumption (MMBtu)"
        ].fillna(
            0
        )
        emissions_check[
            "Check Heat Input Quantity_Boiler Ratio"
        ] = emissions_check["Total Fuel Consumption Quantity"].fillna(
            0
        ) / emissions_check[
            "Sheet 3_Total Fuel Consumption Quantity"
        ].fillna(
            0
        )
        emissions_check["Check Heat Input MMBtu_Boiler_Gen"] = (
            emissions_check["Total Fuel Consumption MMBtu"].fillna(0)
            + emissions_check[
                "Sheet 1_Union Total Fuel Consumption (MMBtu)"
            ].fillna(0)
        ) - emissions_check["Sheet 1_Total Fuel Consumption (MMBtu)"].fillna(0)
        emissions_check["Check Heat Input Quantity_Boiler_Gen"] = (
            emissions_check["Total Fuel Consumption Quantity"].fillna(0)
            + emissions_check[
                "Sheet 1_Union Total Fuel Consumption Quantity"
            ].fillna(0)
        ) - emissions_check["Sheet 1_Total Fuel Consumption Quantity"].fillna(
            0
        )
        emissions_check["Check Heat Input MMBtu_Boiler_Gen Ratio"] = (
            emissions_check["Total Fuel Consumption MMBtu"].fillna(0)
            + emissions_check[
                "Sheet 1_Union Total Fuel Consumption (MMBtu)"
            ].fillna(0)
        ) / emissions_check["Sheet 1_Total Fuel Consumption (MMBtu)"].fillna(0)
        emissions_check["Check Heat Input Quantity_Boiler_Gen Ratio"] = (
            emissions_check["Total Fuel Consumption Quantity"].fillna(0)
            + emissions_check[
                "Sheet 1_Union Total Fuel Consumption Quantity"
            ].fillna(0)
        ) / emissions_check["Sheet 1_Total Fuel Consumption Quantity"].fillna(
            0
        )

        return emissions_check

    def eia_gen_fuel_co2_ch4_n2o_emissions(eia923_gen_fuel):

        emissions = pd.DataFrame()

        for row in ef_co2_ch4_n2o.itertuples():

            fuel_type = eia923_gen_fuel_sub.loc[
                eia923_gen_fuel_sub["Reported Fuel Type Code"].astype(str)
                == str(row.EIA_Fuel_Type_Code)
            ].copy()

            fuel_type["CO2 (Tons)"] = (row.ton_CO2_mmBtu) * fuel_type[
                "Total Fuel Consumption MMBtu"
            ].astype(float, errors="ignore")
            fuel_type["CH4 (lbs)"] = (row.pound_methane_per_mmbtu) * fuel_type[
                "Total Fuel Consumption MMBtu"
            ].astype(float, errors="ignore")
            fuel_type["N2O (lbs)"] = (row.pound_n2o_per_mmBtu) * fuel_type[
                "Total Fuel Consumption MMBtu"
            ].astype(float, errors="ignore")

            emissions = pd.concat([emissions, fuel_type])

        emissions_agg = emissions.groupby(
            ["Plant Id", "Plant Name", "Operator Name"]
        )[
            "CO2 (Tons)",
            "CH4 (lbs)",
            "N2O (lbs)",
            "Total Fuel Consumption MMBtu",
            "Total Fuel Consumption Quantity",
        ].sum()
        emissions_agg = emissions_agg.reset_index()
        emissions_agg["Plant Id"] = emissions_agg["Plant Id"].astype(str)

        return emissions_agg

    def eia_boiler_co2_ch4_n2o_emissions(eia923_boiler):

        emissions = pd.DataFrame()

        for row in ef_co2_ch4_n2o.itertuples():

            fuel_type = eia923_boiler_sub.loc[
                eia923_boiler_sub["Reported Fuel Type Code"].astype(str)
                == str(row.EIA_Fuel_Type_Code)
            ].copy()

            fuel_heating_value_monthly = [
                "MMBtu Per Unit January",
                "MMBtu Per Unit February",
                "MMBtu Per Unit March",
                "MMBtu Per Unit April",
                "MMBtu Per Unit May",
                "MMBtu Per Unit June",
                "MMBtu Per Unit July",
                "MMBtu Per Unit August",
                "MMBtu Per Unit September",
                "MMBtu Per Unit October",
                "MMBtu Per Unit November",
                "MMBtu Per Unit December",
            ]
            fuel_quantity_monthly = [
                "Quantity Of Fuel Consumed January",
                "Quantity Of Fuel Consumed February",
                "Quantity Of Fuel Consumed March",
                "Quantity Of Fuel Consumed April",
                "Quantity Of Fuel Consumed May",
                "Quantity Of Fuel Consumed June",
                "Quantity Of Fuel Consumed July",
                "Quantity Of Fuel Consumed August",
                "Quantity Of Fuel Consumed September",
                "Quantity Of Fuel Consumed October",
                "Quantity Of Fuel Consumed November",
                "Quantity Of Fuel Consumed December",
            ]

            fuel_type["Total Fuel Consumption MMBtu"] = (
                np.multiply(
                    fuel_type[fuel_heating_value_monthly],
                    fuel_type[fuel_quantity_monthly],
                )
            ).sum(axis=1, skipna=True)

            fuel_type["CO2 (Tons)"] = (row.ton_CO2_mmBtu) * fuel_type[
                "Total Fuel Consumption MMBtu"
            ].astype(float, errors="ignore")
            fuel_type["CH4 (lbs)"] = (row.pound_methane_per_mmbtu) * fuel_type[
                "Total Fuel Consumption MMBtu"
            ].astype(float, errors="ignore")
            fuel_type["N2O (lbs)"] = (row.pound_n2o_per_mmBtu) * fuel_type[
                "Total Fuel Consumption MMBtu"
            ].astype(float, errors="ignore")

            emissions = pd.concat([emissions, fuel_type])

        emissions_agg = emissions.groupby(
            ["Plant Id", "Plant Name", "Operator Name"], as_index=False
        )[
            "CH4 (lbs)",
            "N2O (lbs)",
            "CO2 (Tons)",
            "Total Fuel Consumption MMBtu",
            "Total Fuel Consumption Quantity",
        ].sum()
        emissions_agg["Plant Id"] = emissions_agg["Plant Id"].astype(str)

        return emissions_agg

    def eia_gen_fuel_net_gen(eia923_gen_fuel):

        net_gen_monthly = [
            "Netgen January",
            "Netgen February",
            "Netgen March",
            "Netgen April",
            "Netgen May",
            "Netgen June",
            "Netgen July",
            "Netgen August",
            "Netgen September",
            "Netgen October",
            "Netgen November",
            "Netgen December",
        ]
        eia923_gen_fuel["Annual Net Generation (MWh)"] = eia923_gen_fuel[
            net_gen_monthly
        ].sum(axis=1, skipna=True)
        eia_923_gen_fuel_agg = eia923_gen_fuel.groupby(
            ["Plant Id", "Plant Name", "Operator Name"]
        )["Annual Net Generation (MWh)"].sum()
        eia_923_gen_fuel_agg = eia_923_gen_fuel_agg.reset_index()
        eia_923_gen_fuel_agg_fuel_type = eia923_gen_fuel.groupby(
            [
                "Plant Id",
                "Plant Name",
                "Operator Name",
                "Reported Fuel Type Code",
            ]
        )["Annual Net Generation (MWh)"].sum()
        eia_923_gen_fuel_agg_fuel_type = (
            eia_923_gen_fuel_agg_fuel_type.reset_index()
        )
        eia_923_gen_fuel_agg_fuel_type_pivot = eia_923_gen_fuel_agg_fuel_type.pivot(
            index="Plant Id",
            columns="Reported Fuel Type Code",
            values="Annual Net Generation (MWh)",
        )
        eia_923_gen_fuel_agg_fuel_type_pivot = (
            eia_923_gen_fuel_agg_fuel_type_pivot.reset_index()
        )
        eia_923_gen_fuel_agg = eia_923_gen_fuel_agg.merge(
            eia_923_gen_fuel_agg_fuel_type_pivot, on="Plant Id", how="left"
        )
        eia_923_gen_fuel_agg["Plant Id"] = eia_923_gen_fuel_agg[
            "Plant Id"
        ].astype(str)

        return eia_923_gen_fuel_agg

    def eia_gen_fuel_so2_emissions(eia923_gen_fuel_sub):

        #        emissions = pd.DataFrame()
        emissions = eia923_gen_fuel_sub.merge(
            ef_so2.loc[ef_so2["Boiler_Firing_Type_Code"] == "None", :],
            left_on=["Reported Prime Mover", "Reported Fuel Type Code"],
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
            emissions.loc[criteria, "Total Fuel Consumption Quantity"]
            * emissions.loc[criteria, "Emission_Factor"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] == "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "No"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "Total Fuel Consumption MMBtu"]
            * emissions.loc[criteria, "Emission_Factor"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] == "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "Yes"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "Avg Sulfur Content (%)"]
            * emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "Total Fuel Consumption MMBtu"]
        )
        criteria = (emissions["Emission_Factor_Denominator"] != "MMBtu") & (
            emissions["Multiply_by_S_Content"] == "Yes"
        )
        emissions.loc[criteria, "SO2 (lbs)"] = (
            emissions.loc[criteria, "Avg Sulfur Content (%)"]
            * emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "Total Fuel Consumption Quantity"]
        )

        emissions_agg = emissions.groupby(
            ["Plant Id", "Plant Name", "Operator Name"], as_index=False
        )[
            "SO2 (lbs)",
            "Total Fuel Consumption Quantity",
            "Total Fuel Consumption MMBtu",
        ].sum()
        emissions_agg["Plant Id"] = emissions_agg["Plant Id"].astype(str)

        return emissions_agg

    def eia_boiler_so2_emissions(eia923_boiler_firing_type):

        fuel_heating_value_monthly = [
            "MMBtu Per Unit January",
            "MMBtu Per Unit February",
            "MMBtu Per Unit March",
            "MMBtu Per Unit April",
            "MMBtu Per Unit May",
            "MMBtu Per Unit June",
            "MMBtu Per Unit July",
            "MMBtu Per Unit August",
            "MMBtu Per Unit September",
            "MMBtu Per Unit October",
            "MMBtu Per Unit November",
            "MMBtu Per Unit December",
        ]
        fuel_quantity_monthly = [
            "Quantity Of Fuel Consumed January",
            "Quantity Of Fuel Consumed February",
            "Quantity Of Fuel Consumed March",
            "Quantity Of Fuel Consumed April",
            "Quantity Of Fuel Consumed May",
            "Quantity Of Fuel Consumed June",
            "Quantity Of Fuel Consumed July",
            "Quantity Of Fuel Consumed August",
            "Quantity Of Fuel Consumed September",
            "Quantity Of Fuel Consumed October",
            "Quantity Of Fuel Consumed November",
            "Quantity Of Fuel Consumed December",
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
            "Sulfur Content January",
            "Sulfur Content February",
            "Sulfur Content March",
            "Sulfur Content April",
            "Sulfur Content May",
            "Sulfur Content June",
            "Sulfur Content July",
            "Sulfur Content August",
            "Sulfur Content September",
            "Sulfur Content October",
            "Sulfur Content November",
            "Sulfur Content December",
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
                "Reported Prime Mover",
                "Reported Fuel Type Code",
                "Firing Type 1",
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
        emissions["Total Fuel Consumption MMBtu"] = emissions[
            fuel_heat_quantity_monthly
        ].sum(axis=1)
        emissions_merge = emissions.merge(
            eia_so2_rem_eff, on=["Plant Id", "Boiler Id"], how="left"
        )
        emissions_merge[
            "SO2 Removal Efficiency Rate at Annual Operating Factor"
        ] = emissions_merge[
            "SO2 Removal Efficiency Rate at Annual Operating Factor"
        ].fillna(
            0
        )
        emissions_merge["SO2 (lbs) with AEC"] = emissions_merge[
            "SO2 (lbs)"
        ] * (
            1
            - emissions_merge[
                "SO2 Removal Efficiency Rate at Annual Operating Factor"
            ]
        )
        emissions_agg = emissions_merge.groupby(
            ["Plant Id", "Plant Name", "Operator Name"], as_index=False
        )[
            "SO2 (lbs) with AEC",
            "Total Fuel Consumption Quantity",
            "Total Fuel Consumption MMBtu",
        ].sum()
        emissions_agg["Plant Id"] = emissions_agg["Plant Id"].astype(str)
        emissions_agg = emissions_agg.rename(
            columns={"SO2 (lbs) with AEC": "SO2 (lbs)"}
        )

        return emissions_agg

    def eia_gen_fuel_nox_emissions(eia923_gen_fuel_sub):

        #        emissions = pd.DataFrame()
        emissions = eia923_gen_fuel_sub.merge(
            ef_nox,
            left_on=["Reported Fuel Type Code", "Reported Prime Mover"],
            right_on=["Reported_Fuel_Type_Code", "Reported_Prime_Mover"],
            how="left",
        )
        emissions["NOx (lbs)"] = None
        criteria = emissions["Emission_Factor_Denominator"] == "MMBtu"
        emissions.loc[criteria, "NOx (lbs)"] = (
            emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "Total Fuel Consumption MMBtu"]
        )
        criteria = emissions["Emission_Factor_Denominator"] != "MMBtu"
        emissions.loc[criteria, "NOx (lbs)"] = (
            emissions.loc[criteria, "Emission_Factor"]
            * emissions.loc[criteria, "Total Fuel Consumption Quantity"]
        )
        emissions_agg = emissions.groupby(
            ["Plant Id", "Plant Name", "Operator Name"], as_index=False
        )[
            "NOx (lbs)",
            "Total Fuel Consumption Quantity",
            "Total Fuel Consumption MMBtu",
        ].sum()
        emissions_agg["Plant Id"] = emissions_agg["Plant Id"].astype(str)

        return emissions_agg

    def eia_boiler_nox(row):
        if row["NOX Emission Rate Entire Year (lbs/MMBtu)"] > 0:
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
                "Reported Fuel Type Code",
                "Reported Prime Mover",
                "Firing Type 1",
            ],
            right_on=[
                "Reported_Fuel_Type_Code",
                "Reported_Prime_Mover",
                "Boiler_Firing_Type_Code",
            ],
            how="left",
        )
        emissions["NOx (lbs)"] = emissions["Emission_Factor"] * emissions[
            "Total Fuel Consumption Quantity"
        ].astype(float, errors="ignore")

        emissions.dropna(subset=["NOx (lbs)"], inplace=True)
        emissions["Total Fuel Consumption MMBtu"] = emissions[
            fuel_heat_quantity_monthly
        ].sum(axis=1)
        emissions_boiler = emissions.merge(
            eia_nox_rate, on=["Plant Id", "Boiler Id"], how="left"
        )
        emissions_boiler["NOx Based on Annual Rate (lbs)"] = (
            emissions_boiler["Total Fuel Consumption MMBtu"]
            * emissions_boiler["NOX Emission Rate Entire Year (lbs/MMBtu)"]
        )
        emissions_boiler = emissions_boiler.assign(
            NOx_lbs=emissions_boiler.apply(eia_boiler_nox, axis=1)
        )
        emissions_agg = emissions_boiler.groupby(
            ["Plant Id", "Plant Name", "Operator Name"], as_index=False
        )[
            "NOx_lbs",
            "Total Fuel Consumption Quantity",
            "Total Fuel Consumption MMBtu",
        ].sum()
        emissions_agg["Plant Id"] = emissions_agg["Plant Id"].astype(str)
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
            subset=["Reported Fuel Type Code"]
        )
        eia923_boiler_unique_fuel_codes = (
            eia923_boiler[["Reported Fuel Type Code"]]
            .drop_duplicates()
            .dropna()
        )
        eia923_boiler_unique_fuel_codes.columns = ["Reported_Fuel_Type_Code"]

        for row in eia923_boiler_unique_fuel_codes.itertuples():
            fuel_type = eia923_boiler_drop_na.loc[
                eia923_boiler_drop_na["Reported Fuel Type Code"].astype(str)
                == str(row.Reported_Fuel_Type_Code)
            ].copy()
            fuel_quantity_monthly = [
                "Quantity Of Fuel Consumed January",
                "Quantity Of Fuel Consumed February",
                "Quantity Of Fuel Consumed March",
                "Quantity Of Fuel Consumed April",
                "Quantity Of Fuel Consumed May",
                "Quantity Of Fuel Consumed June",
                "Quantity Of Fuel Consumed July",
                "Quantity Of Fuel Consumed August",
                "Quantity Of Fuel Consumed September",
                "Quantity Of Fuel Consumed October",
                "Quantity Of Fuel Consumed November",
                "Quantity Of Fuel Consumed December",
            ]
            sulfur_content_monthly = [
                "Sulfur Content January",
                "Sulfur Content February",
                "Sulfur Content March",
                "Sulfur Content April",
                "Sulfur Content May",
                "Sulfur Content June",
                "Sulfur Content July",
                "Sulfur Content August",
                "Sulfur Content September",
                "Sulfur Content October",
                "Sulfur Content November",
                "Sulfur Content December",
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
            ["Reported Fuel Type Code"], as_index=False
        )["Sulfur Weighted", "Total Fuel Consumption Quantity"].sum()
        sulfur_content_agg["Avg Sulfur Content (%)"] = (
            sulfur_content_agg["Sulfur Weighted"]
            / sulfur_content_agg["Total Fuel Consumption Quantity"]
        )
        sulfur_content_agg = sulfur_content_agg[
            ["Reported Fuel Type Code", "Avg Sulfur Content (%)"]
        ]

        return sulfur_content_agg

    def eia_primary_fuel(row):
        if row["Primary Fuel %"] < primary_fuel_threshold:
            return "Mixed Fuel Type"
        else:
            return row["Primary Fuel"]

    def emissions_logic_CO2(row):
        if (
            (
                row["ampd Heat Input (MMBtu)"]
                < row["Total Fuel Consumption MMBtu"] * 1.2
            )
            | (
                row["ampd Heat Input (MMBtu)"]
                > row["Total Fuel Consumption MMBtu"] * 0.8
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
                < row["Total Fuel Consumption MMBtu"] * 1.2
            )
            | (
                row["ampd Heat Input (MMBtu)"]
                > row["Total Fuel Consumption MMBtu"] * 0.8
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
                < row["Total Fuel Consumption MMBtu"] * 1.2
            )
            | (
                row["ampd Heat Input (MMBtu)"]
                > row["Total Fuel Consumption MMBtu"] * 0.8
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
    eia923_gen_fuel = pd.read_pickle(
        f"{data_dir}/EIA 923/Pickle Files/Generation and Fuel/EIA 923 Generation and Fuel {year}.pkl"
    )
    eia923_boiler = pd.read_pickle(
        f"{data_dir}/EIA 923/Pickle Files/Boiler Fuel/EIA 923 Boiler Fuel {year}.pkl"
    )
    eia923_aec = pd.read_pickle(
        f"{data_dir}/EIA 923/Pickle Files/Air Emissions Control/EIA 923 AEC {year}.pkl"
    )
    eia860_env_assoc_boiler_NOx = pd.read_pickle(
        f"{data_dir}/EIA 860/Pickle Files/Environmental Associations/EIA 860 Boiler NOx {year}.pkl"
    )
    eia860_env_assoc_boiler_SO2 = pd.read_pickle(
        f"{data_dir}/EIA 860/Pickle Files/Environmental Associations/EIA 860 Boiler SO2 {year}.pkl"
    )
    eia860_boiler_design = pd.read_pickle(
        f"{data_dir}/EIA 860/Pickle Files/Boiler Info & Design Parameters/EIA 860 Boiler Design {year}.pkl"
    )
    ef_co2_ch4_n2o = pd.read_excel(
        f"{data_dir}/EFs/eLCI EFs.xlsx", sheet_name="CO2,CH4,N2O"
    )
    ef_so2 = pd.read_csv(f"{data_dir}/EFs/eLCI EFs_SO2.csv", index_col=0)
    ef_nox = pd.read_csv(f"{data_dir}/EFs/eLCI EFs_NOx.csv", index_col=0)
    eia_nox_rate = eia923_aec[
        [
            "Plant Id",
            "NOX Control Id",
            "NOX Emission Rate Entire Year (lbs/MMBtu)",
        ]
    ].copy()

    eia_nox_rate["NOX Emission Rate Entire Year (lbs/MMBtu)"] = eia_nox_rate[
        "NOX Emission Rate Entire Year (lbs/MMBtu)"
    ].apply(pd.to_numeric, errors="coerce")
    eia_nox_rate = eia_nox_rate.dropna().drop_duplicates()
    eia_nox_rate["NOX Control Id"] = eia_nox_rate["NOX Control Id"].astype(str)
    eia_nox_rate["Plant Id"] = eia_nox_rate["Plant Id"].astype(str)
    eia_nox_rate = eia_nox_rate.merge(
        eia860_env_assoc_boiler_NOx[
            ["Plant Id", "NOX Control Id", "Boiler Id"]
        ],
        on=["Plant Id", "NOX Control Id"],
        how="left",
    )
    eia_nox_rate = eia_nox_rate.dropna()
    eia_nox_rate = eia_nox_rate[
        ["Plant Id", "NOX Emission Rate Entire Year (lbs/MMBtu)", "Boiler Id"]
    ].drop_duplicates(["Plant Id", "Boiler Id"])

    eia_so2_rem_eff = eia923_aec[
        [
            "Plant Id",
            "SO2 Control Id",
            "SO2 Removal Efficiency Rate at Annual Operating Factor",
        ]
    ].copy()
    eia_so2_rem_eff[
        "SO2 Removal Efficiency Rate at Annual Operating Factor"
    ] = eia_so2_rem_eff[
        "SO2 Removal Efficiency Rate at Annual Operating Factor"
    ].apply(
        pd.to_numeric, errors="coerce"
    )
    eia_so2_rem_eff = eia_so2_rem_eff.dropna().drop_duplicates()
    eia_so2_rem_eff["SO2 Control Id"] = eia_so2_rem_eff[
        "SO2 Control Id"
    ].astype(str)
    eia_so2_rem_eff["Plant Id"] = eia_so2_rem_eff["Plant Id"].astype(str)
    eia_so2_rem_eff = eia_so2_rem_eff.merge(
        eia860_env_assoc_boiler_SO2[
            ["Plant Id", "SO2 Control Id", "Boiler Id"]
        ],
        on=["Plant Id", "SO2 Control Id"],
        how="left",
    )
    eia_so2_rem_eff = eia_so2_rem_eff.dropna()
    eia_so2_rem_eff = eia_so2_rem_eff[
        [
            "Plant Id",
            "SO2 Removal Efficiency Rate at Annual Operating Factor",
            "Boiler Id",
        ]
    ].drop_duplicates(["Plant Id", "Boiler Id"])

    eia923_gen_fuel_unique_fuel_codes = (
        eia923_gen_fuel[["Reported Fuel Type Code"]].drop_duplicates().dropna()
    )
    wtd_sulfur_content_fuel = eia923_gen_fuel_unique_fuel_codes.merge(
        # Check this routine
        eia_wtd_sulfur_content(eia923_boiler),
        on=["Reported Fuel Type Code"],
        how="outer",
    ).fillna(0)
    wtd_sulfur_content_fuel.set_index("Reported Fuel Type Code", inplace=True)

    eia923_gen_fuel = eia923_gen_fuel.rename(
        columns={"Prime Mover": "Reported Prime Mover"}
    )
    index1 = pd.MultiIndex.from_arrays(
        [
            eia923_gen_fuel[col]
            for col in [
                "Plant Id",
                "Reported Prime Mover",
                "Reported Fuel Type Code",
            ]
        ]
    )
    index2 = pd.MultiIndex.from_arrays(
        [
            eia923_boiler[col]
            for col in [
                "Plant Id",
                "Reported Prime Mover",
                "Reported Fuel Type Code",
            ]
        ]
    )
    eia923_gen_fuel_sub = eia923_gen_fuel.loc[~index1.isin(index2)]
    eia923_boiler_sub = eia923_boiler.loc[index2.isin(index1)]

    del index1, index2

    index1 = pd.MultiIndex.from_arrays(
        [
            eia923_gen_fuel[col]
            for col in ["Reported Prime Mover", "Reported Fuel Type Code"]
        ]
    )
    index2 = pd.MultiIndex.from_arrays(
        [
            eia923_boiler[col]
            for col in ["Reported Prime Mover", "Reported Fuel Type Code"]
        ]
    )
    eia923_gen_fuel_union_boiler = eia923_gen_fuel.loc[~index1.isin(index2)]
    eia923_gen_fuel_union_boiler = eia923_gen_fuel_union_boiler.loc[
        eia923_gen_fuel_union_boiler["Plant Id"].isin(
            eia923_boiler["Plant Id"]
        )
    ]

    del index1, index2
    logging.info("Summing eia923 fuel generation")
    eia923_gen_fuel_sub_agg = eia923_gen_fuel_sub.groupby(
        ["Plant Id"], as_index=False
    )["Total Fuel Consumption MMBtu", "Total Fuel Consumption Quantity"].sum()
    eia923_gen_fuel_sub_agg.columns = [
        "Plant Id",
        "Sheet 1_Total Fuel Consumption (MMBtu)",
        "Sheet 1_Total Fuel Consumption Quantity",
    ]
    eia923_gen_fuel_sub_agg["Plant Id"] = eia923_gen_fuel_sub_agg[
        "Plant Id"
    ].astype(str)

    eia923_boiler_sub_agg = eia923_boiler_sub.copy()
    fuel_heating_value_monthly = [
        "MMBtu Per Unit January",
        "MMBtu Per Unit February",
        "MMBtu Per Unit March",
        "MMBtu Per Unit April",
        "MMBtu Per Unit May",
        "MMBtu Per Unit June",
        "MMBtu Per Unit July",
        "MMBtu Per Unit August",
        "MMBtu Per Unit September",
        "MMBtu Per Unit October",
        "MMBtu Per Unit November",
        "MMBtu Per Unit December",
    ]
    fuel_quantity_monthly = [
        "Quantity Of Fuel Consumed January",
        "Quantity Of Fuel Consumed February",
        "Quantity Of Fuel Consumed March",
        "Quantity Of Fuel Consumed April",
        "Quantity Of Fuel Consumed May",
        "Quantity Of Fuel Consumed June",
        "Quantity Of Fuel Consumed July",
        "Quantity Of Fuel Consumed August",
        "Quantity Of Fuel Consumed September",
        "Quantity Of Fuel Consumed October",
        "Quantity Of Fuel Consumed November",
        "Quantity Of Fuel Consumed December",
    ]
    logging.info("Summing eia923 boiler data")
    eia923_boiler_sub_agg["Total Fuel Consumption MMBtu"] = (
        np.multiply(
            eia923_boiler_sub_agg[fuel_heating_value_monthly],
            eia923_boiler_sub_agg[fuel_quantity_monthly],
        )
    ).sum(axis=1, skipna=True)
    eia923_boiler_sub_agg = eia923_boiler_sub_agg.groupby(
        ["Plant Id"], as_index=False
    )["Total Fuel Consumption MMBtu", "Total Fuel Consumption Quantity"].sum()
    eia923_boiler_sub_agg.columns = [
        "Plant Id",
        "Sheet 3_Total Fuel Consumption (MMBtu)",
        "Sheet 3_Total Fuel Consumption Quantity",
    ]
    eia923_boiler_sub_agg["Plant Id"] = eia923_boiler_sub_agg[
        "Plant Id"
    ].astype(str)

    eia923_gen_fuel_union_boiler_agg = eia923_gen_fuel_union_boiler.groupby(
        ["Plant Id"], as_index=False
    )["Total Fuel Consumption MMBtu", "Total Fuel Consumption Quantity"].sum()
    eia923_gen_fuel_union_boiler_agg.columns = [
        "Plant Id",
        "Sheet 1_Union Total Fuel Consumption (MMBtu)",
        "Sheet 1_Union Total Fuel Consumption Quantity",
    ]
    eia923_gen_fuel_union_boiler_agg[
        "Plant Id"
    ] = eia923_gen_fuel_union_boiler_agg["Plant Id"].astype(str)

    eia923_gen_fuel_boiler_agg = eia923_gen_fuel.loc[
        eia923_gen_fuel["Plant Id"].isin(eia923_boiler["Plant Id"])
    ]
    eia923_gen_fuel_boiler_agg = eia923_gen_fuel_boiler_agg.groupby(
        ["Plant Id"], as_index=False
    )["Total Fuel Consumption MMBtu", "Total Fuel Consumption Quantity"].sum()
    eia923_gen_fuel_boiler_agg.columns = [
        "Plant Id",
        "Sheet 1_Total Fuel Consumption (MMBtu)",
        "Sheet 1_Total Fuel Consumption Quantity",
    ]
    eia923_gen_fuel_boiler_agg["Plant Id"] = eia923_gen_fuel_boiler_agg[
        "Plant Id"
    ].astype(str)

    del fuel_heating_value_monthly, fuel_quantity_monthly

    eia_860_boiler_firing_type = eia860_boiler_design[
        ["Plant Id", "Boiler Id", "Firing Type 1"]
    ].copy()
    eia_860_boiler_firing_type["Plant Id"] = eia_860_boiler_firing_type[
        "Plant Id"
    ].astype(float, errors="ignore")
    eia923_boiler_firing_type = eia923_boiler_sub.merge(
        eia_860_boiler_firing_type, on=["Plant Id", "Boiler Id"], how="left"
    )
    eia923_boiler_firing_type["Firing Type 1"] = eia923_boiler_firing_type[
        "Firing Type 1"
    ].fillna("None")
    eia923_boiler_firing_type["Plant Id"] = eia923_boiler_firing_type[
        "Plant Id"
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

    primary_fuel_threshold = 0.9

    eia_gen_fuel_net_gen_output = eia_gen_fuel_net_gen_output.assign(
        Primary_Fuel=eia_gen_fuel_net_gen_output.apply(
            eia_primary_fuel, axis=1
        )
    )
    plant_fuel_class = eia_gen_fuel_net_gen_output[
        ["Plant Id", "Primary_Fuel", "Primary Fuel %"]
    ].copy()
    plant_fuel_class["Plant Id"] = plant_fuel_class["Plant Id"].astype(str)
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
        1, "ton", "Mg"
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
    ampd_rev["Plant Id"] = ampd_rev["plant_id_eia"].astype(str)

    eia_923_gen_fuel_plant = eia923_gen_fuel.groupby(
        ["Plant Id", "Plant Name", "Operator Name"], as_index=False
    )["Net Generation (MWh)", "Total Fuel Consumption MMBtu"].sum()
    eia_923_gen_fuel_plant["Plant Id"] = eia_923_gen_fuel_plant[
        "Plant Id"
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
        ["Plant Id", "Plant Name", "Operator Name"], as_index=False
    )["CO2 (Tons)", "CH4 (lbs)", "N2O (lbs)", "SO2 (lbs)", "NOx (lbs)"].sum()
    eia_plant = eia_plant.merge(
        eia_923_gen_fuel_plant,
        on=["Plant Id", "Plant Name", "Operator Name"],
        how="left",
    )
    result_agg = eia_plant.merge(ampd_rev, on=["Plant Id"], how="left")
    result_agg = result_agg.merge(
        plant_fuel_class, on=["Plant Id"], how="left"
    )
    result_agg["Plant Id"] = result_agg["Plant Id"].astype(int)

    result_agg_final = result_agg.copy()
    result_agg_final["CO2_emissions_tons"] = result_agg_final["CO2 (Tons)"]
    result_agg_final["CO2_Source"] = "ap42"
    fuel_input_criteria = result_agg_final["ampd Heat Input (MMBtu)"].between(
        result_agg_final["Total Fuel Consumption MMBtu"] * 0.8,
        result_agg_final["Total Fuel Consumption MMBtu"] * 1.2,
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
    ] = result_agg_final.loc[total_criteria, "SO2 (lbs)"]
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
    ] = result_agg_final.loc[total_criteria, "NOx (lbs)"]
    result_agg_final["NOx_Source"] = "ampd"
    result_agg_final = result_agg_final.assign(
        NOx_emissions_lbs=result_agg_final.apply(emissions_logic_NOx, axis=1)
    )
    result_agg_final["NOx_emissions_lbs"], result_agg_final[
        "NOx_Source"
    ] = zip(*result_agg_final["NOx_emissions_lbs"])

    result_agg_final["Net Efficiency"] = (
        result_agg_final["Net Generation (MWh)"]
        * pq.convert(1, "MW*h", "Btu")
        / 10 ** 6
        / result_agg_final["Total Fuel Consumption MMBtu"]
    )
    result_agg_final["Gross Efficiency"] = (
        result_agg_final["ampd Gross Generation (MWh)"]
        * pq.convert(1, "MW*h", "Btu")
        / 10 ** 6
        / result_agg_final["ampd Heat Input (MMBtu)"]
    )

    netl_harmonized = result_agg_final[
        [
            "Plant Id",
            "Plant Name",
            "Operator Name",
            "Primary_Fuel",
            "Net Generation (MWh)",
            "Total Fuel Consumption MMBtu",
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
            "Total Fuel Consumption MMBtu": "Total Fuel Consumption (MMBtu)",
        }
    )
    logging.info("Melting and mapping")
    netl_harmonized_melt = netl_harmonized.melt(
        id_vars=[
            "Plant Id",
            "Plant Name",
            "Operator Name",
            "Primary_Fuel",
            "Net Generation (MWh)",
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
            "Plant Id": "eGRID_ID",
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
    if use_primaryfuel_for_coal:
        netl_harmonized_melt.loc[
            netl_harmonized_melt["FuelCategory"] == "COAL", "FuelCategory"
        ] = netl_harmonized_melt.loc[
            netl_harmonized_melt["FuelCategory"] == "COAL", "PrimaryFuel"
        ]
    netl_harmonized_melt["DataCollection"] = 5
    netl_harmonized_melt["GeographicalCorrelation"] = 1
    netl_harmonized_melt["TechnologicalCorrelation"] = 1
    netl_harmonized_melt["ReliabilityScore"] = 1
    netl_harmonized_melt.sort_values(by=["eGRID_ID", "FlowName"], inplace=True)
    netl_harmonized_melt["eGRID_ID"] = netl_harmonized_melt["eGRID_ID"].astype(
        int
    )
    netl_harmonized_melt.reset_index(inplace=True, drop=True)
    return netl_harmonized_melt


if __name__ == "__main__":
    netl_harmonized_melt = generate_plant_emissions(2016)
    netl_harmonized_melt.to_csv(f"{output_dir}/netl_harmonized.csv")
