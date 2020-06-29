import openpyxl
import pandas as pd
import numpy as np

# from electricitylci.egrid_facilities import egrid_subregions
from electricitylci.globals import data_dir
from electricitylci.model_config import net_trading, replace_egrid
from electricitylci.process_dictionary_writer import (
    exchange,
    exchange_table_creation_input_con_mix,
    ref_exchange_creator,
    process_table_creation_con_mix,
    process_table_creation_surplus
)

if not replace_egrid:
    wb2 = openpyxl.load_workbook(data_dir+'/eGRID_Consumption_Mix_new.xlsx', data_only=True)
    data = wb2['ConsumptionMixContributions']

    if net_trading == True:
        nerc_region = data['A4:A29']
        surplus_pool_trade_in = data['F4':'F29']
        trade_matrix = data['I3':'AP13']
        generation_quantity = data['E4':'E29']
        nerc_region2 = data['H4:H13']
        egrid_regions = data['C4:C29']

    else:
        nerc_region = data['A36:A61']
        surplus_pool_trade_in = data['F36':'F61']
        trade_matrix = data['I35':'AP45']
        generation_quantity = data['E36':'E61']
        nerc_region2 = data['H36:H45']
        egrid_regions = data['C36:C61']


def surplus_pool_dictionary(nerc_region, surplus_pool_trade_in, trade_matrix, gen_quantity, eGRID_region, nerc_region2):

    surplus_dict = dict()
    for i in range(0, len(nerc_region2)):

        region = nerc_region2[i][0].value
        exchanges_list = []

        exchange(ref_exchange_creator(), exchanges_list)
        # y  = len(trade_matrix[0])

        # chk=0;
        for j in range(0, 34):
            input_region_surplus_amount = trade_matrix[i + 1][j].value
            if input_region_surplus_amount != None and input_region_surplus_amount != 0:
                # name = 'Electricity; at region '+trade_matrix[0][j].value+'; Trade Mix'
                input_region_acronym = trade_matrix[0][j].value
                exchange(exchange_table_creation_input_con_mix(input_region_surplus_amount, input_region_acronym), exchanges_list)
                # exchange(exchange_table_creation_input_con_mix(trade_matrix[i+1][j].value,trade_matrix[0][j].value),exchanges_list)
                # chk = 1;

        final = process_table_creation_surplus(region, exchanges_list)
        print(region+' NERC Surplus Process Created')
        surplus_dict['SurplusPool'+region] = final;
    return surplus_dict


def consumption_mix_dictionary(nerc_region, surplus_pool_trade_in, trade_matrix, generation_quantity, egrid_regions, nerc_region2):

    surplus_dict = surplus_pool_dictionary(nerc_region, surplus_pool_trade_in, trade_matrix, generation_quantity, egrid_regions, nerc_region2)
    # global region
    consumption_dict = dict()
    for reg in range(0, len(egrid_regions)):
        region = egrid_regions[reg][0].value

        exchanges_list = []
        exchange(ref_exchange_creator(), exchanges_list)

        y = len(trade_matrix[0])
        chk = 0;
        for nerc in range(0, len(nerc_region2)):

            if nerc_region[reg][0].value == nerc_region2[nerc][0].value:

                if surplus_pool_trade_in[reg][0].value != 0:

                    for j in range(0, y):

                        # name = surplus_dict[nerc_region[reg][0].value]['name']

                        if trade_matrix[nerc+1][j].value != None and trade_matrix[nerc+1][j].value !=0:
                            print(nerc_region[reg][0].value)
                            exchange(exchange_table_creation_input_con_mix(surplus_pool_trade_in[reg][0].value, nerc_region[reg][0].value), exchanges_list)
                            chk=1;
                            break;
            # name = 'Electricity from generation mix '+eGRID_region[reg][0].value
            # fuelname =
        if chk == 1:
            exchange(exchange_table_creation_input_con_mix(generation_quantity[reg][0].value, region), exchanges_list)
        else:
            exchange(exchange_table_creation_input_con_mix(1, region), exchanges_list)

        final = process_table_creation_con_mix(region, exchanges_list)
        print(region+' Consumption Mix Process Created')
        consumption_dict['Consumption'+region] = final;
    return consumption_dict


def check_trading_normalized(trading_matrix):

    if trading_matrix.iloc[:, 0].sum() > 1:
        for col in trading_matrix.columns:
            trading_matrix[col] /= trading_matrix[col].sum()


def trading_mix_fuels(gen_mix, trading_matrix):
    """
    Calculate the incoming fuel mix of for each region based on an I/O trading
    matrix. The matrix and generation mix should contain the same regions.

    Parameters
    ----------
    gen_mix : dataframe
        Dataframe with columns ['Subregion', 'FuelCategory', 'Electricity',
        'Generation_Ratio'].
    trading_matrix : dataframe
        A square input-output trading matrix of electricity between regions.

    Returns
    -------
    dataframe
        The fraction of every fuel/region combo that makes up consumption
        within a region. Columns include:

        ['region', 'from_region', 'FuelCategory', 'trading_gen_ratio']
    """
    _gen_mix = gen_mix.dropna().set_index('Subregion')
    assert set(_gen_mix.index.unique()).issubset(set(trading_matrix.index))

    check_trading_normalized(trading_matrix)

    regions = trading_matrix.index

    df_list = []
    for region in regions:
        region_df = _gen_mix.copy()

        # The index was set to region names above. Because of this
        # pandas will automatically match the values correctly.
        # Much faster than looping through everything multiple times.
        region_df['trading_amount'] = trading_matrix[region]
        region_df['Subregion'] = region
        region_df['trading_gen_ratio'] = (
            region_df['trading_amount']
            * region_df['Generation_Ratio']
        )

        df_list.append(region_df)

    full_gen_df = pd.concat(df_list)
    full_gen_df['from_region'] = full_gen_df.index
    full_gen_df.dropna(inplace=True)

    keep_cols = [
        'Subregion',
        'from_region',
        'FuelCategory',
        'trading_gen_ratio',
    ]
    full_gen_df = full_gen_df.loc[
        full_gen_df['trading_gen_ratio'] > 0,
        keep_cols
    ].reset_index(drop=True)

    return full_gen_df


def consumption_flows(fuels_mix, flows):
    """
    Calculate the flows (e.g. emissions) attributable to each fuel/region
    combination that is attributable to consumption within each region. For
    example, the emissions of CO2 from electricity generation from every fuel
    type in regions 'A', 'B', 'C', and 'D' that can be attributed to consumption
    of electricity in region 'A', where 'A' exchanges electricity with 'B', 'C',
    and 'D'.

    Parameters
    ----------
    fuels_mix : dataframe
        The mix of fuels attributable to consumption within each region. Includes
        both the columns 'Subregion' for the consumption region and 'from_region'
        for the production region. Fuel names are listed under 'FuelCategory',
        and 'trading_gen_ratio' is the fraction of generation from each fuel/region
        pair.
    flows : dataframe
        Flows attributable to generation from each fuel type in each region.

    Returns
    -------
    dataframe
        Joined fuel mix and flows data.
    """

    results = pd.merge(fuels_mix, flows, left_on=['FuelCategory', 'from_region'],
                       right_on=['FuelCategory', 'Subregion'])

    return results


if not replace_egrid:
    # Creating Surplus Pool dictionary
    surplus_dict = surplus_pool_dictionary(nerc_region, surplus_pool_trade_in, trade_matrix, generation_quantity, egrid_regions, nerc_region2)
    # del surplus_dict['']

    # Creating Consumption dictionary
    consumption_dict = consumption_mix_dictionary(nerc_region, surplus_pool_trade_in, trade_matrix, generation_quantity, egrid_regions, nerc_region2)


# Test distr


# The distribution mix dictionary does not require any new information and it will be fine to use the GEN mix dictionary to write these templates.
# Only extra infomration required is the efficiency
# distribution_dict = distribution_mix_dictionary(egrid_subregions,efficiency_of_distribution_grid)
# del distribution_dict['']


# consumption_mix_template_generator(consumption_dict)
# surplus_pool_mix_template_generator(surplus_dict,nerc_region2)
# distribution_template_generator(distribution_dict,efficiency_of_distribution_grid)
