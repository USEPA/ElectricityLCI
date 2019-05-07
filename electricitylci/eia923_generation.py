import pandas as pd
import zipfile
import io
import os
from os.path import join
import requests
from electricitylci.globals import (
    data_dir,
    EIA923_BASE_URL,
    FUEL_CAT_CODES,
)
from electricitylci.utils import download_unzip, find_file_in_folder
from electricitylci.model_config import (
    include_only_egrid_facilities_with_positive_generation,
    filter_on_efficiency,
    filter_on_min_plant_percent_generation_from_primary_fuel,
    min_plant_percent_generation_from_primary_fuel_category,
    filter_non_egrid_emission_on_NAICS,
    egrid_facility_efficiency_filters,
    inventories_of_interest,
    eia_gen_year,
)
from electricitylci.eia860_facilities import eia860_balancing_authority


def eia923_download(year, save_path):
    """
    Download and unzip one year of EIA 923 annual data to a subfolder
    of the data directory

    Parameters
    ----------
    year : int or str
        The year of data to download and save
    save_path : path or str
        A folder where the zip file contents should be extracted

    """
    current_url = EIA923_BASE_URL + 'xls/f923_{}.zip'.format(year)
    archive_url = EIA923_BASE_URL + 'archive/xls/f923_{}.zip'.format(year)

    # try to download using the most current year url format
    try:
        download_unzip(current_url, save_path)
    except ValueError:
        download_unzip(archive_url, save_path)


def load_eia923_excel(eia923_path):

    eia = pd.read_excel(eia923_path,
                        sheet_name='Page 1 Generation and Fuel Data',
                        header=5,
                        na_values=['.'],
                        dtype={'Plant Id': str,
                               'YEAR': str,
                               'NAICS Code': str})
    # Get ride of line breaks. And apparently 2015 had 'Plant State'
    # instead of 'State'
    eia.columns = (eia.columns.str.replace('\n', ' ')
                              .str.replace('Plant State', 'State'))

    # colstokeep = [
    #     'Plant Id',
    #     'Plant Name',
    #     'State',
    #     'Reported Prime Mover',
    #     'Reported Fuel Type Code',
    #     'Total Fuel Consumption MMBtu',
    #     'Net Generation (Megawatthours)',
    #     'YEAR'
    # ]
    # eia = eia.loc[:, colstokeep]

    return eia


def eia923_download_extract(
    year,
    group_cols = [
        'Plant Id',
        'Plant Name',
        'State',
        'NAICS Code',
        'Reported Prime Mover',
        'Reported Fuel Type Code',
        'YEAR'
    ]
):
    """
    Download (if necessary) and extract a single year of generation/fuel
    consumption data from EIA-923.

    Data are grouped by plant level

    Parameters
    ----------
    year : int or str
        Year of data to download/extract
    group_cols : list, optional
        The columns from EIA923 generation and fuel sheet to use when grouping
        generation and fuel consumption data.

    """
    expected_923_folder = join(data_dir, 'f923_{}'.format(year))

    if not os.path.exists(expected_923_folder):
        print('Downloading EIA-923 files')
        eia923_download(year=year, save_path=expected_923_folder)

        eia923_path, eia923_name = find_file_in_folder(
            folder_path=expected_923_folder,
            file_pattern_match='2_3_4_5',
            return_name=True
        )
        # eia923_files = os.listdir(expected_923_folder)

        # # would be more elegent with glob but this works to identify the
        # # Schedule_2_3_4_5 file
        # for f in eia923_files:
        #     if '2_3_4_5' in f:
        #         gen_file = f

        # eia923_path = join(expected_923_folder, gen_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia923_excel(eia923_path)

        # Save as csv for easier access in future
        csv_fn = eia923_name.split('.')[0] + '.csv'
        csv_path = join(expected_923_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_923_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [f for f in all_files
                    if '.csv' in f
                    and '{}_Final'.format(year) in f]

        # Read and return the existing csv file if it exists
        if csv_file:
            print('Loading {} EIA-923 data from csv file'.format(year))
            fn = csv_file[0]
            csv_path = join(expected_923_folder, fn)
            eia = pd.read_csv(csv_path,
                              dtype={'Plant Id': str,
                                     'YEAR': str,
                                     'NAICS Code': str})

        else:
            print('Loading data from previously downloaded excel file,',
                  ' how did the csv file get deleted?')
            eia923_path, eia923_name = find_file_in_folder(
                folder_path=expected_923_folder,
                file_pattern_match='2_3_4_5',
                return_name=True
            )

            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2_3_4_5' in f:
            #         gen_file = f
            # eia923_path = join(expected_923_folder, gen_file)
            eia = load_eia923_excel(eia923_path)

            csv_fn = eia923_name.split('.')[0] + '.csv'
            csv_path = join(expected_923_folder, csv_fn)
            eia.to_csv(csv_path, index=False)

    # EIA_923 = eia
    # Grouping similar facilities together.
    # group_cols = ['Plant Id', 'Plant Name', 'State', 'YEAR']
    sum_cols = [
        'Total Fuel Consumption MMBtu',
        'Net Generation (Megawatthours)'
    ]
    EIA_923_generation_data = eia.groupby(group_cols,
                                          as_index=False)[sum_cols].sum()

    return EIA_923_generation_data


def group_fuel_categories(df):

    new_fuel_categories = df['Reported Fuel Type Code'].map(FUEL_CAT_CODES)

    return new_fuel_categories


def eia923_primary_fuel(eia923_gen_fuel=None, year=None,
                        method_col='Net Generation (Megawatthours)'):
    """
    Determine the primary fuel for each power plant. Include the NAICS code
    for each plant in output.

    Primary fuel can be determined using either generation (output) or fuel
    consumption (input). EIA923 doesn't list fuel inputs for non-combustion
    generation (wind, sun, hydro, etc), so an additional step to determine
    the primary fuel of these plants if 'Total Fuel Consumption MMBtu' is
    selected as the method.

    Parameters
    ----------
    year : int
        Year of 923 data
    method_col : str, optional
        The method to use when determining the primary fuel of a power plant
        (the default is 'Net Generation (Megawatthours)', and the alternative
        is 'Total Fuel Consumption MMBtu')

    """
    if year:
        eia923_gen_fuel = eia923_download_extract(year)
    # eia923_gen_fuel['FuelCategory'] = group_fuel_categories(eia923_gen_fuel)
    # eia923_gen_fuel.rename(columns={'Reported Fuel Type Code'})

    group_cols = ['Plant Id', 'NAICS Code', 'Reported Fuel Type Code']

    sum_cols = [
        'Net Generation (Megawatthours)',
        'Total Fuel Consumption MMBtu',
    ]
    plant_fuel_total = (eia923_gen_fuel.groupby(
                            group_cols, as_index=False
                        )[sum_cols].sum())

    # Find the dataframe index for the fuel with the most gen at each plant
    # Use this to slice the dataframe and return plant code and primary fuel
    primary_fuel_idx = (plant_fuel_total.groupby('Plant Id')[method_col].idxmax())

    data_cols = [
        'Plant Id',
        'NAICS Code',
        'Reported Fuel Type Code',
        'Net Generation (Megawatthours)',
    ]
    primary_fuel = plant_fuel_total.loc[primary_fuel_idx, data_cols]

    # Also going to include the percent of total generation from primary
    # fuel
    total_gen_plant = eia923_gen_fuel.groupby('Plant Id', as_index=False)[
        'Net Generation (Megawatthours)'
    ].sum()
    total_gen_plant.rename(columns={'Net Generation (Megawatthours)':'total_gen'},
                           inplace=True)
    primary_fuel = primary_fuel.merge(total_gen_plant, on='Plant Id')
    primary_fuel['primary fuel percent gen'] = (
        primary_fuel['Net Generation (Megawatthours)']
        / primary_fuel['total_gen']
        * 100
    )

    primary_fuel['FuelCategory'] = group_fuel_categories(primary_fuel)
    primary_fuel.rename(columns={'Reported Fuel Type Code': 'PrimaryFuel'}, inplace=True)



    primary_fuel.reset_index(inplace=True, drop=True)

    keep_cols = [
        'Plant Id',
        'NAICS Code',
        'FuelCategory',
        'PrimaryFuel',
        'primary fuel percent gen'
    ]

    return primary_fuel.loc[:, keep_cols]


def calculate_plant_efficiency(gen_fuel_data):

    plant_total = gen_fuel_data.groupby('Plant Id', as_index=False).sum()
    plant_total['efficiency'] = (plant_total['Net Generation (Megawatthours)']
                                 * 10
                                 / (plant_total['Total Fuel Consumption MMBtu']
                                    * 3.412) * 100)
    return plant_total


def efficiency_filter(df):

    upper = egrid_facility_efficiency_filters['upper_efficiency']
    lower = egrid_facility_efficiency_filters['lower_efficiency']

    df = df.loc[(df['efficiency'] >= lower)
                & (df['efficiency'] <= upper), :]

    return df


def build_generation_data(egrid_facilities_to_include=None, generation_years=None):
    """
    Build a dataset of facility-level generation using EIA923. This
    function will apply filters for positive generation, generation
    efficiency within a given range, and a minimum percent of generation
    from the primary fuel (if set in the config file). The returned
    dataframe also includes the balancing authority for every power
    plant.

    Parameters
    ----------
    egrid_facilities_to_include : list, optional
        List of plant codes to include (default is None, which builds a list)
    generation_years : list, optional
        Years of generation data to include in the output (default is None,
        which builds a list from the inventories of interest and eia_gen_year
        parameters)

    Returns
    ----------
    DataFrame

    Dataframe columns include:
    ['FacilityID', 'Electricity', 'Year']
    """

    if not generation_years:
        # Use the years from inventories of interest
        generation_years = set(
            list(inventories_of_interest.values())
            + [eia_gen_year]
        )

    df_list = []
    for year in generation_years:
        if not egrid_facilities_to_include:
            gen_fuel_data = eia923_download_extract(year)
            primary_fuel = eia923_primary_fuel(gen_fuel_data)
            gen_efficiency = calculate_plant_efficiency(gen_fuel_data)

            final_gen_df = gen_efficiency.merge(primary_fuel, on='Plant Id')

            if include_only_egrid_facilities_with_positive_generation:
                final_gen_df = final_gen_df.loc[
                    final_gen_df['Net Generation (Megawatthours)'] >= 0, :
                ]
            if filter_on_efficiency:
                final_gen_df = efficiency_filter(final_gen_df)
            if filter_on_min_plant_percent_generation_from_primary_fuel:
                final_gen_df = final_gen_df.loc[
                    final_gen_df['primary fuel percent gen']
                    >= min_plant_percent_generation_from_primary_fuel_category, :
                ]
            # if filter_non_egrid_emission_on_NAICS:
            #     # Check with Wes to see what the filter here is supposed to be
            #     final_gen_df = final_gen_df.loc[
            #         final_gen_df['NAICS Code'] == '22', :
            #     ]
        else:
            final_gen_df = final_gen_df.loc[
                final_gen_df['Plant Id'].isin(egrid_facilities_to_include), :
            ]

        ba_match = eia860_balancing_authority(year)

        final_gen_df = final_gen_df.merge(ba_match, on='Plant Id', how='left')
        final_gen_df['Year'] = int(year)
        df_list.append(final_gen_df)

    all_years_gen = pd.concat(df_list)

    all_years_gen = all_years_gen.rename(
        columns={
            'Plant Id': 'FacilityID',
            'Net Generation (Megawatthours)': 'Electricity',
        }
    )

    all_years_gen = all_years_gen.loc[:, ['FacilityID', 'Electricity', 'Year']]
    all_years_gen.reset_index(drop=True, inplace=True)
    all_years_gen['Year']=all_years_gen['Year'].astype('int32')
    return all_years_gen
