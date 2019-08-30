"""
Download and import EIA860 data, including power plant information such as
plant code, location, balancing authority, and primary fuel type.

This is using most of the code from eia923_generation.py. It could be
combined and generalized in the future.

"""
import pandas as pd
import zipfile
import io
import os
from os.path import join
import requests
from electricitylci.globals import data_dir, EIA860_BASE_URL
from electricitylci.utils import (
    download_unzip,
    find_file_in_folder,
    create_ba_region_map,
)
from electricitylci.model_config import regional_aggregation


def _clean_columns(df):
    "Remove special characters and convert column names to snake case"
    df.columns = (
        df.columns.str.lower()
        .str.replace("[^0-9a-zA-Z\-]+", " ")
        .str.replace("-", "")
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


def eia860_download(year, save_path):
    """
    Download and unzip one year of EIA 860 annual data to a subfolder
    of the data directory

    Parameters
    ----------
    year : int or str
        The year of data to download and save
    save_path : path or str
        A folder where the zip file contents should be extracted

    """
    current_url = EIA860_BASE_URL + "xls/eia860{}.zip".format(year)
    archive_url = EIA860_BASE_URL + "archive/xls/eia860{}.zip".format(year)

    # try to download using the most current year url format
    try:
        download_unzip(current_url, save_path)
    except ValueError:
        download_unzip(archive_url, save_path)


def load_eia860_excel(eia860_path, sheet="Plant", header=1):
    eia = pd.read_excel(
        eia860_path,
        sheet_name=sheet,
        header=header,
        na_values=[".", " "],
        dtype={"Plant Code": str},
    )
    # Get ride of line breaks. Rename Plant Code to Plant Id (match
    # the 923 column name)
    eia.columns = (
        eia.columns.str.replace("\n", " ")
        .str.replace("Plant Code", "Plant Id")
        .str.replace("Plant State", "State")
    )

    return eia


def eia860_balancing_authority(year):

    expected_860_folder = join(data_dir, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        print("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["2___Plant"],
            return_name=True,
        )
        # eia860_files = os.listdir(expected_860_folder)

        # # would be more elegent with glob but this works to identify the
        # # Schedule_2_3_4_5 file
        # for f in eia860_files:
        #     if '2___Plant' in f:
        #         plant_file = f

        # eia860_path = join(expected_860_folder, plant_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia860_excel(eia860_path)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + ".csv"
        csv_path = join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if ".csv" in f and "Plant_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            print("Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str})

        else:
            print("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["2___Plant"],
                return_name=True,
            )
            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2___Plant' in f:
            #         plant_file = f
            # eia860_path = join(expected_860_folder, plant_file)
            eia = load_eia860_excel(eia860_path)

            csv_fn = eia860_name.split(".")[0] + ".csv"
            csv_path = join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)

    ba_cols = [
        "Plant Id",
        "State",
        "NERC Region",
        "Balancing Authority Code",
        "Balancing Authority Name",
    ]
    eia_plant_ba_match = eia.loc[:, ba_cols].drop_duplicates()

    # Map the balancing authority to a larger region (e.g. FERC or EIA)
    if regional_aggregation:
        region_map = create_ba_region_map(region_col=regional_aggregation)
        eia_plant_ba_match[regional_aggregation] = eia_plant_ba_match[
            "Balancing Authority Code"
        ].map(region_map)

    return eia_plant_ba_match


def eia860_primary_capacity(year):

    pass


def eia860_EnviroAssoc_so2(year):
    expected_860_folder = join(data_dir, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        print("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
            return_name=True,
        )
        # eia860_files = os.listdir(expected_860_folder)

        # # would be more elegent with glob but this works to identify the
        # # Schedule_2_3_4_5 file
        # for f in eia860_files:
        #     if '2___Plant' in f:
        #         plant_file = f

        # eia860_path = join(expected_860_folder, plant_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia860_excel(eia860_path, "Boiler SO2", 1)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_so2.csv"
        csv_path = join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_boiler_so2.csv" in f
            and "6_1_EnviroAssoc_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            print("Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str})

        else:
            print("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
                return_name=True,
            )
            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2___Plant' in f:
            #         plant_file = f
            # eia860_path = join(expected_860_folder, plant_file)
            eia = load_eia860_excel(eia860_path, "Boiler SO2", 1)

            csv_fn = eia860_name.split(".")[0] + "_boiler_so2.csv"
            csv_path = join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia


def eia860_boiler_info_design(year):
    expected_860_folder = join(data_dir, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        print("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["6_2_EnviroEquip", "xlsx"],
            return_name=True,
        )

        eia = load_eia860_excel(
            eia860_path, "Boiler Info & Design Parameters", 1
        )

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_info.csv"
        csv_path = join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_boiler_info.csv" in f
            and "6_2_EnviroEquip_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            print("Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str})

        else:
            print("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_2_EnviroEquip", "xlsx"],
                return_name=True,
            )
            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2___Plant' in f:
            #         plant_file = f
            # eia860_path = join(expected_860_folder, plant_file)
            eia = load_eia860_excel(
                eia860_path, "Boiler Info & Design Parameters", 1
            )

            csv_fn = eia860_name.split(".")[0] + "_boiler_info.csv"
            csv_path = join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia


def eia860_EnviroAssoc_nox(year):
    expected_860_folder = join(data_dir, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        print("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
            return_name=True,
        )
        # eia860_files = os.listdir(expected_860_folder)

        # # would be more elegent with glob but this works to identify the
        # # Schedule_2_3_4_5 file
        # for f in eia860_files:
        #     if '2___Plant' in f:
        #         plant_file = f

        # eia860_path = join(expected_860_folder, plant_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia860_excel(eia860_path, "Boiler NOx", 1)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_boiler_nox.csv"
        csv_path = join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_boiler_nox.csv" in f
            and "6_1_EnviroAssoc_Y{}".format(year) in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            print("Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str})

        else:
            print("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["6_1_EnviroAssoc", "xlsx"],
                return_name=True,
            )
            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2___Plant' in f:
            #         plant_file = f
            # eia860_path = join(expected_860_folder, plant_file)
            eia = load_eia860_excel(eia860_path, "Boiler NOx", 1)

            csv_fn = eia860_name.split(".")[0] + "_boiler_nox.csv"
            csv_path = join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia

def eia860_generator_info(year):
    expected_860_folder = join(data_dir, "eia860_{}".format(year))

    if not os.path.exists(expected_860_folder):
        print("Downloading EIA-860 files")
        eia860_download(year=year, save_path=expected_860_folder)

        eia860_path, eia860_name = find_file_in_folder(
            folder_path=expected_860_folder,
            file_pattern_match=["3_1_Generator", "xlsx"],
            return_name=True,
        )
        # eia860_files = os.listdir(expected_860_folder)

        # # would be more elegent with glob but this works to identify the
        # # Schedule_2_3_4_5 file
        # for f in eia860_files:
        #     if '2___Plant' in f:
        #         plant_file = f

        # eia860_path = join(expected_860_folder, plant_file)

        # colstokeep = group_cols + sum_cols
        eia = load_eia860_excel(eia860_path, "Operable", 1)

        # Save as csv for easier access in future
        csv_fn = eia860_name.split(".")[0] + "_generator_operable.csv"
        csv_path = join(expected_860_folder, csv_fn)
        eia.to_csv(csv_path, index=False)

    else:
        all_files = os.listdir(expected_860_folder)

        # Check for both csv and year<_Final> in case multiple years
        # or other csv files exist
        csv_file = [
            f
            for f in all_files
            if "_generator_operable.csv" in f
            and "3_1_Generator" in f
        ]

        # Read and return the existing csv file if it exists
        if csv_file:
            print("Loading {} EIA-860 plant data from csv file".format(year))
            fn = csv_file[0]
            csv_path = join(expected_860_folder, fn)
            eia = pd.read_csv(csv_path, dtype={"Plant Id": str})

        else:
            print("Loading data from previously downloaded excel file")
            eia860_path, eia860_name = find_file_in_folder(
                folder_path=expected_860_folder,
                file_pattern_match=["3_1_Generator", "xlsx"],
                return_name=True,
            )
            # # would be more elegent with glob but this works to identify the
            # # Schedule_2_3_4_5 file
            # for f in all_files:
            #     if '2___Plant' in f:
            #         plant_file = f
            # eia860_path = join(expected_860_folder, plant_file)
            eia = load_eia860_excel(eia860_path, "Operable", 1)

            csv_fn = eia860_name.split(".")[0] + "_generator_operable.csv"
            csv_path = join(expected_860_folder, csv_fn)
            eia.to_csv(csv_path, index=False)
    eia = _clean_columns(eia)
    return eia


if __name__ == "__main__":
    eia_nox = eia860_EnviroAssoc_nox(2016)
    eia_so2 = eia860_EnviroAssoc_so2(2016)
    eia_boiler = eia860_boiler_info_design(2016)
