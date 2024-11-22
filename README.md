# Electricity Life Cycle Inventory

A python package that uses standardized facility releases and generation data to create regionalized life cycle inventory (LCI) models for the generation, mix of generation, mix of consumption, and distribution of electricity to end users for the US, with embedded system processes of upstream fuel production and infrastructure.
Pre-configured model specifications are included (in electricitylci/modelconfig) or users can specify their own models by creating new config.yml files in the modelconfig directory. The created LCI models can be exported for use in standard life cycle assessment software (e.g., in JSON-LD format using the openLCA v2.0 schema).

This code was created as part of a collaboration between US EPA Office of Research and Development (USEPA) and the National Energy Technology Laboratory (NETL) with contributions from the National Renewable Energy Laboratory (NREL) and support from Eastern Research Group (ERG). More information on this effort can be found in the [Framework for an Open-Source Life Cycle Baseline for Electricity Consumption in the United States](https://netl.doe.gov/energy-analysis/details?id=4004).

## Disclaimer

This United States Environmental Protection Agency (EPA) and National Energy Technology Laboratory (NETL) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use. EPA and NETL have relinquished control of the information and no longer
has responsibility to protect the integrity, confidentiality, or availability of the information.
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA or NETL.

# Setup
A Python virtual environment (v 3.11 or higher) is required with the following packages installed, which are the latest as of writing (February 2024).

+ `pip install git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List#egg=fedelemflowlist`
    * Successfully installs:
        + appdirs-1.4.4
        + boto3-1.34.40
        + botocore-1.34.40
        + certifi-2024.2.2
        + charset-normalizer-3.3.2
        + esupy-0.3.3
        + fedelemflowlist-1.2.4
        + idna-3.6
        + jmespath-1.0.1
        + numpy-1.26.4
        + olca-schema-0.0.12
        + pandas-2.2.0
        + pyarrow-15.0.0
        + python-dateutil-2.8.2
        + pytz-2024.1
        + pyyaml-6.0.1
        + requests-2.31.0
        + requests_ftp-0.3.1 s
        + 3transfer-0.10.0
        + six-1.16.0
        + tzdata-2024.1
        + urllib3-2.0.7
+ `pip install git+https://github.com/USEPA/standardizedinventories#egg=StEWI`
    * Successfully installed:
        + StEWI-1.1.3
        + beautifulsoup4-4.12.3
        + et-xmlfile-1.1.0
        + openpyxl-3.1.2
        + soupsieve-2.5
        + xlrd-2.0.1
+ `pip install scipy`
    * Successfully installs:
        + scipy-1.12.0

# API
In the latest version of ElectricityLCI, there is a dependency on two external datasets that require the use of an application programming interface (API) key.
These keys are free to request and assist their governing agencies with reporting and justifying their data service.

The EPA's Continuous Emissions Monitors (CEMs) background data are provided by the Clean Air Markets API Portal.
Request a free API key (a long string of numbers used to unlock data access) at the following site.
Keep your API key secret, as it links you to the their data.

- https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup

The EIA's bulk U.S. Electric System Operating Data (EBA) is provided by their Open Data API (v2).
Request a free API key by registering at the following address.

- https://www.eia.gov/opendata/.

Be careful when the ElectricityLCI prompts you for an API key, as there are two (EPA and EIA).
For convenience, the API keys may be stored in the configuration YAML file in the `epa_cam_api` and `eia_api` parameters.
Make sure you supply the right one!

# Use
Check which eLCI model configuration you want to use, or create a new configuration.
To show the location of the YAML files, run the following:

```py
>>> from electricitylci.globals import get_config_dir
>>> get_config_dir()
'~/ElectricityLCI/electricitylci/modelconfig'
```

The output string may be copied to File Explorer or Finder address bar.

To run the installed package from the terminal/command line:

```sh
$ python -m electricitylci.main
```

To run in a Python interpreter from within a cloned repository:

```py
>>> exec(open("electricitylci/main.py").read())
```

The `main()` method has four steps:

1. `build_model_config()`
    - Prompts the user to select one of the model configurations.
    - The 2016 baseline configurations are:
        * ELCI_1
        * ELCI_2
        * ELCI_3
    - The latest baselines are built from 'ELCI_1' and include:
        * ELCI_2020
        * ELCI_2021
        * ELCI_2022
    - These configurations statically change the module, model_config.py, which is an object read by other modules.
    - To change configuration values, edit the YAML before running the code.
2. `run_generation()`
    - Pulls upstream inventories for coal, natural gas, petroleum, nuclear, and plant construction
    - Creates generation processes
        * Optionally includes renewables (e.g., geothermal, wind, solar PV, solar thermal, and hydroelectric)
        * Optionally includes plant water use
        * Adds Canadian mixes
        * Aggregates to regions of interest (e.g., balancing authority areas or FERC regions)
3. `run_distribution(gen_data, gen_dict)`
    - Creates the at-grid generation mix processes
    - Creates the at-grid consumption mix processes (following one of two trade models)
    - Creates the at-user consumption mix processes (based on calculated transmission and distribution losses)
4. `run_post_processes()`
    - Cleans the JSON-LD files (e.g., removing zero-value product flows, removing untracked flows, and creating consecutive internal exchange IDs)
    - Builds the product systems for balancing authority areas, FERC regions, and US.


# Known Issues
* `Enter EIA API key: ` prompt used for bad EBA.zip data (found in August 2024)
    - In globals.py, there is a paramter `EIA_API` that can be toggled to true
    - You will need an EIA API key (https://www.eia.gov/opendata/)
* `Enter EPA API key: ` prompt encountered during 2020–2022 baseline runs.
    - The CEMS data are no longer available via the old FTP.
    - See https://github.com/USEPA/ElectricityLCI/issues/207
    - In the short-term, a free-to-register EPA Data API key is required (https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup)
    - Copy and paste the API key (a long string of numbers and letters) to the prompt and hit Enter to download the missing data.
    - The missing data are archived in state-based zip files the epacems2020, epacems2021, and epacems2022 folders found in the `paths.local_path` location (as defined in electricitylci.globals.py).
* `ParseError` when importing generation.py.
    - This is likely caused by RCRAInfo download through stewicombo: https://github.com/USEPA/standardizedinventories/issues/151.
    - In egrid_emissions_and_waste_by_facility.py, uncomment the line with 'download_if_missing=True', restart Python kernel, build model specs, and import generation.py. This should download the cached version of the RCRAInfo data. Re-comment the line when you're done.
* `ValueError` encountered during 2021 and 2022 baseline runs.
    - The 'coalpublic2021.xls' and 'coalpublic2022.xls' files downloaded to your f7a_2021 and f7a_2022 folders (in the `paths.local_path` location found in electricitylci.globals.py) are actually XML spreadsheets.
    - The simple fix is to open these files in Microsoft Excel and "Save as" Microsoft Excel 95 Workbook, replacing the previous file with the new (do not leave both copies in the folder!)
* The data quality indicators used for flows and processes (dqi.py) may deviate from the standards:
    - [Flow Pedigree](https://www.lcacommons.gov/lca-collaboration/National_Renewable_Energy_Laboratory/USLCI_Database_Public/dataset/DQ_SYSTEM/d13b2bc4-5e84-4cc8-a6be-9101ebb252ff)
    - [Process Pedigree](https://www.lcacommons.gov/lca-collaboration/National_Renewable_Energy_Laboratory/USLCI_Database_Public/dataset/DQ_SYSTEM/70bf370f-9912-4ec1-baa3-fbd4eaf85a10)
* The mapping of Balancing Authorities to FERC regions may need updating as BAs are introduced to the grid.
    - See GitHub [Issue #215](https://github.com/USEPA/ElectricityLCI/issues/215)

# Troubleshooting
If you receive a TypeError in `write_jsonld`, got unexpected keyword argument 'zw', then it's likely you have an outdated version of [fedelemflowlist](https://github.com/USEPA/fedelemflowlist).
Please ensure these USEPA packages are up-to-date:

- esupy
- fedelemflowlist
- stewi

If GitHub-hosted packages fail to clone and install, manually downloading the zip files, extracting them, and running the pip install command within package folder also works (see snippet below for example for older version of fedelemflowlist).

```bash
# Download the correct version of the repo
wget https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List/archive/refs/tags/v1.1.2.zip
unzip v1.1.2.zip
cd cd Federal-LCA-Commons-Elementary-Flow-List-1.1.2/
pip install .
```

# Data Store
This package downloads a significant amount of background and inventory data (>2.5 GB) in order to process electricity baselines.

The data store for ElectricityLCI is built from the USEPA's [esupy](https://github.com/USEPA/esupy) package (i.e., processed_data_mgmt.py), which uses the definition of a user's non-roaming data directory from [appdirs](https://github.com/ActiveState/appdirs).

To see where the data store folder is located on your machine, try running the following in a Python interpreter:

```python
>>> from electricitylci.globals import get_datastore_dir
>>> print(get_datastore_dir())
```

The application folder for this package is 'electricitylci'.

Inventory data is provided by USEPA's Standardized Emission and Waste Inventories ([StEWI](https://github.com/USEPA/standardizedinventories)) package (via stewicombo).
The inventory data associated with StEWI are stored in the application folders, 'stewi' and 'stewicombo'.

Flow mapping is handled using USEPA's Federal Elementary Flow List Python package and is saved in the application folder 'fedelemflowlist'.

The following is an example of the 217 data files downloaded (and summarized) from running the 2020 configuration file.
Note that once downloaded, these files are referenced (and not downloaded again) unless a different year of data is referenced in a configuration file.

    users_data_dir/                <- Folder as defined by appdirs
    ├── electricitylci/            <- Support data (183 MB) and outputs (80 MB)
    │   ├── bulk_data/
    │   │   ├── eia_bulk_demand_2020.json (0.5 MB)
    │   │   ├── eia_bulk_id_2020.json (2.6 MB)
    │   │   └── eia_bulk_netgen_2020.json (0.6 MB)
    │   │
    │   ├── eia860_2016/
    │   │   ├── 1___Utility_Y2016.xlsx (0.4 MB)
    │   │   ├── 2___Plant_Y2016.csv (2.6 MB)
    │   │   ├── 2___Plant_Y2016.xlsx (2.5 MB)
    │   │   ├── 3_1_Generator_Y2016.xlsx (7.8 MB)
    │   │   ├── 3_2_Wind_Y2016.xlsx (0.2 MB)
    │   │   ├── 3_3_Solar_Y2016.xlsx (0.5 MB)
    │   │   ├── 3_4_Energy_Storage_Y2016.xlsx (28 KB)
    │   │   ├── 3_5_Multifuel_Y2016.xlsx (0.7 MB)
    │   │   ├── 4___Owner_Y2016.xlsx (0.4 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2016.xlsx (1.2 MB)
    │   │   ├── 6_2_EnviroEquip_Y2016.xlsx (2.9 MB)
    │   │   ├── Form EIA-860 (2016).pdf (0.8 MB)
    │   │   ├── Form EIA-860 Insturctions (2016).pdf (0.4 MB)
    │   │   └── LayoutY2016.xlsx (0.2 MB)
    │   │
    │   ├── eia860_2019/
    │   │   ├── 1___Utility_Y2019.xlsx (0.4 MB)
    │   │   ├── 2___Plant_Y2019.csv (3.2 MB)
    │   │   ├── 2___Plant_Y2019.xlsx (3.1 MB)
    │   │   ├── 3_1_Generator_Y2019.xlsx (8.7 MB)
    │   │   ├── 3_2_Wind_Y2019.xlsx (0.2 MB)
    │   │   ├── 3_3_Solar_Y2019.xlsx (0.8 MB)
    │   │   ├── 3_4_Energy_Storage_Y2019.xlsx (48 KB)
    │   │   ├── 3_5_Multifuel_Y2019.xlsx (0.7 MB)
    │   │   ├── 4___Owner_Y2019.xlsx (0.4 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2019.xlsx (1.2 MB)
    │   │   ├── 6_2_EnviroEquip_Y2019.xlsx (2.9 MB)
    │   │   ├── EIA-860 Form.xlsx (3.1 MB)
    │   │   ├── EIA-860 instruction.pdf (0.6 MB)
    │   │   └── LayoutY2019.xlsx (0.2 MB)
    │   │
    │   ├── eia860_2020/
    │   │   ├── 1___Utility_Y2020.xlsx (0.4 MB)
    │   │   ├── 2___Plant_Y2020.csv (3.4 MB)
    │   │   ├── 2___Plant_Y2020.xlsx (3.3 MB)
    │   │   ├── 3_1_Generator_Y2020.xlsx (9.0 MB)
    │   │   ├── 3_1_Generator_Y2020_generator_operable.csv (5.2 MB)
    │   │   ├── 3_2_Wind_Y2020.xlsx (0.2 MB)
    │   │   ├── 3_3_Solar_Y2020.xlsx (1.0 MB)
    │   │   ├── 3_4_Energy_Storage_Y2020.xlsx (60 KB)
    │   │   ├── 3_5_Multifuel_Y2020.xlsx (0.7 MB)
    │   │   ├── 4___Owner_Y2020.xlsx (0.4 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2020.xlsx (1.2 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2020_boiler_nox.csv (0.1 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2020_boiler_so2.csv (64 KB)
    │   │   ├── 6_2_EnviroEquip_Y2020.xlsx (2.9 MB)
    │   │   ├── 6_2_EnviroEquip_Y2020_boiler_info.csv (0.6 MB)
    │   │   ├── EIA-860 Form.xlsx (3.1 MB)
    │   │   ├── EIA-860 Instructions.pdf (0.8 MB)
    │   │   └── LayoutY2020.xlsx (0.2 MB)
    │   │
    │   ├── energyfutures/
    │   │   └── electricity-generation-2023.csv (1.0 MB)
    │   │
    │   ├── epacems2020/                     <- 48 lower states + D.C. (0.1 MB)
    │   │   ├── epacems2020al.zip (2 KB)
    │   │   ├── epacems2020ar.zip (2 KB)
    │   │   ├── ...
    │   │   └── epacems2020wy.zip (1 KB)
    │   │
    │   ├── f7a_2020/
    │   │   └── coalpublic2020.xls (0.2 MB)
    │   │
    │   ├── f923_2016/
    │   │   ├── EIA923_Schedule_8_Annual_Environmental_Information_\
    │   │   │     2016_Final_Revision.xlsx (3.1 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx (16 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2016_Final_\
    │   │   │     Revisionpage_1.csv (7.1 MB)
    │   │   └── EIA923_Schedules_6_7_NU_SourceNDisposition_\
    │   │         2016_Final_Revision.xlsx (0.7 MB)
    │   │
    │   ├── f923_2019/
    │   │   ├── EIA923_Schedule_8_Annual_Environmental_Information_\
    │   │   │     2019_Final_Revision.xlsx (3.1 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2019_Final_Revision.xlsx (19 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2019_Final_\
    │   │   │     Revisionpage_1.csv (8.1 MB)
    │   │   └── EIA923_Schedules_6_7_NU_SourceNDisposition_\
    │   │         2019_Final_Revision.xlsx (0.9 MB)
    │   │
    │   ├── f923_2020/
    │   │   ├── EIA923_Schedule_8_Annual_Environmental_Information_\
    │   │   │     2020_Final_Revision.xlsx (3.0 MB)
    │   │   ├── EIA923_Schedule_8_Annual_Environmental_Information_\
    │   │   │     2020_Final_Revision_page_8c.csv (0.6 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2020_Final_Revision.xlsx (18 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2020_Final_Revision_page_1.csv (8.4 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2020_Final_Revision_page_3.csv (3.2 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2020_Final_Revision_page_5_reduced.csv (2.3 MB)
    │   │   └── EIA923_Schedules_6_7_NU_SourceNDisposition_\
    │   │         2020_Final_Revision.xlsx (1.0 MB)
    │   │
    │   ├── FRS_bridges/
    │   │   └── NEI_2020_RCRAInfo_2019_TRI_2020_eGRID_2020.csv (0.3 MB)
    │   │
    │   ├── netl/
    │   │   └── Transportation-Inventories.xlsx (0.5 MB)
    │   │
    │   ├── output/
    │   │   ├── BAA_final_trade_2020.csv (65 KB)
    │   │   ├── elci.log (0 KB)
    │   │   ├── elci.log.1 (67 MB)
    │   │   ├── ELCI_2020_jsonld_20241028_153952.zip (12 MB)
    │   │   └── ferc_final_trade_2020.csv (12 KB)
    │   │
    │   └── t_and_d_2020/                         <- 50 states (4.3 MB)
    │       ├── ak.xlsx (84 KB)
    │       ├── al.xlsx (89 KB)
    │       ├── ...
    │       └── wy.xlsx (83 KB)
    │
    ├── fedelemflowlist/                 <- Flow mapping data (14 MB)
    │   └── FedElemFlowListMaster_v1.2.0_e57a542.parquet (14 MB)
    │
    ├── stewi/                           <- Inventory data / metadata (2.6 GB)
    │   ├── eGrid Data Files/
    │   │   ├── eGRID_2020_v1.1.2_metadata.json (1 KB)
    │   │   └── eGRID2020_Data_v2.xlsx (11.6 MB)
    │   │
    │   ├── facility/
    │   │   ├── eGRID_2020_v1.1.2.parquet (0.7 MB)
    │   │   ├── NEI_2020_v1.1.2.parquet (6.0 MB)
    │   │   ├── RCRAInfo_2019_v1.1.2.parquet (1.3 MB)
    │   │   └── TRI_2020_v1.1.2.parquet (1.8 MB)
    │   │
    │   ├── flow/
    │   │   ├── eGRID_2020_v1.1.2.parquet (4 KB)
    │   │   ├── NEI_2020_v1.1.2.parquet (21 KB)
    │   │   ├── RCRAInfo_2019_v1.1.2.parquet (28 KB)
    │   │   └── TRI_2020_v1.1.2.parquet (20 KB)
    │   │
    │   ├── flowbyfacility/
    │   │   ├── eGRID_2020_v1.1.2.parquet (0.5 MB)
    │   │   ├── NEI_2020_v1.1.2.parquet (31 MB)
    │   │   ├── RCRAInfo_2019_v1.1.2.parquet (2.1 MB)
    │   │   └── TRI_2020_v1.1.2.parquet (1.2 MB)
    │   │
    │   ├── flowbyprocess/
    │   │   └── NEI_2020_v1.1.2.parquet (54 MB)
    │   │
    │   ├── NEI Data Files/
    │   │   ├── sppd_rtr_24240.parquet (385 MB)
    │   │   ├── sppd_rtr_24240_v1.0.5_metadata.json (1 KB)
    │   │   ├── sppd_rtr_24506.parquet (85 MB)
    │   │   ├── sppd_rtr_24506_v1.0.5_metadata.json (1 KB)
    │   │   ├── sppd_rtr_24507.parquet (124 MB)
    │   │   ├── sppd_rtr_24507_v1.0.5_metadata.json (1 KB)
    │   │   ├── sppd_rtr_24592.parquet (12 MB)
    │   │   └── sppd_rtr_24592_v1.0.5_metadata.json (1 KB)
    │   │
    │   ├── RCRAInfo Data Files/
    │   │   ├── RCRAInfo_by_year/
    │   │   │   └── br_reporting_2019.csv (774 MB)
    │   │   ├── BR_REPORTING_2019_0.csv (446 MB)
    │   │   ├── BR_REPORTING_2019_1.csv (445 MB)
    │   │   ├── BR_REPORTING_2019_2.csv (147 MB)
    │   │   └── RCRAInfo_2019_v1.1.2_metadata.json (1 KB)
    │   │
    │   ├── TRI Data Files/
    │   │   ├── TRI_2020_v1.1.2_metadata.json (1 KB)
    │   │   ├── US_1a_2020.csv (63 MB)
    │   │   └── US_3a_2020.csv (64 MB)
    │   │
    │   ├── validation/
    │   │   ├── eGRID_2020.csv (1 KB)
    │   │   ├── eGRID_2020_validationset_metadata.json (1 KB)
    │   │   ├── NEI_2020.csv (1 KB)
    │   │   ├── NEI_2020_validationset_metadata.json (1 KB)
    │   │   ├── RCRAInfo_2019.csv (5 KB)
    │   │   ├── RCRAInfo_2019_validationset_metadata.json (1 KB)
    │   │   ├── TRI_2020.csv (140 KB)
    │   │   └── TRI_2020_validationset_metadata.json (1 KB)
    │   │
    │   ├── eGRID_2020_v1.1.2_metadata.json (1 KB)
    │   ├── NEI_2020_v1.1.2_metadata.json (3 KB)
    │   ├── RCRAInfo_2019_v1.1.2_metadata.json (1 KB)
    │   └── TRI_2020_v1.1.2_metadata.json (1 KB)
    │
    └── stewicombo/      <- Data / metadata generated by stewicombo
        ├── ELCI_2020_v1.1.2.parquet (2.2 MB)
        └── ELCI_2020_v1.1.2_metadata.json (7 KB)
