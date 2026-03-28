[![DOI - 10.18141/2570075](https://img.shields.io/badge/DOI-10.18141%2F2570075-blue)](https://doi.org/10.18141/2570075)

# Electricity Life Cycle Inventory

A Python package that uses standardized facility releases and generation data to create regionalized life cycle inventory (LCI) models for the generation, mix of generation, mix of consumption, and distribution of electricity to end users for the US, with embedded system processes of upstream fuel production and infrastructure.
Pre-configured model specifications are included (in the modelconfig directory of the package install: electricitylci/modelconfig) or users can specify their own models by creating new configuration YAML files in the modelconfig directory.
The created LCI models are exported for use in standard life cycle assessment software (i.e., in JSON-LD format using the openLCA v2.0 schema).

This code was created as part of a collaboration between US EPA Office of Research and Development (USEPA) and the National Energy Technology Laboratory (NETL) with contributions from the National Renewable Energy Laboratory (NREL) and support from Eastern Research Group (ERG).
More information on this effort can be found in the [Framework for an Open-Source Life Cycle Baseline for Electricity Consumption in the United States](https://www.osti.gov/biblio/1576767).

## Disclaimer

    This United States Department of Energy (DOE) and National Energy
    Technology Laboratory (NETL) GitHub project code is provided on an "as is"
    basis and the user assumes responsibility for its use. DOE and NETL have
    relinquished control of the information and no longer have responsibility to
    protect the integrity, confidentiality, or availability of the information.
    Any reference to specific commercial products, processes, or services by
    service mark, trademark, manufacturer, or otherwise, does not constitute or
    imply their endorsement, recommendation or favoring by DOE or NETL.

# Setup
A Python environment (recommended v3.12) is required with the following packages installed, which were recorded in March 2026.
Dependency versions change.
Note asterisks beside versions of esupy, fedelemflowlist, and StEWI that were used in the latest model development.
_Note that Python 3.14 is not supported (yet)._

+ `pip install pandas==2.2.3`
+ `pip install git+https://github.com/FLCAC-admin/fedelemflowlist`
    * Successfully installs:
        + appdirs-1.4.4
        + boto3-1.42.37
        + botocore-1.42.37
        + certifi-2026.1.4
        + charset-normalizer-3.4.4
        + esupy-0.4.2 (*)
        + fedelemflowlist-1.3.1 (*)
        + idna-3.11
        + jmespath-1.1.0
        + numpy-2.4.1
        + olca-schema-2.4.0
        + pyarrow-23.0.0
        + python-dateutil-2.9.0
        + PyYAML-6.0.3
        + requests-2.32.5
        + s3transfer-0.16.0
        + six-1.17.0
        + tzdata-2025.3
        + urllib3-2.6.3
+ `pip install git+https://github.com/USEPA/standardizedinventories#egg=StEWI`
    * Successfully installed:
        + StEWI-1.2.1 (*)
        + et-xmlfile-2.0.0
        + openpyxl-3.1.5
        + xlrd-2.0.2
+ `pip install scipy`
    * Successfully installs:
        + scipy-1.17.0
+ `pip install pytz`
    * Successfully installs:
        + pytz-2025.2

# API
In the latest version of ElectricityLCI, there is a dependency on three external datasets that require the use of an application programming interface (API) key.
These keys are free to request and assist their governing agencies with reporting and justifying their data service.

The EPA's Continuous Emissions Monitors (CEMs) background data are provided by the Clean Air Markets API Portal.
Request a free API key (a long string of numbers used to unlock data access) at the following site.
Keep your API key secret, as it links you to the their data.

- https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup

The EIA's bulk U.S. Electric System Operating Data (EBA) is provided by their Open Data API (v2).
Request a free API key by registering at the following address.

- https://www.eia.gov/opendata/.

NETL's upstream inventory data (e.g., coal transportation and natural gas extraction and processing) are provided through public URLs on [EDX](https://edx.netl.doe.gov), found within the [Life Cycle Analysis](https://edx.netl.doe.gov/group/life-cycle-analysis) group.
An automated download of the Excel workbooks will trigger a request for an EDX API key.
API keys require registration.

- https://edx.netl.doe.gov/user/register

Non-government users are required to list a point of contact for registration.
Please raise as an issue on this GitHub repository to request this additional information.

Be careful when the ElectricityLCI prompts you for an API key, as there are three you need to keep track of (EDX, EPA and EIA).
For convenience, the API keys may be stored in the configuration YAML files in the `epa_cam_api`, `eia_api`, and `edx_api` parameters.
Make sure you paste the right ones!

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
        * ELCI_1 (_Fed Commons published version_)
        * ELCI_2
        * ELCI_3
    - Version 2 baselines include:
        * ELCI_2020
        * ELCI_2021
        * ELCI_2022
    - Version 2.1 baselines include:
        * ELCI_2023
        * ELCI_2024
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
    - Cleans the JSON-LD files (e.g., removing zero-value product flows, removing untracked flows, correcting flow categories, and creating consecutive internal exchange IDs)
    - Generates residual electricity mix processes (based on public sales data provided by O'Shaughnessy et al., 2025).
    - Builds the product systems for balancing authority areas, FERC regions, and US.

# Known Issues
See Appendix A in [this discussion](https://github.com/NETL-RIC/ElectricityLCI/discussions/288) for an overview of unresolved issues in version 2.

# Troubleshooting
If you receive a TypeError in `write_jsonld`, got unexpected keyword argument 'zw', then it's likely you have an outdated version of [fedelemflowlist](https://github.com/FLCAC-admin/fedelemflowlist).

Please ensure these additional USEPA packages are up-to-date:

- [esupy](https://github.com/USEPA/esupy)
- [stewi](https://github.com/USEPA/standardizedinventories)

If GitHub-hosted packages fail to clone and install, manually downloading the zip files, extracting them, and running the `pip install .` command within package folder also works (see snippet below for example for older version of fedelemflowlist).

```bash
# Download the correct version of the repo
wget https://github.com/FLCAC-admin/fedelemflowlist/archive/refs/tags/v1.1.2.zip
unzip v1.1.2.zip
cd cd fedelemflowlist-1.1.2/
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

To see what files you have in your data store, you can call the following function and loop through its dictionaries' lists.

```python
>>> from electricitylci.utils import _build_data_store
>>> ds = _build_data_store()
>>> for file_name in ds['electricitylci']['files']:
...     print(file_name)
```

The application folder for this package is 'electricitylci'.

Inventory data is provided by USEPA's Standardized Emission and Waste Inventories ([StEWI](https://github.com/USEPA/standardizedinventories)) package (via stewicombo).
The inventory data associated with StEWI are stored in the application folders, 'stewi' and 'stewicombo'.

Flow mapping is handled using Federal LCA Commons's Federal Elementary Flow List Python package and is saved in the application folder 'fedelemflowlist'.

The following is an example of the data files downloaded from running the 2023 configuration file (updated in March 2026).
EIA Form 860 Excel workbooks have worksheets that are summarized into CSV files for speed.
Note that once downloaded, these files are referenced (and not downloaded again) unless a different year of data is referenced in a configuration file.

    users_data_dir/                <- Folder as defined by appdirs
    ├── electricitylci/            <- Support data (183 MB) and outputs (80 MB)
    │   ├── bulk_data/
    │   │   ├── eia_bulk_demand_2023.json (0.5 MB)
    │   │   ├── eia_bulk_id_2023.json (2.6 MB)
    │   │   └── eia_bulk_netgen_2023.json (0.6 MB)
    │   │
    │   ├── cer_rer/
    │   │   └── electricity-trade-summary-resume-echanges-commerciaux-electricite.xlsx (100 KB)
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
    │   ├── eia860_2020/
    │   │   ├── 1___Utility_Y2020.xlsx (0.4 MB)
    │   │   ├── 2___Plant_Y2020.csv (3.4 MB)
    │   │   ├── 2___Plant_Y2020.xlsx (3.3 MB)
    │   │   ├── 3_1_Generator_Y2020.xlsx (9.0 MB)
    │   │   ├── 3_2_Wind_Y2020.xlsx (0.2 MB)
    │   │   ├── 3_3_Solar_Y2020.xlsx (1.0 MB)
    │   │   ├── 3_4_Energy_Storage_Y2020.xlsx (60 KB)
    │   │   ├── 3_5_Multifuel_Y2020.xlsx (0.7 MB)
    │   │   ├── 4___Owner_Y2020.xlsx (0.4 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2020.xlsx (1.2 MB)
    │   │   ├── 6_2_EnviroEquip_Y2020.xlsx (2.9 MB)
    │   │   ├── EIA-860 Form.xlsx (3.1 MB)
    │   │   ├── EIA-860 Instructions.pdf (0.8 MB)
    │   │   └── LayoutY2020.xlsx (0.2 MB)
    │   │
    │   ├── eia860_2023/
    │   │   ├── 1___Utility_Y2023.xlsx (0.5 MB)
    │   │   ├── 2___Plant_Y2023.csv (4.0 MB)
    │   │   ├── 2___Plant_Y2023.xlsx (3.9 MB)
    │   │   ├── 3_1_Generator_Y2023.xlsx (10.0 MB)
    │   │   ├── 3_1_Generator_Y2023_generator_operable.csv (5.8 MB)
    │   │   ├── 3_2_Wind_Y2023.xlsx (0.2 MB)
    │   │   ├── 3_3_Solar_Y2023.xlsx (1.4 MB)
    │   │   ├── 3_4_Energy_Storage_Y2023.xlsx (0.3 KB)
    │   │   ├── 3_5_Multifuel_Y2023.xlsx (0.7 MB)
    │   │   ├── 4___Owner_Y2023.xlsx (0.5 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2023.xlsx (1.2 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2023_boiler_nox.csv (0.1 MB)
    │   │   ├── 6_1_EnviroAssoc_Y2023_boiler_so2.csv (0.1 MB)
    │   │   ├── 6_2_EnviroEquip_Y2023.xlsx (2.7 MB)
    │   │   ├── 6_2_EnviroEquip_Y2023_boiler_info.csv (0.6 MB)
    │   │   ├── EIA-860 Form.xlsx (0.4 MB)
    │   │   ├── EIA-860 Instructions.pdf (0.7 MB)
    │   │   └── LayoutY2023.xlsx (0.1 MB)
    │   │
    │   ├── eia930/
    │   │   └── EIA930_Reference_Tables.xlsx (43 KB)
    │   │
    │   ├── energyfutures/
    │   │   └── electricity-generation-2023.csv (1.0 MB)
    │   │
    │   ├── epacems2023/                     <- 48 lower states + D.C. (0.1 MB)
    │   │   ├── epacems2023al.zip (2 KB)
    │   │   ├── epacems2023ar.zip (2 KB)
    │   │   ├── ...
    │   │   └── epacems2023wy.zip (1 KB)
    │   │
    │   ├── f7a_2023/
    │   │   └── coalpublic2023.xls (0.1 MB)
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
    │   ├── f923_2020/
    │   │   ├── EIA923_Schedule_8_Annual_Environmental_Information_\
    │   │   │     2020_Final_Revision.xlsx (3.0 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2020_Final_Revision.xlsx (18 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2020_Final_Revisionpage_1.csv (2.0 MB)
    │   │   └── EIA923_Schedules_6_7_NU_SourceNDisposition_\
    │   │         2020_Final_Revision.xlsx (1.0 MB)
    │   │
    │   ├── f923_2023/
    │   │   ├── EIA923_Schedule_8_Annual_Envir_Info_2023_Final.xlsx (3.0 MB)
    │   │   ├── EIA923_Schedule_8_Annual_Envir_Info_2023_Final_page_8c.csv (0.5 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_2023_Final_Revision.xlsx (19 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2023_Final_Revision_page_1.csv (9.2 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2023_Final_Revision_page_3.csv (3.1 MB)
    │   │   ├── EIA923_Schedules_2_3_4_5_M_12_\
    │   │   │     2023_Final_Revision_page_5_reduced.csv (2.4 MB)
    │   │   └── EIA923_Schedules_6_7_NU_SourceNDisposition_\
    │   │         2023_Final_Revision.xlsx (1.2 MB)
    │   │
    │   ├── fedcommons/
    │   │   ├── dq_sources.json (0.5 KB)
    │   │   ├── dq_systems.json (6 KB)
    │   │   ├── flow_properties.json (10 KB)
    │   │   ├── locations.json (99 KB)
    │   │   └── unit_groups.json (33 KB)
    │   │
    │   ├── FRS_bridges/
    │   │   └── NEI_2020_RCRAInfo_2023_TRI_2023_eGRID_2023.csv (0.3 MB)
    │   │
    │   ├── netl/
    │   │   ├──Transportation_Inventories_02262025.xlsx (0.5 MB)
    │   │   └── 2020_ng/
    │   │       ├── ng_lci_2020rev1.csv (43 KB)
    │   │       └── 2020_ng_model/
    │   │           ├── Appendix_F_2020_Full_Inventory_Results_Midwest_\
    │   │           │   ProdThruTrans.xlsx (14 MB)
    │   │           ├── Appendix_F_2020_Full_Inventory_Results_Northeast_\
    │   │           │   ProdThruTrans.xlsx (11 MB)
    │   │           ├── Appendix_F_2020_Full_Inventory_Results_Pacific_\
    │   │           │   ProdThruTrans.xlsx (3 MB)
    │   │           ├── Appendix_F_2020_Full_Inventory_Results_Rocky_Mountain_\
    │   │           │   ProdThruTrans.xlsx (3 MB)
    │   │           ├── Appendix_F_2020_Full_Inventory_Results_Southeast_\
    │   │           │   ProdThruTrans.xlsx (13 MB)
    │   │           └── Appendix_F_2020_Full_Inventory_Results_Southwest_\
    │   │               ProdThruTrans.xlsx (13 MB)
    │   │
    │   ├── output/                              <- ELCI model results
    │   │   ├── BAA_final_trade_2023.csv (67 KB)
    │   │   ├── elci.log (0 KB)
    │   │   ├── elci.log.1 (106 MB)
    │   │   ├── ELCI_2023_jsonld_20260326_140109.zip (22 MB)
    │   │   └── ferc_final_trade_2023.csv (17 KB)
    │   │
    │   └── t_and_d_2023/                         <- 50 states (4.3 MB)
    │       ├── ak.xlsx (84 KB)
    │       ├── al.xlsx (89 KB)
    │       ├── ...
    │       └── wy.xlsx (83 KB)
    │
    ├── facilitymatcher/                 <- Facility mapping data (1.1 GB)
    │   └── FRS Data Files/
    │       ├── NATIONAL_ENVIRONMENTAL_INTEREST_FILE_v1.2.1_metadata.json
    │       └── NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV
    │
    ├── fedelemflowlist/                 <- Flow mapping data (14 MB)
    │   └── FedElemFlowListMaster_v1.3.0_a79846d.parquet (14 MB)
    │
    ├── stewi/                           <- Inventory data / metadata
    │   ├── eGRID_2023_v1.2.1_3687292_metadata.json (1 KB)
    │   └── facility/
    │       └── eGRID_2023_v1.2.1_3687292.parquet (0.7 MB)
    │
    └── stewicombo/      <- Data / metadata generated by stewicombo
        ├── ELCI_2023_v1.2.1_3687292.parquet (1.9 MB)
        └── ELCI_2023_v1.2.1_3687292_metadata.json (7 KB)

# Developer's Corner

To install the dependencies for this package without installing the package itself, put the following in a text file, called requirements.txt

    fedelemflowlist @ git+https://github.com/FLCAC-Admin/fedelemflowlist
    StEWI @ git+https://github.com/USEPA/standardizedinventories#egg=StEWI
    scipy>=1.10
    pytz

To checkout a pull request locally for testing:

- Open the pull request in GitHub and find the number associated with it (e.g., [PR\#14](https://github.com/KeyLogicLCA/ElectricityLCI/pull/14)); it should be next to the title.
- Create a new branch (e.g., pr14) and pull the repository changes to your local machine (e.g., `git fetch origin pull/14/head:pr14`)
- Switch to new branch (e.g., `git checkout pr14` or `git switch pr14`)

To see all branches:

- Both local and remote branches are listed using: `git branch -a`

To see changes between two branches:

- See what files are different between your two branches (e.g., `git diff --name-only dev pr14` or simply `git diff --name-only pr14`)
- You can also see who made the changes (e.g. `git log dev..pr14`)
- You can also see differences in a specific file by passing the file path (e.g., `git diff dev pr14 README.md`)

To merge one branch with another:

- With two branches (e.g., 'dev' and 'pr14'), you can merge the changes from one branch into another using the `git merge` command.
- Checkout the branch you want to merge changes in to (e.g., `git checkout dev`)
- Merge all new changes from one branch into the active branch (e.g., `git merge pr14`)
- Or you can merge a single file (e.g., `git checkout pr14 electricitylci/file.py`); note that this copies the file between branches making the local copy look like it does on the other branch.
