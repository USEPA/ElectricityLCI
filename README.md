# Electricity Life Cycle Inventory

A Python package that uses standardized facility releases and generation data to create regionalized life cycle inventory (LCI) models for the generation, mix of generation, mix of consumption, and distribution of electricity to end users for the US, with embedded system processes of upstream fuel production and infrastructure.
Pre-configured model specifications are included (in the modelconfig directory of the package install: electricitylci/modelconfig) or users can specify their own models by creating new configuration YAML files in the modelconfig directory.
The created LCI models are exported for use in standard life cycle assessment software (i.e., in JSON-LD format using the openLCA v2.0 schema).

This code was created as part of a collaboration between US EPA Office of Research and Development (USEPA) and the National Energy Technology Laboratory (NETL) with contributions from the National Renewable Energy Laboratory (NREL) and support from Eastern Research Group (ERG).
More information on this effort can be found in the [Framework for an Open-Source Life Cycle Baseline for Electricity Consumption in the United States](https://netl.doe.gov/energy-analysis/details?id=4004).

## Disclaimer

    This United States Environmental Protection Agency (EPA) and National Energy
    Technology Laboratory (NETL) GitHub project code is provided on an "as is"
    basis and the user assumes responsibility for its use. EPA and NETL have
    relinquished control of the information and no longer has responsibility to
    protect the integrity, confidentiality, or availability of the information.
    Any reference to specific commercial products, processes, or services by
    service mark, trademark, manufacturer, or otherwise, does not constitute or
    imply their endorsement, recommendation or favoring by EPA or NETL.

# Setup
A Python virtual environment (recommended v3.12) is required with the following packages installed, which were recorded in February 2025.
_Note that Python 3.14 is not supported (yet)._

+ `pip install git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List#egg=fedelemflowlist`
    * Successfully installs:
        + appdirs-1.4.4
        + boto3-1.36.18
        + botocore-1.36.18
        + certifi-2025.1.31
        + charset-normalizer-3.4.1
        + esupy-0.4.0
        + fedelemflowlist-1.3.0
        + idna-3.10
        + jmespath-1.0.1
        + numpy-2.2.2
        + olca-schema-2.4.0
        + pandas-2.2.3
        + pyarrow-19.0.0
        + python-dateutil-2.9.0
        + pytz-2025.1
        + PyYAML-6.0.2
        + requests-2.32.3
        + s3transfer-0.11.2
        + six-1.17.0
        + tzdata-2025.1
        + urllib3-2.3.0
+ `pip install git+https://github.com/USEPA/standardizedinventories#egg=StEWI`
    * Successfully installed:
        + StEWI-1.1.4
        + beautifulsoup4-4.12.3
        + et-xmlfile-2.0.0
        + openpyxl-3.1.5
        + soupsieve-2.5
        + xlrd-2.0.1
+ `pip install scipy`
    * Successfully installs:
        + scipy-1.15.1

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

NETL's coal transportation inventory update is provided through a public URL on [EDX](https://edx.netl.doe.gov), found within the [Life Cycle Analysis](https://edx.netl.doe.gov/group/life-cycle-analysis) group.
An automated download of the Excel workbook will trigger a request for an EDX API key.
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
        * ELCI_1
        * ELCI_2
        * ELCI_3
    - Version 2 baselines include:
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
    - Cleans the JSON-LD files (e.g., removing zero-value product flows, removing untracked flows, correcting flow categories, and creating consecutive internal exchange IDs)
    - Builds the product systems for balancing authority areas, FERC regions, and US.

# Known Issues
See Appendix A in [this discussion](https://github.com/USEPA/ElectricityLCI/discussions/288) for an overview of unresolved issues in version 2.

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

Flow mapping is handled using USEPA's Federal Elementary Flow List Python package and is saved in the application folder 'fedelemflowlist'.

The following is an example of the 181 data files downloaded from running the 2020 configuration file (updated in May 2025).
EIA Form 860 Excel workbooks have worksheets that are summarized into CSV files for speed.
Note that once downloaded, these files are referenced (and not downloaded again) unless a different year of data is referenced in a configuration file.

    users_data_dir/                <- Folder as defined by appdirs
    ├── electricitylci/            <- Support data (183 MB) and outputs (80 MB)
    │   ├── bulk_data/
    │   │   ├── eia_bulk_demand_2020.json (0.5 MB)
    │   │   ├── eia_bulk_id_2020.json (2.6 MB)
    │   │   └── eia_bulk_netgen_2020.json (0.6 MB)
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
    │   ├── eia930/
    │   │   └── EIA930_Reference_Tables.xlsx (43 KB)
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
    │   ├── fedcommons/
    │   │   ├── dq_sources.json (0.5 KB)
    │   │   ├── dq_systems.json (6 KB)
    │   │   ├── flow_properties.json (12 KB)
    │   │   └── unit_groups.json (36 KB)
    │   │
    │   ├── FRS_bridges/
    │   │   └── NEI_2020_RCRAInfo_2019_TRI_2020_eGRID_2020.csv (0.3 MB)
    │   │
    │   ├── netl/
    │   │   └── Transportation_Inventories_02262025.xlsx (0.5 MB)
    │   │
    │   ├── output/                              <- ELCI model results
    │   │   ├── BAA_final_trade_2020.csv (69 KB)
    │   │   ├── elci.log (0 KB)
    │   │   ├── elci.log.1 (112 MB)
    │   │   ├── ELCI_2020_jsonld_20250528_142931.zip (23 MB)
    │   │   └── ferc_final_trade_2020.csv (18 KB)
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
    ├── stewi/                           <- Inventory data / metadata (45 MB)
    │   ├── facility/
    │   │   ├── eGRID_2020_v1.1.3_6710a0f.parquet (0.7 MB)
    │   │   ├── NEI_2020_v1.1.0_084a311.parquet (6.0 MB)
    │   │   ├── RCRAInfo_2019_v1.0.5_f40a6aa.parquet (1.3 MB)
    │   │   └── TRI_2020_v1.1.0_084a311.parquet (1.8 MB)
    │   │
    │   ├── flowbyfacility/
    │   │   ├── eGRID_2020_v1.1.3_6710a0f.parquet (0.5 MB)
    │   │   ├── NEI_2020_v1.1.0_084a311.parquet (31 MB)
    │   │   ├── RCRAInfo_2019_v1.0.5_f40a6aa.parquet (2.1 MB)
    │   │   └── TRI_2020_v1.1.0_084a311.parquet (1.2 MB)
    │   │
    │   ├── eGRID_2020_v1.1.3_6710a0f_metadata.json (1 KB)
    │   ├── NEI_2020_v1.1.0_084a311_metadata.json (3 KB)
    │   ├── RCRAInfo_2019_v1.0.5_f40a6aa_metadata.json (1 KB)
    │   └── TRI_2020_v1.1.0_084a311_metadata.json (1 KB)
    │
    └── stewicombo/      <- Data / metadata generated by stewicombo
        ├── ELCI_2020_v1.1.2.parquet (2.2 MB)
        └── ELCI_2020_v1.1.2_metadata.json (7 KB)

# Developer's Corner

To install the dependencies for this package without installing the package itself, put the following in a text file, called requirements.txt

    fedelemflowlist @ git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List#egg=fedelemflowlist
    StEWI @ git+https://github.com/USEPA/standardizedinventories#egg=StEWI
    scipy>=1.10


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
