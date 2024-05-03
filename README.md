# Electricity Life Cycle Inventory

A python package that uses standardized facility releases and generation data to create regionalized life cycle inventory (LCI) models for the generation, mix of generation, mix of consumption, and distribution of electricity to end users for the US, with embedded system processes of upstream fuel production and infrastructure.
Pre-configured model specifications are included (in electricitylci/modelconfig) or users can specify their own models by creating new config.yml files in the modelconfig directory. The created LCI models can be exported for use in standard life cycle assessment software (e.g., in JSON-LD format using the openLCA v2.0 schema).

See the [wiki](http://github.com/USEPA/ElectricityLCI/wiki) for installation and use instructions, descriptions of files, and a list of contributors.

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
        + esupy-0.3.2
        + fedelemflowlist-1.2.2
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
        + StEWI-1.1.2
        + beautifulsoup4-4.12.3
        + et-xmlfile-1.1.0
        + openpyxl-3.1.2
        + soupsieve-2.5
        + xlrd-2.0.1
+ `pip install scipy`
    * Successfully installs:
        + scipy-1.12.0


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

To run from within a cloned repository:

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
* `Enter EPA API key: ` prompt encountered during 2020–2022 baseline runs.
    - The CEMS data are no longer available via the old FTP.
    - See https://github.com/USEPA/ElectricityLCI/issues/207
    - In the short-term, a free-to-register EPA Data API key is required (https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup)
    - Copy and paste the API key (a long string of numbers and letters) to the prompt and hit Enter to download the missing data.
    - The missing data are archived in state-based zip files the epacems2020, epacems2021, and epacems2022 folders found in the `paths.local_path` location (as defined in electricitylci.globals.py).
* `ValueError` encountered during 2021 and 2022 baseline runs.
    - The 'coalpublic2021.xls' and 'coalpublic2022.xls' files downloaded to your f7a_2021 and f7a_2022 folders (in the `paths.local_path` location found in electricitylci.globals.py) are actually XML spreadsheets.
    - The simple fix is to open these files in Microsoft Excel and "Save as" Microsoft Excel 95 Workbook, replacing the previous file with the new (do not leave both copies in the folder!)
* The data quality indicators used for flows and processes (dqi.py) may deviate from the standards:
    - [Flow Pedigree](https://www.lcacommons.gov/lca-collaboration/National_Renewable_Energy_Laboratory/USLCI_Database_Public/dataset/DQ_SYSTEM/d13b2bc4-5e84-4cc8-a6be-9101ebb252ff)
    - [Process Pedigree](https://www.lcacommons.gov/lca-collaboration/National_Renewable_Energy_Laboratory/USLCI_Database_Public/dataset/DQ_SYSTEM/70bf370f-9912-4ec1-baa3-fbd4eaf85a10)
* The mapping of Balancing Authorities to FERC regions may need updating as BAs are introduced to the grid.
    - See GitHub [Issue #215](https://github.com/USEPA/ElectricityLCI/issues/215)

# Troubleshooting
If GitHub-hosted packages fail to clone and install, manually downloading the zip files, extracting them, and running the pip install command within package folder also works (see snippet below for example for older version of fedelemflowlist).

```bash
# Download the correct version of the repo
wget https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List/archive/refs/tags/v1.1.2.zip
unzip v1.1.2.zip
cd cd Federal-LCA-Commons-Elementary-Flow-List-1.1.2/
pip install .
```

# Data
This package downloads a significant amount of background and inventory data in order to process electricity baselines.

To see where the data are saved on your machine, try running the following:

```python
>>> from electricitylci.globals import output_dir
>>> print(output_dir)
```
