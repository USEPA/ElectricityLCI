# Electricity Life Cycle Inventory

A python package that uses standardized facility release and generation data to create regionalized life cycle inventory (LCI) models for the generation,
 mix of generation, mix of consumption, and distribution of electricity to end users for the US, with embedded system processes of upstream fuel production and infrastructure. Pre-configured model specifications are included or users can specify their own models. The created LCI models can be exported
 for use in standard life cycle assessment software.

See the [wiki](http://github.com/USEPA/ElectricityLCI/wiki) for installation and use instructions, descriptions of files, and a list of contributors.

This code was created as part of a collaboration between US EPA Office of Research and Development (USEPA) and the National Energy Technology Laboratory (NETL) with contributions from the National Renewable Energy Laboratory (NREL) and support from Eastern Research Group (ERG). More information on this effort can be found in the [Framework for an Open-Source Life Cycle Baseline for Electricity Consumption in the United States](https://netl.doe.gov/energy-analysis/details?id=4004).

## Disclaimer

This United States Environmental Protection Agency (EPA) and National Energy Technology Laboratory (NETL) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use. EPA and NETL have relinquished control of the information and no longer
has responsibility to protect the integrity, confidentiality, or availability of the information.
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA or NETL.

# Setup
A Python virtual environment (v 3.11 or higher) is required with the following packages installed, which are the latest as of writing (summer 2023):

+ `pip install git+https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List@v1.1.1#egg=fedelemflowlist`
    * Successfully installs:
        + appdirs-1.4.4
        + boto3-1.28.12
        + botocore-1.31.12
        + certifi-2023.7.22
        + charset-normalizer-3.2.0
        + esupy-0.2.2
        + fedelemflowlist-1.1.1
        + idna-3.4
        + jmespath-1.0.1
        + numpy-1.25.1
        + olca-ipc-2.0.0
        + olca-schema-0.0.11
        + pandas-2.0.3
        + pyarrow-12.0.1
        + python-dateutil-2.8.2
        + pytz-2023.3
        + pyyaml-6.0.1
        + requests-2.31.0
        + requests_ftp-0.3.1
        + s3transfer-0.6.1
        + six-1.16.0
        + tzdata-2023.3
        + urllib3-1.26.16
+ `pip install git+https://github.com/USEPA/standardizedinventories@v1.1.1#egg=St
EWI`
    * Successfully installed:
        + StEWI-1.1.1
        + beautifulsoup4-4.12.2
        + esupy-0.3.0
            - Uninstalls esupy-0.2.2
            - Note: there is no version requirement in StEWI for this package
        + et-xmlfile-1.1.0
        + openpyxl-3.1.2
        + soupsieve-2.4.1
        + xlrd-2.0.1
+ `pip install sympy`
    * Successfully installs:
        + mpmath-1.3.0
        + sympy-1.12
+ `pip install scipy`
    * Successfully installs:
        + scipy-1.11.1
