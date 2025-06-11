
# Data Availability

Data and vintages handled by third-party data providers:

- USEPA's [StEWI](https://github.com/USEPA/standardizedinventories#usepa-inventories-covered-by-data-reporting-year-current-version)
    - eGRID
    - National Emissions Inventory (NEI)
    - Resource Conservation and Recovery Act Information (RCRAInfo)
    - Toxic Release Inventory (TRI)
- Energy Information Administration
    - EIA Form 860 ([.html](https://www.eia.gov/electricity/data/eia860/))
    - EIA Form 923 ([.html](https://www.eia.gov/electricity/data/eia923/))
    - EIA 7a ([.html](https://www.eia.gov/coal/data.php); see 'Production' Excel workbooks)
    - US Electric System Operating Data (EBA); used for consumption mixes
        * Bulk data ([.html](https://www.eia.gov/opendata/v1/bulkfiles.php))
        * API; requires a free OpenData API key ([.html](https://www.eia.gov/opendata/))
    - Transmission and distribution losses, based on state electricity profiles ([.html](https://www.eia.gov/electricity/state/)); see [here](https://www.eia.gov/tools/faqs/faq.php?id=105&t=3) for methods
- Environmental Protection Agency (EPA)
    - Continuous Emission Monitoring System (CEMS); requires a free Clean Air Markets API key ([.html](https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup))
        - The same data are also available through EPA's Clean Air Markets Program Data (CAMPD) custom data download tool ([.html](https://campd.epa.gov/data/custom-data-download))
- [Canadian Energy Regulator](https://www.cer-rec.gc.ca/en/about/index.html)
    - Electricity trade summary, 2010&ndash;2024 ([.html](https://www.cer-rec.gc.ca/en/data-analysis/energy-commodities/electricity/statistics/electricity-trade-summary/electricity-trade-summary-resume-echanges-commerciaux-electricite.xlsx))
    - Electricity generation mix (2023), from EnergyFutures ([.html](https://open.canada.ca/data/en/dataset/7643c948-d661-4d90-ab91-e9ac732fc737))

The following is a summary of input data and years available for use in building electricity models.

| Dataset | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 |
|---|---|---|---|---|---|---|---|---|
| Coal mining and transport LCI | | x | | | | | | |
| Geothermal LCI<sup>1</sup> | | x | | | | | | |
| Hydro LCI<sup>1</sup> | | x | | | | | | |
| Natural gas LCI | | x | | | | | | |
| Nuclear LCI<sup>1</sup> | | x | | | | | | |
| Solar PV LCI<sup>2</sup> | | x | | | | x | | |
| Solar thermal LCI<sup>2</sup> | | x | | | | x | | |
| Wind farm LCI<sup>2</sup> | | x | | | | x | | |

Notes:

<sup>1</sup> The inventories for these technologies are based on data from 2016; however, so long as the same plants exist in other years in the EIA 923 data, that inventory can be re-used and applied to different years.

<sup>2</sup> The inventories for these technologies are based on data from 2016 and 2020 (as defined in the RENEWABLE_VINTAGE parameter in globals.py).
