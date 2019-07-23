# -*- coding: utf-8 -*-



def subregion_col(subregion="BA"):
    if subregion == "all":
        region_agg = ["EIA_region"]
    elif subregion == "NERC":
        region_agg = ["NERC"]
    elif subregion == "BA":
        region_agg = ["Balancing Authority Name"]
    elif subregion == "US":
        region_agg = None
    elif subregion == "FERC":
        region_agg = ["FERC_Region"]
    elif subregion == "EIA":
        region_agg = ["EIA_Region"]
    return region_agg