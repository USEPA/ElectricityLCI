import logging

logger = logging.getLogger("dqi")

# Scoring based on USEPA 2016: Guidance on Data Quality Assessment for Life Cycle Inventory Data

flow_data_quality_fields = ['Reliability_Score', 'TemporalCorrelation', 'GeographicalCorrelation',
                            'TechnologicalCorrelation', 'DataCollection']
temporal_correlation_lower_bound_to_dqi = {3: 1, 6: 2, 10: 3, 15: 4, None: 5}

data_collection_lower_bound_to_dqi = {.4: 4, .6: 3, .8: 2, 1: 1, None: 5}

# This is a varation from USEPA 2016 flow indicators. Instead this is intended to
# represent fraction of generation coming from this intended fuel
technological_correlation_lower_bound_to_dqi = {.4: 4, .6: 3, .8: 2, 1: 1, None: 5}


def lookup_score_with_bound_key(raw_score, bound_to_dqi):
    breakpoints = list(bound_to_dqi.keys())
    if raw_score <= breakpoints[0]:
        score = bound_to_dqi[breakpoints[0]]
    elif (raw_score > breakpoints[0]) & (raw_score <= breakpoints[1]):
        score = bound_to_dqi[breakpoints[1]]
    elif (raw_score > breakpoints[1]) & (raw_score <= breakpoints[2]):
        score = bound_to_dqi[breakpoints[2]]
    elif (raw_score > breakpoints[2]) & (raw_score <= breakpoints[3]):
        score = bound_to_dqi[breakpoints[3]]
    elif (raw_score<0):
        logging.debug('Error: invalid dqi score')
    else:
        score = bound_to_dqi[None]
    return score
