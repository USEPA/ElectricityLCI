temporal_correlation_lower_bound_to_dqi = {3:1,6:2,10:3,15:4,None:5}


def lookup_temporal_score(x):
    breakpoints = list(temporal_correlation_lower_bound_to_dqi.keys())
    if x <= breakpoints[0]:
        score = temporal_correlation_lower_bound_to_dqi[breakpoints[0]]
    elif (x > breakpoints[0]) & (x <= breakpoints[1]):
        score = temporal_correlation_lower_bound_to_dqi[breakpoints[1]]
    elif (x > breakpoints[1]) & (x <= breakpoints[2]):
        score = temporal_correlation_lower_bound_to_dqi[breakpoints[2]]
    elif (x > breakpoints[2]) & (x <= breakpoints[3]):
        score = temporal_correlation_lower_bound_to_dqi[breakpoints[3]]
    else:
        score = temporal_correlation_lower_bound_to_dqi[None]
    return score
