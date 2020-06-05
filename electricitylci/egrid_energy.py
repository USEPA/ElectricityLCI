import numpy as np
import pandas as pd
from os.path import join
from electricitylci.egrid_flowbyfacilty import egrid_flowbyfacility
from electricitylci.globals import data_dir
from electricitylci.model_config import egrid_year

# Filter warnings to remove warning about setting value on a slide of a df
import warnings
warnings.filterwarnings("ignore")

# Get flow by facility data for egrid
egrid_net_generation = egrid_flowbyfacility[egrid_flowbyfacility['FlowName'] == 'Electricity']
# Convert flow amount to MWh
egrid_net_generation.loc[:, 'Electricity'] = egrid_net_generation['FlowAmount']*0.00027778
# drop unneeded columns
egrid_net_generation = egrid_net_generation.drop(columns=['ReliabilityScore', 'FlowName', 'FlowAmount', 'Compartment', 'Unit'])
# Now just has 'FacilityID' and 'Electricity' in MWh

# Get length
len(egrid_net_generation)
# 2016:7715


# Returns list of egrid ids with positive_generation
def list_egrid_facilities_with_positive_generation():
    egrid_net_generation_above_min = egrid_net_generation[egrid_net_generation['Electricity'] > 0]
    return list(egrid_net_generation_above_min['FacilityID'])


egrid_efficiency = egrid_flowbyfacility[egrid_flowbyfacility['FlowName'].isin(['Electricity', 'Heat'])]
egrid_efficiency = egrid_efficiency.pivot(index = 'FacilityID', columns = 'FlowName', values = 'FlowAmount').reset_index()
egrid_efficiency.sort_values(by='FacilityID', inplace=True)
egrid_efficiency['Efficiency'] = egrid_efficiency['Electricity']*100/egrid_efficiency['Heat']
egrid_efficiency = egrid_efficiency.replace([np.inf, -np.inf], np.nan)
egrid_efficiency.dropna(inplace=True)
egrid_efficiency.head(50)
len(egrid_efficiency)


def list_egrid_facilities_in_efficiency_range(min_efficiency, max_efficiency):
    egrid_efficiency_pass = egrid_efficiency[(egrid_efficiency['Efficiency'] >= min_efficiency) & (egrid_efficiency['Efficiency'] <= max_efficiency)]
    return list(egrid_efficiency_pass['FacilityID'])


# Get egrid generation reference data by subregion from the egrid data files ..used for validation
# import reference data
path = join(data_dir,
            'egrid_subregion_generation_by_fuelcategory_reference_{}.csv'.format(egrid_year))
ref_egrid_subregion_generation_by_fuelcategory = pd.read_csv(path)
# ref_egrid_subregion_generation_by_fuelcategory = pd.read_csv(data_dir+'egrid_subregion_generation_by_fuelcategory_reference_' + str(egrid_year) + '.csv')

ref_egrid_subregion_generation_by_fuelcategory = ref_egrid_subregion_generation_by_fuelcategory.rename(columns={'Electricity': 'Ref_Electricity_Subregion_FuelCategory'})
