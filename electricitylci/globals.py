import os
import pandas as pd
def set_dir(directory):
    if not os.path.exists(directory): os.makedirs(directory)
    return directory

datadir = 'electricitylci/data/'
outputdir = set_dir('output/')

#Declare target year for LCI (recommend current year by default) and year of eGRID data to use
electricity_lci_target_year = 2018
egrid_year = 2016

#emissions data to include in LCI
inventories_of_interest = {'eGRID':'2016','TRI':'2016','NEI':'2016','RCRAInfo':'2015'}
inventories = inventories_of_interest.keys()

#Criteria for filtering eGRID data to include
include_only_egrid_facilities_with_positive_generation = True

egrid_facility_efficiency_filters = {'lower_efficiency':10,
                                     'upper_efficiency':100}

min_plant_percent_generation_from_primary_fuel_category = 90

efficiency_of_distribution_grid = 0.95

##Data quality

def map_inventory_flows_to_FedCommons_elementary_flows():
    print('Map flows')

data_dir = os.path.dirname(os.path.realpath(__file__))+"\\data\\"
os.chdir(data_dir)  

#Reading the fuel name file
fuel_name = pd.read_csv('fuelname.csv')


#Trading methodology
net_trading = True