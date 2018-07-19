import openpyxl
import os

from electricitylci.egrid_facilities import egrid_subregions
from electricitylci.process_dictionary_writer import consumption_mix_dictionary
from electricitylci.process_dictionary_writer import surplus_pool_dictionary
from electricitylci.process_dictionary_writer import distribution_mix_dictionary
from electricitylci.globals import efficiency_of_distribution_grid
from electricitylci.egrid_template_builder import distribution_template_generator
from electricitylci.egrid_template_builder import consumption_mix_template_generator
from electricitylci.egrid_template_builder import surplus_pool_mix_template_generator

data_dir = os.path.dirname(os.path.realpath(__file__))+"/data/"
os.chdir(data_dir)  
wb2 = openpyxl.load_workbook('eGRID_Consumption_Mix_new.xlsx',data_only=True)
data = wb2['ConsumptionMixContributions']

nerc_region = data['A4:A29']
surplus_pool_trade_in = data['F4':'F29']
trade_matrix = data['I3':'AP13']
generation_quantity = data['E4':'E29'] 
nerc_region2 = data['H4:H13']         
egrid_regions = data['C4:C29']


#Creating Consumption dictionary
consumption_dict = consumption_mix_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)
del consumption_dict['']

#Creating Surplus Pool dictionary
surplus_dict = surplus_pool_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)
del surplus_dict['']


#The distribution mix dictionary does not require any new information and it will be fine to use the GEN mix dictionary to write these templates.
#Only extra infomration required is the efficiency
distribution_dict = distribution_mix_dictionary(egrid_subregions,efficiency_of_distribution_grid)
del distribution_dict['']


distribution_template_generator(distribution_dict,efficiency_of_distribution_grid)

consumption_mix_template_generator(consumption_dict)

surplus_pool_mix_template_generator(surplus_dict,nerc_region2)