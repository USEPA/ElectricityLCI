import pandas as pd
import os
from electricitylci.globals import fuel_name
from electricitylci.process_dictionary_writer import *
from electricitylci.egrid_template_builder import trade_mix_template_generator

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'electricitylci/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'


def name_change(name):
    
    for row in fuel_name.itertuples():
         if name == row[1]:
             new_name = row[2]
             
    if name == 'COAL':
        new_name = 'coal'
    return new_name




Canada = pd.read_csv(data_dir+'Trade_regions.csv',index_col=0)

trade_dict = {'':''}

fuelnameslist= list(Canada.columns.get_values())
 
for reg in Canada.itertuples():
    
    region = reg[0]
    exchanges_list =[]
    exchange(ref_flow_creator(region),exchanges_list)
    
    
    for fuel in Canada.iteritems():
        
        fuelname = fuel[0]
        fuel_new_name = name_change(fuel[0])
                
        name = 'Electricity from '+fuel_new_name+' in the region of '+region;
        if Canada.loc[region,fuelname]/100 != 0:
          exchange(exchange_table_creation_input_con_mix(Canada.loc[region,fuelname]/100,name,region),exchanges_list)
    
    final = process_table_creation_trade_mix(region,exchanges_list)
    del final['']
    print(region+' Trade Mix Process Created')
    trade_dict[region] = final;
    
del trade_dict['']
trade_mix_template_generator(trade_dict)
    

