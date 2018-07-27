import pandas as pd
import os
from electricitylci.globals import fuel_name
from electricitylci.process_dictionary_writer import *
from electricitylci.egrid_template_builder import trade_mix_template_generator

data_dir = os.path.dirname(os.path.realpath(__file__))+"/data/"
os.chdir(data_dir) 



def name_change(name):
    
    for row in fuel_name.itertuples():
         if name == row[1]:
             new_name = row[2]
             
    if name == 'COAL':
        new_name = 'coal'
    return new_name




Canada = pd.read_csv('Trade_regions.csv',index_col=0)

trade_dict = {'':''}

fuelnameslist= list(Canada.columns.get_values())
 
for reg in Canada.itertuples():
    
    region = reg[0]
    exchange_list_creator(region)
    exchange(ref_flow_creator())
    
    
    for fuel in Canada.iteritems():
        
        fuelname = fuel[0]
        fuel_new_name = name_change(fuel[0])
                
        name = 'Electricity from '+fuel_new_name+' in the region of '+region;
        if Canada.loc[region,fuelname]/100 != 0:
          exchange(exchange_table_creation_input_con_mix(Canada.loc[region,fuelname]/100,name,region))
    
    final = process_table_creation_trade_mix()
    del final['']
    print(region+' Trade Mix Process Created')
    trade_dict[region] = final;
    
del trade_dict['']
trade_mix_template_generator(trade_dict)        
    

