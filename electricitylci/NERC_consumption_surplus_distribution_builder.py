import openpyxl
import os

from electricitylci.egrid_facilities import egrid_subregions
from electricitylci.fedlcacommons_template_builder import distribution_template_generator
from electricitylci.fedlcacommons_template_builder import consumption_mix_template_generator
from electricitylci.fedlcacommons_template_builder import surplus_pool_mix_template_generator
from electricitylci.globals import data_dir,net_trading,efficiency_of_distribution_grid

wb2 = openpyxl.load_workbook(data_dir+'eGRID_Consumption_Mix_new.xlsx',data_only=True)
data = wb2['ConsumptionMixContributions']

if net_trading == True:
    nerc_region = data['A4:A29']
    surplus_pool_trade_in = data['F4':'F29']
    trade_matrix = data['I3':'AP13']
    generation_quantity = data['E4':'E29'] 
    nerc_region2 = data['H4:H13']         
    egrid_regions = data['C4:C29']
    
else:
    nerc_region = data['A36:A61']
    surplus_pool_trade_in = data['F36':'F61']
    trade_matrix = data['I35':'AP45']
    generation_quantity = data['E36':'E61'] 
    nerc_region2 = data['H36:H45']         
    egrid_regions = data['C36:C61']





def surplus_pool_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,gen_quantity, eGRID_region,nerc_region2):
     
     surplus_dict = {'':''}
     for i in range(0,len(nerc_region2)):
         
           region = nerc_region2[i][0].value
           
           exchanges_list = []
           
           exchange(ref_flow_creator('US-NERC-'+region),exchanges_list)
           y  = len(trade_matrix[0])
           
           chk=0;
           for j in range(0,8):
               
               if trade_matrix[i+1][j].value != None and trade_matrix[i+1][j].value != 0:
                   
                   name = 'Electricity; at region '+trade_matrix[0][j].value+'; Trade Mix';
                   exchange(exchange_table_creation_input_con_mix(trade_matrix[i+1][j].value,name,'CA-'+trade_matrix[0][j].value),exchanges_list)
                   chk = 1;
           
           for j in range(8,34):
               
               if trade_matrix[i+1][j].value != None and trade_matrix[i+1][j].value != 0:
                   name = 'Electricity from generation mix '+trade_matrix[0][j].value;
                   exchange(exchange_table_creation_input_con_mix(trade_matrix[i+1][j].value,name,'US-eGRID-'+trade_matrix[0][j].value),exchanges_list)
                   chk = 1;
        
           final = process_table_creation_surplus(region,exchanges_list)
           del final['']
           print(region+' NERC Surplus Process Created')
           surplus_dict[region] = final;
           #del consumption_dict['']
     return surplus_dict







def consumption_mix_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,gen_quantity, eGRID_region,nerc_region2):

   
   surplus_dict = surplus_pool_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,gen_quantity, eGRID_region,nerc_region2)
   global region
   consumption_dict = {'':''}
   for reg in range(0,len(eGRID_region)):
           region = eGRID_region[reg][0].value
           
           exchanges_list = []
           exchange(ref_flow_creator('US-eGRID-'+region),exchanges_list)
           
           
           y  = len(trade_matrix[0])
           chk = 0;
           for nerc in range(0,len(nerc_region2)):
            
             if nerc_region[reg][0].value == nerc_region2[nerc][0].value: 
                 
                 if surplus_pool_trade_in[reg][0].value != 0:
                   
                   for  j in range(0,y):
                         
                        name = surplus_dict[nerc_region[reg][0].value]['name']
                        
                        if trade_matrix[nerc+1][j].value != None and trade_matrix[nerc+1][j].value !=0:
                            
                            exchange(exchange_table_creation_input_con_mix(surplus_pool_trade_in[reg][0].value,name,'US-NERC-'+nerc_region[reg][0].value),exchanges_list)
                            chk=1;
                            break;
           name = 'Electricity from generation mix '+eGRID_region[reg][0].value   
           if chk == 1:
               exchange(exchange_table_creation_input_con_mix(gen_quantity[reg][0].value,name,'US-eGRID-'+region),exchanges_list)
           else:
               exchange(exchange_table_creation_input_con_mix(1,name,'US-eGRID-'+region),exchanges_list)
           
           final = process_table_creation_con_mix(region,exchanges_list)
           del final['']
           print(region+' Consumption Mix Process Created')
           consumption_dict[eGRID_region[reg][0].value] = final;
           #del consumption_dict['']
   return consumption_dict













def distribution_mix_dictionary(eGRID_subregions,efficiency_of_distribution):

    distribution_dict = {'':''}
    for reg in eGRID_subregions:
            global region;
            region = reg
            exchanges_list =[]

            exchange(ref_flow_creator('US-eGRID-'+region),exchanges_list)
            name =  consumption_dict[reg]['name']
            exchange(exchange_table_creation_input_con_mix(1/efficiency_of_distribution,name,'US-eGRID-'+region),exchanges_list)

            final = process_table_creation_distribution(region,exchanges_list)

            del final['']
            print(region+' Distribution Process Created')

            distribution_dict[region] = final;
           #del consumption_dict['']
    return distribution_dict




















#Creating Surplus Pool dictionary
surplus_dict = surplus_pool_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)
del surplus_dict['']




#Creating Consumption dictionary
consumption_dict = consumption_mix_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)
del consumption_dict['']



#The distribution mix dictionary does not require any new information and it will be fine to use the GEN mix dictionary to write these templates.
#Only extra infomration required is the efficiency
distribution_dict = distribution_mix_dictionary(egrid_subregions,efficiency_of_distribution_grid)
del distribution_dict['']



consumption_mix_template_generator(consumption_dict)
surplus_pool_mix_template_generator(surplus_dict,nerc_region2)
distribution_template_generator(distribution_dict,efficiency_of_distribution_grid)





