import openpyxl

#from electricitylci.egrid_facilities import egrid_subregions
from electricitylci.globals import data_dir,net_trading
from electricitylci.process_dictionary_writer import exchange,exchange_table_creation_input_con_mix,ref_exchange_creator,process_table_creation_con_mix,process_table_creation_surplus

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
     
     surplus_dict = dict()
     for i in range(0,len(nerc_region2)):
         
           region = nerc_region2[i][0].value
           exchanges_list = []
           
           exchange(ref_exchange_creator(), exchanges_list)
           #y  = len(trade_matrix[0])
           
           #chk=0;
           for j in range(0,34):
               input_region_surplus_amount = trade_matrix[i + 1][j].value
               if input_region_surplus_amount != None and input_region_surplus_amount != 0:
                   #name = 'Electricity; at region '+trade_matrix[0][j].value+'; Trade Mix'
                   input_region_acronym = trade_matrix[0][j].value
                   exchange(exchange_table_creation_input_con_mix(input_region_surplus_amount,input_region_acronym),exchanges_list)
                   #exchange(exchange_table_creation_input_con_mix(trade_matrix[i+1][j].value,trade_matrix[0][j].value),exchanges_list)
                   #chk = 1;

           final = process_table_creation_surplus(region,exchanges_list)
           print(region+' NERC Surplus Process Created')
           surplus_dict['SurplusPool'+region] = final;
     return surplus_dict


def consumption_mix_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity, egrid_regions,nerc_region2):

   surplus_dict = surplus_pool_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)
   #global region
   consumption_dict = dict()
   for reg in range(0,len(egrid_regions)):
           region = egrid_regions[reg][0].value
           
           exchanges_list = []
           exchange(ref_exchange_creator(), exchanges_list)
           
           
           y  = len(trade_matrix[0])
           chk = 0;
           for nerc in range(0,len(nerc_region2)):
            
             if nerc_region[reg][0].value == nerc_region2[nerc][0].value: 
                 
                 if surplus_pool_trade_in[reg][0].value != 0:
                   
                   for  j in range(0,y):
                         
                        #name = surplus_dict[nerc_region[reg][0].value]['name']

                        if trade_matrix[nerc+1][j].value != None and trade_matrix[nerc+1][j].value !=0:
                            print(nerc_region[reg][0].value)
                            exchange(exchange_table_creation_input_con_mix(surplus_pool_trade_in[reg][0].value,nerc_region[reg][0].value),exchanges_list)
                            chk=1;
                            break;
           #name = 'Electricity from generation mix '+eGRID_region[reg][0].value
           #fuelname =
           if chk == 1:
               exchange(exchange_table_creation_input_con_mix(generation_quantity[reg][0].value,region),exchanges_list)
           else:
               exchange(exchange_table_creation_input_con_mix(1,region),exchanges_list)
           
           final = process_table_creation_con_mix(region,exchanges_list)
           print(region+' Consumption Mix Process Created')
           consumption_dict['Consumption'+region] = final;
   return consumption_dict

































#Creating Surplus Pool dictionary
surplus_dict = surplus_pool_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)
#del surplus_dict['']




#Creating Consumption dictionary
consumption_dict = consumption_mix_dictionary(nerc_region,surplus_pool_trade_in,trade_matrix,generation_quantity,egrid_regions,nerc_region2)


#Test distr


#The distribution mix dictionary does not require any new information and it will be fine to use the GEN mix dictionary to write these templates.
#Only extra infomration required is the efficiency
#distribution_dict = distribution_mix_dictionary(egrid_subregions,efficiency_of_distribution_grid)
#del distribution_dict['']


#consumption_mix_template_generator(consumption_dict)
#surplus_pool_mix_template_generator(surplus_dict,nerc_region2)
#distribution_template_generator(distribution_dict,efficiency_of_distribution_grid)





