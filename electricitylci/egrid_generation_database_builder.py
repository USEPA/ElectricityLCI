#Dictionary Creator
#This is themain file that creates the dictionary with all the regions and fuel. This is essentially the database generator in a dictionary format.

import sys
import pandas as pd
import os
import warnings
import numpy as np
warnings.filterwarnings("ignore")


from electricitylci.process_dictionary_writer import *
from electricitylci.egrid_facilities import egrid_subregions
from electricitylci.egrid_facilities import egrid_facilities
from electricitylci.egrid_emissions_and_waste_by_facility import years_in_emissions_and_wastes_by_facility
from electricitylci.globals import egrid_year
from electricitylci.globals import fuel_name
from electricitylci.eia923_generation import eia_download_extract

#from electricitylci.generation_processes_from_egrid import emissions_and_waste_by_facility_for_selected_egrid_facilities

   
def create_process_dict(emissions_and_waste_by_facility_for_selected_egrid_facilities):       
    
    global egrid_facilities
    global egrid_subregions
    
    
    data_dir = os.path.dirname(os.path.realpath(__file__))+"/data/"
    os.chdir(data_dir)  
    
    
    
    #Set aside the egrid emissions because these are not filtered
    egrid_emissions_for_selected_egrid_facilities = emissions_and_waste_by_facility_for_selected_egrid_facilities[emissions_and_waste_by_facility_for_selected_egrid_facilities['Source'] == 'eGRID']
    
    #Set aside the other emissions sources
    #non_egrid_emissions_for_selected_egrid_facilities = emissions_and_waste_by_facility_for_selected_egrid_facilities[emissions_and_waste_by_facility_for_selected_egrid_facilities['Source'] != 'eGRID']
    
    #print(list(egrid_emissions_for_selected_egrid_facilities.columns.get_values()))
    #['FacilityID', 'FlowAmount', 'FlowName', 'Compartment', 'ReliabilityScore', 'Source', 'Year', 'FRS_ID', 'eGRID_ID', 'FlowID', 'SRS_CAS', 'SRS_ID', 'Waste Code Type']
    
    #Correcting eGRID ID to numeric type for better merging and code stability
    emissions_and_waste_by_facility_for_selected_egrid_facilities['eGRID_ID'] = emissions_and_waste_by_facility_for_selected_egrid_facilities['eGRID_ID'].apply(pd.to_numeric,errors = 'coerce')
    
    
    #pivoting the database from a list format to a table format
    #egrid_emissions_for_selected_egrid_facilities_pivot = pd.pivot_table(egrid_emissions_for_selected_egrid_facilities,index = ['FacilityID', 'FRS_ID','eGRID_ID','Year','Source','ReliabilityScore'] ,columns = 'FlowName', values = 'FlowAmount').reset_index()
    emissions_for_selected_egrid_facilities_pivot = pd.pivot_table(emissions_and_waste_by_facility_for_selected_egrid_facilities,index = ['eGRID_ID','Year','Source','ReliabilityScore','Compartment'] ,columns = 'FlowName', values = 'FlowAmount').reset_index()
    emissions_for_selected_egrid_facilities_pivot =  emissions_for_selected_egrid_facilities_pivot.drop_duplicates()
    
    
    #Getting the electricity column for all flows
    electricity_for_selected_egrid_facilities_pivot = pd.pivot_table(emissions_and_waste_by_facility_for_selected_egrid_facilities,index = ['eGRID_ID'] ,columns = 'FlowName', values = 'FlowAmount').reset_index()
    electricity_for_selected_egrid_facilities_pivot = electricity_for_selected_egrid_facilities_pivot.drop_duplicates()
    electricity_for_selected_egrid_facilities_pivot = electricity_for_selected_egrid_facilities_pivot[['eGRID_ID','Electricity']]
    electricity_for_selected_egrid_facilities_pivot[['eGRID_ID_1','Electricity_1']] = electricity_for_selected_egrid_facilities_pivot[['eGRID_ID','Electricity']]
    electricity_for_selected_egrid_facilities_pivot=electricity_for_selected_egrid_facilities_pivot.drop(columns = ['eGRID_ID','Electricity'])  
    
    #merging main database with the electricity/net generation columns
    emissions_for_selected_egrid_facilities_final = emissions_for_selected_egrid_facilities_pivot.merge(electricity_for_selected_egrid_facilities_pivot,left_on=['eGRID_ID'],right_on = ['eGRID_ID_1'],how='left')
    
    #Finalizing the database. Also converting MJ to MWH of the egrid generation columns. 
    emissions_for_selected_egrid_facilities_final[['Electricity']] = emissions_for_selected_egrid_facilities_final[['Electricity_1']]*0.00027778
    emissions_for_selected_egrid_facilities_final  = emissions_for_selected_egrid_facilities_final.drop(columns = ['Electricity_1','eGRID_ID_1'])
    
    
    
    database_for_genmix =  emissions_for_selected_egrid_facilities_final[ emissions_for_selected_egrid_facilities_final['Source'] == 'eGRID']
    
    
    #Converting to numeric for better mergind and code stability
    egrid_facilities[['FacilityID']] = egrid_facilities[['FacilityID']].apply(pd.to_numeric,errors = 'coerce')
    database_for_genmix_final = egrid_facilities.merge(database_for_genmix, left_on = ['FacilityID'], right_on = ['eGRID_ID'], how = 'right')
    #Checking the odd year
    for year in years_in_emissions_and_wastes_by_facility:
        
        if year != egrid_year:
           odd_year = year;
           
       
    #checking if any of the years are odd. If yes, we need EIA data. 
    non_egrid_emissions_odd_year = emissions_for_selected_egrid_facilities_final[emissions_for_selected_egrid_facilities_final['Year'] == odd_year]
    odd_database = pd.unique(non_egrid_emissions_odd_year['Source'])
    
    #Downloading the required EIA923 data
    if odd_year != None:
        EIA_923_gen_data = eia_download_extract(odd_year)
    
    
    
    EIA_923_gen_data['Plant Id'] = EIA_923_gen_data['Plant Id'].apply(pd.to_numeric,errors = 'coerce')
    
    #Merging database with EIA 923 data
    database_with_new_generation = emissions_for_selected_egrid_facilities_final.merge(EIA_923_gen_data, left_on = ['eGRID_ID'],right_on = ['Plant Id'],how = 'left')
    
    
    
    database_with_new_generation['Year'] = database_with_new_generation['Year'].apply(pd.to_numeric,errors = 'coerce')
    
    database_with_new_generation = database_with_new_generation.sort_values(by = ['Year'])
    
    
    #Replacing the odd year Net generations with the EIA net generations. 
    database_with_new_generation['Electricity']= np.where(database_with_new_generation['Year'] == int(odd_year), database_with_new_generation['Net Generation\r\n(Megawatthours)'],database_with_new_generation['Electricity'])
    
    
    #Dropping unnecessary columns
    emissions_corrected_gen = database_with_new_generation.drop(columns = ['Plant Id','Plant Name','Plant State','YEAR','Net Generation\r\n(Megawatthours)','Total Fuel Consumption\r\nMMBtu'])
    
    #return emissions_corrected_gen, None
    #non_egrid_emissions_for_selected_egrid_facilities_pivot = emissions_corrected_gen[emissions_corrected_gen['Source'] != 'eGRID']
    
    #Choosing only those columns needed
    egrid_facilities = egrid_facilities[['FacilityID','Subregion','PrimaryFuel','FuelCategory']]
    
    #Converting to numeric for better mergind and code stability
    egrid_facilities[['FacilityID']] = egrid_facilities[['FacilityID']].apply(pd.to_numeric,errors = 'coerce')
    emissions_corrected_gen[['eGRID_ID']] = emissions_corrected_gen[['eGRID_ID']].apply(pd.to_numeric,errors = 'coerce')
    
    
    #Merging with the egrid_facilites file to get the subregion information in the database!!!
    emissions_corrected_final_data = egrid_facilities.merge(emissions_corrected_gen, left_on = ['FacilityID'], right_on = ['eGRID_ID'], how = 'right')
    
    emissions_corrected_final_data  = emissions_corrected_final_data.sort_values(by = ['FacilityID'])
    #store the total elci data in a csv file just for checking
    #emissions_corrected_final_data.to_excel('elci_summary.xlsx')
    
    #storing the list of databases used in the elci calculations
    d_list = list(pd.unique(emissions_corrected_final_data['Source']))
    
    #this is the final database with all the information in the form of olca schema dictionary
    generation_process_dict = {'':''}
    generation_mix_dict = {'':''}
   
    
    
    
    
    
    
    
    
    #Looping through different subregions to create the files
    for reg in egrid_subregions:
        
        #Cropping out based on regions
        database = emissions_corrected_final_data[emissions_corrected_final_data['Subregion'] == reg]
        database_for_genmix_reg_specific = database_for_genmix_final[database_for_genmix_final['Subregion'] == reg]
        
        
        print(reg)   
        
        for row in fuel_name.itertuples():
                    
            
            
            
            
                       
            #Reading complete fuel name and heat content information       
            fuelname = row[2]
            fuelheat = float(row[3])
            
    
            v= [];
            v1= [];
            
            
            #THis code block is used for finding the NERC regions and states.          
            for roww in database.itertuples():
                if row[1] == roww[9] or row[1] == roww[10]:
                    v1.append(roww[4])
                    v.append(roww[11])
                    break;
            v2 = list(set(v1)) 
            str1 = ','.join(v2)
            v2 = list(set(v))
            str2 = ','.join(v2)
           
    
            
                          
            #croppping the database according to the current fuel being considered
            database_f1 = database[database['FuelCategory'] == row[1]]
            if database_f1.empty == True:
                  database_f1 = database[database['PrimaryFuel'] == row[1]]  
            
            
            
            
             
            if database_f1.empty != True:       
            
                        print(fuelname) 
                        data_transfer(database_f1,fuelname,fuelheat,d_list,odd_year,odd_database)
                        generation_process_dict[fuelname+'_'+reg] = olcaschema_genprocess(database_f1,fuelheat,d_list,fuelname)
                        print('\n')
                        
                        
        if database_for_genmix_reg_specific.empty != True:
          data_transfer(database_for_genmix_reg_specific,fuelname,fuelheat,d_list,odd_year,odd_database)
          generation_mix_dict[reg] = olcaschema_genmix(database_for_genmix_reg_specific)
          
          
        
        
    del generation_mix_dict['']  
    del generation_process_dict['']
    return generation_process_dict,generation_mix_dict
                  
                                           
                   
                        
def olcaschema_genprocess(database,fuelheat,d_list,fuelname):
   
    
   #Creating the reference output            
   exchange(exchange_table_creation_ref())
   region = pd.unique(database['Subregion'])[0]
   
   #This part is used for writing the input fuel flow informationn
   if database['Heat'].mean() != 0 and fuelheat != 0:
     #Heat input
     database2 = database[['Electricity','Heat']]
     #THIS CHANGE WAS DONE BECASUE OF THE PRESENCE OF DUPLICATES EGRIDS DUE TO ONE MANY ERRORS IN THE INPUT FILE FROM STEWI COMBO. 
     #THIS DROPS DUPLICATES IF BOTH ELECTRICITY AND FLOW IS SAME. 
     database2.drop_duplicates()
     ra1 = exchange_table_creation_input(database2);
     exchange(ra1)
   
    #Dropping not required columns
   database = database.drop(columns = ['FacilityID','eGRID_ID','Year','Subregion','PrimaryFuel','FuelCategory'])
   
   
   for x in d_list:
                            
            if x == 'eGRID': 
                #database_f3 = database[['Electricity','Carbon dioxide', 'Nitrous oxide', 'Nitrogen oxides', 'Sulfur dioxide', 'Methane']]
                d1 = database[database['Source']=='eGRID']
                d1 = d1.drop(columns = ['Compartment','Source'])
                flowwriter(d1,x,'air')
                
                
            elif x != 'eGRID':  
                database_f3 = database.drop(columns = ['Carbon dioxide', 'Nitrous oxide', 'Nitrogen oxides', 'Sulfur dioxide', 'Methane'])
                
                #This is for extracing only the database being considered fro the d list names. 
                if x == 'TRI':                     
                    database_f3 = database_f3[database_f3['Source']=='TRI']
               
                elif x == 'NEI':
                    database_f3 = database_f3[database_f3['Source']=='NEI']
               
                elif x == 'RCRAInfo':
                    database_f3 = database_f3[database_f3['Source']=='RCRAInfo']
                
                #CHecking if its not empty and differentiating intp the different Compartments that are possible, air , water soil and wastes. 
                if database_f3.empty != True:
                                          
                    #water
                    d1 = database_f3[database_f3['Compartment']=='air']
                    d1 = d1.drop(columns = ['Compartment','Source'])
                    
                    if d1.empty != True:                      
                      flowwriter(d1,x,'air')                           
                    
                    
                    #water
                    d1 = database_f3[database_f3['Compartment']=='water']
                    d1 = d1.drop(columns = ['Compartment','Source'])
                    
                    if d1.empty != True:
                      flowwriter(d1,x,'water')  
                    
                    #soil
                    d1 = database_f3[database_f3['Compartment']=='soil']
                    d1 = d1.drop(columns = ['Compartment','Source'])
                    
                    if d1.empty != True:
                      flowwriter(d1,x,'soil')  
                    
                                            
                    #waste
                    d1 = database_f3[database_f3['Compartment']=='waste']
                    d1 = d1.drop(columns = ['Compartment','Source'])
                    
                    if d1.empty != True:
                      flowwriter(d1,x,'waste')  

                 
            
                
                
   #Writing final file       
   final = process_table_creation()
    
   del final['']
   print(fuelname+'_'+region+' Process Created')
   return final




def flowwriter(database_f1,y,comp):

    
     #THIS CHANGE WAS DONE BECASUE OF THE PRESENCE OF DUPLICATES EGRIDS DUE TO ONE MANY ERRORS IN THE INPUT FILE FROM STEWI COMBO. 
     #THIS DROPS DUPLICATES IF BOTH ELECTRICITY AND FLOW IS SAME.
    database_f1 = database_f1.drop_duplicates()
    
                         
    for i in database_f1.iteritems():
      
      #Only writng the emissions. NOt any other flows or columns in the template files.   

      if str(i[0]) != 'Electricity' and str(i[0]) != 'ReliabilityScore': 

        #This is very important. When the database comes here, it has multiple instances of the same egrid id or plants to preserve individual flow information for the reliability scores. 
        #So we need to make sure that information for a single plant is collaposed to one instance.
        #Along with that we also need ot make sure that the None are also preserved. These correction is very essential. 
        database_f3 = database_f1[['Electricity',i[0]]] 
        database_f3 = database_f3.dropna()
          
        
        database_reliability = database_f1[[i[0],'ReliabilityScore']]
        database_reliability = database_reliability.dropna()
               

        if(compilation(database_f3) != 0 and compilation(database_f3)!= None) and math.isnan(compilation(database_f3)) != True:
            global reliability
            reliability = np.average(database_reliability[['ReliabilityScore']],weights = database_reliability[[i[0]]])
            ra = exchange_table_creation_output(database_f3,y,comp,reliability)
            exchange(ra)

                  
def olcaschema_genmix(database_total):

     
   exchanges_list = []

   region = pd.unique(database_total['Subregion'])[0]
   
    
   #Creating the reference output            
   #exchange_list_creator()
   exchange(exchange_table_creation_ref())
   
   database_total = database_total[['eGRID_ID','Electricity', 'FuelCategory', 'PrimaryFuel']].drop_duplicates()
   for row in fuel_name.itertuples():
           fuelname = row[2]
           #croppping the database according to the current fuel being considered
           database_f1 = database_total[database_total['FuelCategory'] == row[1]]
           if database_f1.empty == True:
                  database_f1 = database_total[database_total['PrimaryFuel'] == row[1]]           
           
             
           if database_f1.empty != True:                
               
                exchange(exchange_table_creation_input_genmix(database_f1,database_total,fuelname))
                 
   #Writing final file       
   final = process_table_creation_genmix()
    
   del final['']
   print(region+' Process Created')
   return final                                                              