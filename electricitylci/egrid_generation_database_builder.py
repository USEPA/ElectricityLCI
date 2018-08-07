#Dictionary Creator
#This is themain file that creates the dictionary with all the regions and fuel. This is essentially the database generator in a dictionary format.

import sys
import pandas as pd
import os
import warnings
import numpy as np
import math
warnings.filterwarnings("ignore")

from electricitylci.process_dictionary_writer import *
from electricitylci.egrid_facilities import egrid_facilities,egrid_subregions
from electricitylci.egrid_emissions_and_waste_by_facility import years_in_emissions_and_wastes_by_facility
from electricitylci.globals import egrid_year, fuel_name
from electricitylci.eia923_generation import eia_download_extract
from electricitylci.process_exchange_aggregator_uncertainty import compilation,uncertainty
from electricitylci.elementaryflows import map_emissions_to_fedelemflows,map_renewable_heat_flows_to_fedelemflows,map_compartment_to_flow_type,add_flow_direction

#Get a subset of the egrid_facilities dataset
egrid_facilities_w_fuel_region = egrid_facilities[['FacilityID','Subregion','PrimaryFuel','FuelCategory']]
egrid_facilities_w_fuel_region['FacilityID'] = egrid_facilities_w_fuel_region['FacilityID'].astype(str)


def create_generation_process_df(generation_data,emissions_data,subregion='ALL'):  

    emissions_data = emissions_data.drop(columns = ['FacilityID'])
    combined_data = generation_data.merge(emissions_data, left_on = ['FacilityID'], right_on = ['eGRID_ID'], how = 'right')   

    #Checking the odd year
    for year in years_in_emissions_and_wastes_by_facility:
        
        if year != egrid_year:
           odd_year = year;

    #checking if any of the years are odd. If yes, we need EIA data.
    non_egrid_emissions_odd_year = combined_data[combined_data['Year'] == odd_year]
    odd_database = pd.unique(non_egrid_emissions_odd_year['Source'])
      
    
    #Downloading the required EIA923 data
    if odd_year != None:
        EIA_923_gen_data = eia_download_extract(odd_year)
    
    EIA_923_gen_data['Plant Id'] = EIA_923_gen_data['Plant Id'].astype(str)
    
    #Merging database with EIA 923 data
    database_with_new_generation = combined_data.merge(EIA_923_gen_data, left_on = ['eGRID_ID'],right_on = ['Plant Id'],how = 'left')
    database_with_new_generation['Year'] = database_with_new_generation['Year'].astype(str)
    database_with_new_generation = database_with_new_generation.sort_values(by = ['Year'])

    #Replacing the odd year Net generations with the EIA net generations. 
    database_with_new_generation['Electricity']= np.where(database_with_new_generation['Year'] == int(odd_year), database_with_new_generation['Net Generation\r\n(Megawatthours)'],database_with_new_generation['Electricity'])
   
    #Dropping unnecessary columns
    emissions_gen_data = database_with_new_generation.drop(columns = ['FacilityID','Plant Id','Plant Name','Plant State','YEAR','Net Generation\r\n(Megawatthours)','Total Fuel Consumption\r\nMMBtu'])

       
    #Merging with the egrid_facilites file to get the subregion information in the database!!!
    final_data = pd.merge(egrid_facilities_w_fuel_region,emissions_gen_data, left_on = ['FacilityID'],right_on = ['eGRID_ID'], how = 'right')
    
    
    #store the total elci data in a csv file just for checking
    #emissions_corrected_final_data.to_excel('elci_summary.xlsx')
    
    if subregion == 'all':
        regions = egrid_subregions
    else:
        regions = [subregion]

    #final_data.to_excel('Main_file.xlsx')
    final_data = final_data.drop(columns = ['FacilityID'])
    
    #THIS CHECK AND STAMENT IS BEING PUT BECAUSE OF SAME FLOW VALUE ERROR STILL BEING THERE IN THE DATA
    final_data = final_data.drop_duplicates(subset = ['Subregion', 'PrimaryFuel','FuelCategory','FlowName','FlowAmount','Compartment'])
    

    b = generation_process_builder_fnc(final_data,regions)
    return b
    


def generation_process_builder_fnc(final_database,regions):
    
    result_database = pd.DataFrame()
    #Looping through different subregions to create the files

    for reg in regions:
        #Cropping out based on regions
        database = final_database[final_database['Subregion'] == reg]
          #database_for_genmix_reg_specific = database_for_genmix_final[database_for_genmix_final['Subregion'] == reg]
        print('\n')
        print(reg)
        print('\n')
        
            
        for index,row in fuel_name.iterrows():
            #Reading complete fuel name and heat content information
            fuelname = row['FuelList']
            fuelheat = float(row['Heatcontent'])
            #croppping the database according to the current fuel being considered
            database_f1 = database[database['FuelCategory'] == row['FuelList']]
            
            if database_f1.empty == True:
                  database_f1 = database[database['PrimaryFuel'] == row['FuelList']]
            if database_f1.empty != True:
                print(row['Fuelname'])

                database_f1  = database_f1.sort_values(by='Source',ascending=False)
                exchange_list = list(pd.unique(database_f1['FlowName']))
                database_f1['FuelCategory'].loc[database_f1['FuelCategory'] == 'COAL'] = database_f1['PrimaryFuel']  
                
                for exchange in exchange_list:
                    database_f2 = database_f1[database_f1['FlowName'] == exchange]
                    database_f2 = database_f2[['Subregion','FuelCategory','PrimaryFuel','eGRID_ID', 'Electricity','FlowName','FlowAmount','Compartment','Year','Source','ReliabilityScore','Unit']]
                    compartment_list = list(pd.unique(database_f2['Compartment']))
                    for compartment in compartment_list:
                        database_f3 = database_f2[database_f2['Compartment'] == compartment]
                       
                        #Getting Emisssion_factor
                        database_f3['Emission_factor'] = compilation(database_f3[['Electricity','FlowAmount']])
                        database_f3['ReliabilityScoreAvg'] = np.average(database_f3['ReliabilityScore'], weights = database_f3['FlowAmount'])
                        uncertainty_info = uncertainty_creation(database_f3[['Electricity','FlowAmount']],exchange,fuelheat)
                        database_f3['GeomMean'] = uncertainty_info['geomMean']
                        database_f3['GeomSD'] = uncertainty_info['geomMean']
                        database_f3['Maximum'] = uncertainty_info['maximum']
                        database_f3['Minimum'] = uncertainty_info['minimum']
                        frames = [result_database,database_f3]
                        result_database  = pd.concat(frames)                   
    result_database = result_database.drop(columns= ['eGRID_ID','Electricity','FlowAmount','ReliabilityScore','PrimaryFuel'])
    result_database = result_database.drop_duplicates()   
    
    
    return result_database
    
            
                
                
        
            #Move to separate function
                #data_transfer(database_f1,fuelname,fuelheat,d_list,odd_year,odd_database)
                #generation_process_dict[fuelname+'_'+reg] = olcaschema_genprocess(database_f1,fuelheat,d_list,fuelname)
                #print('\n')
    #del generation_process_dict['']
    #return generation_process_dict
              
    
     
                                           
def create_generation_mix_process_df(generation_data,subregion='ALL'):
   
   #database_for_genmix =  emissions_for_selected_egrid_facilities_final[emissions_for_selected_egrid_facilities_final['Source'] == 'eGRID']
   generation_data[['Electricity']] = generation_data[['NetGeneration(MJ)']]*0.00027778
    
   #Converting to numeric for better stability and merging
   generation_data['FacilityID'] = generation_data['FacilityID'].astype(str)
   generation_data = generation_data.drop(columns = ['NetGeneration(MJ)'])
   
   database_for_genmix_final = pd.merge(generation_data,egrid_facilities_w_fuel_region, on='FacilityID')
   
   if subregion == 'ALL':
       regions = egrid_subregions
   else:
       regions = [subregion]
   
  
   result_database = pd.DataFrame() 

   for reg in regions:
       database = database_for_genmix_final[database_for_genmix_final['Subregion'] == reg]
       total_gen_reg = np.sum(database['Electricity'])
       for index,row in fuel_name.iterrows():
           # Reading complete fuel name and heat content information
            fuelname = row['FuelList']
            fuelheat = float(row['Heatcontent'])
            #croppping the database according to the current fuel being considered
            database_f1 = database[database['FuelCategory'] == row['FuelList']]
            if database_f1.empty == True:
                  database_f1 = database[database['PrimaryFuel'] == row['FuelList']]
            if database_f1.empty != True:
                  database_f1['FuelCategory'].loc[database_f1['FuelCategory'] == 'COAL'] = database_f1['PrimaryFuel']                  
                  database_f2 = database_f1.groupby(by = ['Subregion','FuelCategory'])['Electricity'].sum()                  
                  database_f2 = database_f2.reset_index()
                  generation = np.sum(database_f2['Electricity'])
                  database_f2['Generation_Ratio'] =  generation/total_gen_reg
                  frames = [result_database,database_f2]
                  result_database  = pd.concat(frames) 
                  
   return result_database
                    
                
           ##MOVE TO NEW FUNCTION
           #if database_for_genmix_reg_specific.empty != True:
               #data_transfer(database_for_genmix_reg_specific, fuelname, fuelheat)
               # Move to separate function
               #generation_mix_dict[reg] = olcaschema_genmix(database_for_genmix_reg_specific)
   #return generation_mix_dict
                        

def uncertainty_creation(data,name,fuelheat):
    
    ar = {'':''}
    
    if name == 'Heat':
            
            temp_data = data
            #uncertianty calculations only if database length is more than 3
            l,b = temp_data.shape
            if l > 3:
               u,s = uncertainty(temp_data)
               if str(fuelheat)!='nan':
                  ar['geomMean'] = str(round(math.exp(u),3)/fuelheat);
                  ar['geomSd']=str(round(math.exp(s),3)/fuelheat); 
               else:
                  ar['geomMean'] = str(round(math.exp(u),3)); 
                  ar['geomSd']=str(round(math.exp(s),3)); 
                  
            else:
                                    
                  ar['geomMean'] = None
                  ar['geomSd']= None 
    else:
    
            #uncertianty calculations
                    l,b = data.shape
                    if l > 3:
                       
                       u,s = (uncertainty(data))
                       ar['geomMean'] = str(round(math.exp(u),3)); 
                       ar['geomSd']=str(round(math.exp(s),3)); 
                    else:
                       ar['geomMean'] = None
                       ar['geomSd']= None 
    
    
    ar['distributionType']='Logarithmic Normal Distribution'
    ar['mean']=''
    ar['meanFormula']=''
    
    ar['geomMeanFormula']=''
    if math.isnan(fuelheat) != True:
        ar['minimum']=((data.iloc[:,1]/data.iloc[:,0]).min())/fuelheat;
        ar['maximum']=((data.iloc[:,1]/data.iloc[:,0]).max())/fuelheat;
    else:
        ar['minimum']=(data.iloc[:,1]/data.iloc[:,0]).min();
        ar['maximum']=(data.iloc[:,1]/data.iloc[:,0]).max();
    ar['minimumFormula']=''
    ar['sd']=''
    ar['sdFormula']=''    
    ar['geomSdFormula']=''
    ar['mode']=''
    ar['modeFormula']=''
   
    ar['maximumFormula']='';
    del ar['']
    
    return ar;


#HAVE THE CHANGE FROM HERE TO WRITE DICTIONARY

def olcaschema_genprocess(database):
   

   generation_process_dict = {}

   #Map emission flows to fed elem flows
   database = map_emissions_to_fedelemflows(database)
   #Map heat flows for renewable fuels to energy elementary flows. This must be applied after emission mapping
   database = map_renewable_heat_flows_to_fedelemflows(database)
   #Add flowtype to the database
   database = map_compartment_to_flow_type(database)
   #Add input-output columns
   database = add_flow_direction(database)
   

   #Creating the reference output
   region = list(pd.unique(database['Subregion']))

   for reg in region: 
       
        database_reg = database[database['Subregion'] == reg]
       
        for index,row in fuel_name.iterrows():
           # Reading complete fuel name and heat content information
            
            fuelname = row['Fuelname']
            fuelheat = float(row['Heatcontent'])             
            database_f1 = database_reg[database_reg['FuelCategory'] == row['FuelList']]
            
            
            if database_f1.empty != True:
                
                exchanges_list=[]
                
                #This part is used for writing the input fuel flow informationn. 
                database2 = database_f1[database_f1['FlowDirection'] == 'input']
                if database2.empty != True:
                                   
                    exchanges_list = exchange(exchange_table_creation_ref(database2),exchanges_list)
                    ra1 = exchange_table_creation_input(database2,fuelname,fuelheat)
                    exchanges_list = exchange(ra1,exchanges_list)
                
                database_f2 = database_f1[database_f1['FlowDirection'] == 'output']
                exchg_list = list(pd.unique(database_f2['FlowName']))
                 
                for exchange_emissions in exchg_list:
                    database_f3 = database_f2[database_f2['FlowName']== exchange_emissions]
                    compartment_list = list(pd.unique(database_f3['Compartment']))
                    for compartment in compartment_list:
                        database_f4 = database_f3[database_f3['Compartment'] == compartment]
                        
                        
                        if len(database_f4) > 1:
                            print('THIS CHECK DIS DONE TO SEE DUPLICATE FLOWS. DELETE THIS IN LINE 312 to LINE 318\n')
                            print(database_f4[['FlowName']])
                            print(database_f4[['Source']])
                            print('\n')
                            
                            
                        ra = exchange_table_creation_output(database_f4)
                        exchanges_list = exchange(ra,exchanges_list)
                
                
                final = process_table_creation(fuelname,exchanges_list,reg)
                del final['']
                generation_process_dict[reg+"_"+fuelname] = final
                
   return generation_process_dict

def olcaschema_genmix(database):
   generation_mix_dict = {}

   region = list(pd.unique(database['Subregion']))
   
   for reg in region:  
     
       
     database_reg = database[database['Subregion'] == reg]
     exchanges_list=[]
     
     #Creating the reference output
     exchange(exchange_table_creation_ref(database_reg),exchanges_list)
     
     for index,row in fuel_name.iterrows():
           # Reading complete fuel name and heat content information 
           fuelname = row['Fuelname']
           #croppping the database according to the current fuel being considered
           database_f1 = database_reg[database_reg['FuelCategory'] == row['FuelList']]
           if database_f1.empty != True:               
               ra = exchange_table_creation_input_genmix(database_f1,fuelname)
               exchange(ra,exchanges_list)
               #Writing final file
     final = process_table_creation_genmix(reg,exchanges_list)
     del final['']
    
   
     print(reg +' Process Created')
     generation_mix_dict[reg] = final
   return generation_mix_dict

