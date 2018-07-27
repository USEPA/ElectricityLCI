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

egrid_subregions = ['CAMX','AZNM']


#Get a subset of the egrid_facilities dataset
egrid_facilities_w_fuel_region = egrid_facilities[['FacilityID','Subregion','PrimaryFuel','FuelCategory']]
egrid_facilities_w_fuel_region['FacilityID'] = egrid_facilities_w_fuel_region['FacilityID'].apply(pd.to_numeric, errors = 'coerce')


def create_generation_process_df(generation_data,emissions_data,subregion='ALL'):
    
    
    
    #Dropping electricity from emissions data
    emissions_data = emissions_data[emissions_data['FlowName']!= 'Electricity']
    

    
    #Converting Units from MJ to MWH
    generation_data[['Electricity']] = generation_data[['NetGeneration(MJ)']]*0.00027778
    
    #Converting to numeric for better stability and merging
    emissions_data['eGRID_ID'] = emissions_data['eGRID_ID'].apply(pd.to_numeric,errors = 'coerce')
    generation_data['eGRID_ID'] = generation_data['FacilityID'].apply(pd.to_numeric,errors = 'coerce')
    generation_data = generation_data.drop(columns = ['FacilityID','NetGeneration(MJ)'])
    emissions_data = emissions_data.drop(columns = ['FacilityID'])
    combined_data = generation_data.merge(emissions_data, left_on = ['eGRID_ID'], right_on = ['eGRID_ID'], how = 'right')
    
    
    

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
    
    EIA_923_gen_data['Plant Id'] = EIA_923_gen_data['Plant Id'].apply(pd.to_numeric,errors = 'coerce')
    
    #Merging database with EIA 923 data
    database_with_new_generation = combined_data.merge(EIA_923_gen_data, left_on = ['eGRID_ID'],right_on = ['Plant Id'],how = 'left')
    database_with_new_generation['Year'] = database_with_new_generation['Year'].apply(pd.to_numeric,errors = 'coerce')
    database_with_new_generation = database_with_new_generation.sort_values(by = ['Year'])

    #Replacing the odd year Net generations with the EIA net generations. 
    database_with_new_generation['Electricity']= np.where(database_with_new_generation['Year'] == int(odd_year), database_with_new_generation['Net Generation\r\n(Megawatthours)'],database_with_new_generation['Electricity'])
   
    #Dropping unnecessary columns
    emissions_gen_data = database_with_new_generation.drop(columns = ['Plant Id','Plant Name','Plant State','YEAR','Net Generation\r\n(Megawatthours)','Total Fuel Consumption\r\nMMBtu'])

       
    #Merging with the egrid_facilites file to get the subregion information in the database!!!
    final_data = pd.merge(egrid_facilities_w_fuel_region,emissions_gen_data,right_on = ['eGRID_ID'], left_on = ['FacilityID'], how = 'right')
    
    
    #store the total elci data in a csv file just for checking
    #emissions_corrected_final_data.to_excel('elci_summary.xlsx')
    
    if subregion == 'all':
        regions = egrid_subregions
    else:
        regions = [subregion]

    
    final_data = final_data.drop(columns = ['FacilityID'])
    
    a = generation_process_builder(final_data,regions)
    return a
    


def generation_process_builder(final_database,regions):
    print(regions)
    result_database = pd.DataFrame()
    #Looping through different subregions to create the files
    for reg in regions:
        #Cropping out based on regions
        database = final_database[final_database['Subregion'] == reg]
          #database_for_genmix_reg_specific = database_for_genmix_final[database_for_genmix_final['Subregion'] == reg]
        print(reg)
               
        for index,row in fuel_name.iterrows():
            #Reading complete fuel name and heat content information
            fuelname = row['FuelList']
            fuelheat = float(row['Heatcontent'])
            #croppping the database according to the current fuel being considered
            database_f1 = database[database['FuelCategory'] == row['FuelList']]
            if database_f1.empty == True:
                  database_f1 = database[database['PrimaryFuel'] == row['FuelList']]
            if database_f1.empty != True:
                print(fuelname)
                          
                database_f1  = database_f1.sort_values(by='Source',ascending=False)
                exchange_list = list(pd.unique(database_f1['FlowName']))
                
                for exchange in exchange_list:
                    database_f2 = database_f1[database_f1['FlowName'] == exchange]
                    database_f2 = database_f2[['Subregion','FuelCategory','PrimaryFuel','eGRID_ID', 'Electricity','FlowName','FlowAmount','Compartment','Year','Source','ReliabilityScore']]
                    #Getting Emisssion_factor
                    database_f2['Emission_factor'] = compilation(database_f2[['Electricity','FlowAmount']])
                    database_f2['ReliabilityScoreAvg'] = np.average(database_f2['ReliabilityScore'], weights = database_f2['FlowAmount'])
                    uncertainty_info = uncertainty_creation(database_f2[['Electricity','FlowAmount']],exchange,fuelheat)
                    database_f2['GeomMean'] = uncertainty_info['geomMean']
                    database_f2['GeomSD'] = uncertainty_info['geomMean']
                    database_f2['Maximum'] = uncertainty_info['maximum']
                    database_f2['Minimum'] = uncertainty_info['minimum']
                    frames = [result_database,database_f2]
                    result_database  = pd.concat(frames)                   
    result_database = result_database.drop(columns= ['eGRID_ID','Electricity','FlowAmount','ReliabilityScore'])
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
   generation_data['FacilityID'] = generation_data['FacilityID'].apply(pd.to_numeric,errors = 'coerce')
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
       print(reg)
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


    





#HAVE THE CHANGE FROM HERE

'''




def olcaschema_genprocess(database,fuelheat,d_list,fuelname):
    
   generation_process_dict = {}

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
   region = pd.unique(database_total['Subregion'])[0]
   exchanges_list = []
   #Creating the reference output
   exchanges_list.append(exchange_table_creation_ref(database_total))
   database_total = database_total[['FacilityID','NetGeneration(MJ)', 'FuelCategory', 'PrimaryFuel']].drop_duplicates()
   for row in fuel_name.itertuples():
           fuelname = row[2]
           #croppping the database according to the current fuel being considered
           database_f1 = database_total[database_total['FuelCategory'] == row[1]]
           if database_f1.empty == True:
                  database_f1 = database_total[database_total['PrimaryFuel'] == row[1]]           
           if database_f1.empty != True:
               exchanges_list.append(exchange_table_creation_input_genmix(database_f1,database_total,fuelname))
   #Writing final file
   final = process_table_creation_genmix(exchanges_list,region)
    
   del final['']
   print(region +' Process Created')
   return final                                                              
   
'''