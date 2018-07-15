#Dictionary Creator
#This is themain file that creates the dictionary with all the regions and fuel. This is essentially the database generator in a dictionary format.


import sys
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


from electricitylci.olcaschema import olcaschema
from electricitylci.egrid_facilities import egrid_subregions
from egrid_databasefile_setup import odd_year,emissions_corrected_final_data,fuel_name,odd_database

#store the total elci data in a csv file just for checking
#emissions_corrected_final_data.to_csv('elci_summary.csv')

#storing the list of databases used in the elci calculations
d_list = list(pd.unique(emissions_corrected_final_data['Source']))

#this is the final database with all the information in the form of olca schema dictionary
final_elci_database = {'':''}

#Looping through different subregions to create the files
for reg in egrid_subregions:
    
    #Cropping out based on regions
    database = emissions_corrected_final_data[emissions_corrected_final_data['Subregion'] == reg]
    
    
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
                    final_elci_database[fuelname+'_'+reg] = olcaschema(database_f1,fuelname,fuelheat,d_list,odd_year,odd_database)
        #del final_elci_database['']
        
del final_elci_database['']    

#from electricitylci import egrid_template_generator                   
                                       
                   
                        
                  
                        