#Data filtered from stewi and Stewi combo is written ina dictionary in this script. 
#The dictionary is basaed on the OLCA schema
#This dictionary can be used for writing json files or templates
import numpy as np
import math
import pandas as pd
from electricitylci.process_exchange_uncertainty import compilation,uncertainty
from electricitylci.globals import egrid_year


 


year = egrid_year

def olcaschema_genprocess(database,fuelname_p,fuelheat_p,d_list_p,odd_year_p,odd_database_p):
   
    
   global region;
   global exchanges_list;  
   global fuelname
   global fuelheat
   global year
   global odd_year
   global odd_database
   global d_list
   
      
   exchanges_list = []
   fuelname = fuelname_p
   fuelheat = fuelheat_p
   odd_year = odd_year_p
   odd_database = odd_database_p
   d_list = d_list_p
   

   region = pd.unique(database['Subregion'])[0]
   
     
   #Creating the reference output            
   exchange(exchange_table_creation_ref())
   
   
   #This part is used for writing the input fuel flow informationn
   if database['Heat'].mean() != 0 and fuelheat != 0:
     #Heat input
     database2 = database[['Electricity','Heat']]
     ra1 = exchange_table_creation_input(database2);
     exchange(ra1)
   
    #Dropping not required columns
   database = database.drop(columns = ['FacilityID','eGRID_ID','Year','Subregion','PrimaryFuel','FuelCategory'])
   
   
   for x in d_list:
                            
            if x == 'eGRID': 
                database_f3 = database[['Electricity','Carbon dioxide', 'Nitrous oxide', 'Nitrogen oxides', 'Sulfur dioxide', 'Methane']]
                flowwriter(database_f3,x,'air')
                
                
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

                         
    for i in database_f1.iteritems():
      
      #Only writng the emissions. NOt any other flows or columns in the template files.   
      if str(i[0]) != 'Electricity' and str(i[0]) != 'ReliabilityScore': 
        database_f2 = database_f1[['Electricity',i[0]]] 
            
        if(compilation(database_f2) != 0 and compilation(database_f2)!= None) and math.isnan(compilation(database_f2)) != True:
            
            
            
            ra = exchange_table_creation_output(database_f2,y,comp)
            exchange(ra)




def olcaschema_genmix(database_total,fuel_name):

   global region;
   global exchanges_list;  
   global year
   global fuelname

   
      
   exchanges_list = []
   

   region = pd.unique(database_total['Subregion'])[0]
   
     
   #Creating the reference output            
   exchange(exchange_table_creation_ref())
   
   #print(database['FuelCategory'])
   for row in fuel_name.itertuples():
           fuelname = row[2]
           #croppping the database according to the current fuel being considered
           database_f1 = database_total[database_total['FuelCategory'] == row[1]]
           if database_f1.empty == True:
                  database_f1 = database_total[database_total['PrimaryFuel'] == row[1]]  
            
            
            
           
             
           if database_f1.empty != True:   
               
                 database_f1 = database_f1[['eGRID_ID','Electricity']].drop_duplicates()
                 database = database_total[['eGRID_ID','Electricity']].drop_duplicates()        
                 
                 
                 exchange(exchange_table_creation_input_genmix(database_f1,database))
                 
   #Writing final file       
   final = process_table_creation_genmix()
    
   del final['']
   print(region+' Process Created')
   return final                                      





def exchange_table_creation_input_genmix(database_f1,database):
    
    global fuelname;
    global year;
    global fuelheat; 
    

    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation('Electricity from '+fuelname,None)
    ar['flowProperty']=''
    ar['input']=True
    ar['quantitativeReference']='True'
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=(np.sum(database_f1['Electricity'])/np.sum(database['Electricity']))
    ar['unit'] = unit('MWh')    
    ar['pedigreeUncertainty']=''
    ar['uncertainty']=''
    ar['comment']='eGRID '+str(year);
    del ar['']
    
    return ar;

def process_table_creation_genmix():
    
    global exchanges_list;
    global region;
    global fuelname;
                              
    ar = {'':''}
    
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=region
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']=''
    ar['name'] = 'Electricity; from Generation; at region '+str(region)+'; Production Mix'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = 'Electricity from generation mix using power plants in the '+str(region)+' region'
    
    
    return ar;




def exchange(flw):
    global exchanges_list;    
    exchanges_list.append(flw)
    
    
    
def process_table_creation():
    
    global exchanges_list;
    global region;
    global fuelname;
                              
    ar = {'':''}
    
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=region
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']=''
    ar['name'] = 'Electricity; from '+str(fuelname)+' ; at generating facility'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+str(fuelname)
    ar['description'] = 'Electricity from '+str(fuelname)+' using power plants in the '+str(region)+' region'
    
    
    return ar;
    

#Will be used later
def category():
    
    global fuelname;
    ar = {'':''}   
    ar['@id'] = ''
    ar['@type'] = 'Category'
    ar['name'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+str(fuelname)
    del ar['']
    return ar
    

#Will be used later
def location():
    
    global region;
    ar = {'':''}   
    ar['@id'] = ''
    ar['@type'] = 'Location'
    ar['name'] = region;
    del ar['']
    return ar



def process_doc_creation():
    
    global year;
    
    

    ar = {'':''}
    ar['timeDescription']=''
    ar['validUntil']='12/31'+str(year)
    ar['validFrom']='1/1/'+str(year)
    ar['technologyDescription']=''
    ar['dataCollectionDescription']=''
    ar['completenessDescription']=''
    ar['dataSelectionDescription']=''
    ar['reviewDetails']=''
    ar['dataTreatmentDescription']=''
    ar['inventoryMethodDescription']=''
    ar['modelingConstantsDescription']=''
    ar['reviewer']='Wes Ingwersen'
    ar['samplingDescription']=''
    ar['sources']=''
    ar['restrictionsDescription']=''
    ar['copyright']=''
    ar['creationDate']=''
    ar['dataDocumentor']='Wes Ingwersen'
    ar['dataGenerator']='Tapajyoti Ghosh'
    ar['dataSetOwner']=''
    ar['intendedApplication']=''
    ar['projectDescription']=''
    ar['publication']=''
    ar['geographyDescription']=''
    del ar['']
    
    return ar;




def exchange_table_creation_input(data):
    
    global fuelname;
    global year;
    global database_f1;
    global fuelheat; 
    

    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(fuelname,'Input Fuel')
    ar['flowProperty']=''
    ar['input']=True
    ar['quantitativeReference']='True'
    ar['baseUncertainty']=''
    ar['provider']=''
    if fuelheat != None:
      ar['amount']=(np.sum(data['Heat'])/np.sum(data['Electricity']))/fuelheat;
    else:
      ar['amount']=(np.sum(data['Heat'])/np.sum(data['Electricity']));
    ar['amountFormula']=''
    if fuelheat != None:        
      ar['unit']=unit('kg');
    else:
      ar['unit'] - unit('MJ')
    
    ar['pedigreeUncertainty']=''
    ar['uncertainty']=uncertainty_table_creation(data)
    ar['comment']='eGRID '+str(year);
    del ar['']
    
    return ar;

def unit(unt):
    ar = {'':''}
    ar['internalId']=''
    ar['@type']='Unit'
    ar['name'] = unt
    del ar['']
    
    return ar

 
def exchange_table_creation_output(data,y,comp):
    
    global d;
    global odd_year;
    
    
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(data.columns[1],comp)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=False
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=compilation(data)
    ar['amountFormula']=''
    ar['unit']=unit('kg');
    ar['pedigreeUncertainty']=''    
    ar['uncertainty']=uncertainty_table_creation(data)
    
    if y == odd_database:
         
         ar['comment'] = str(y)+' '+str(odd_year);
         
    else: 
                         
         ar['comment'] = str(y)+' '+str(year);

    del ar['']
    return ar;    

def exchange_table_creation_ref():
    
    
    data=pd.DataFrame(columns = ['Electricity','Heat'])
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation('Electricity',None)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=True
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=1.0
    ar['amountFormula']=''
    ar['unit']=unit('MWh');
    #ar['uncertainty']=uncertainty_table_creation(data)   
    #ar['uncertainty'] = ''
    del ar['']
    return ar
    #ar['pedigreeUncertainty']=''
    #ar['uncertainty']=uncertainty_table_creation(data)        
        
def uncertainty_table_creation(data):
    
    global fuelheat;
    
    
    ar = {'':''}
    
    if data.columns[1] == 'Heat':
            
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
    
            #uncertianty calculations
                    l,b = data.shape
                    if l > 3:
                       u,s = (uncertainty(data))
                       ar['geomMean'] = str(round(math.exp(u),3)); 
                       ar['geomSd']=str(round(math.exp(s),3)); 

    
    
    ar['distributionType']='Logarithmic Normal Distribution'
    ar['mean']=''
    ar['meanFormula']=''
    
    ar['geomMeanFormula']=''
    if fuelheat != None:
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


def flow_table_creation(fl,comp):
    
    global region;
    
                    
    ar = {'':''}
    ar['flowType']='PRODUCT_FLOW'
    ar['cas']=''
    ar['formula']=''
    ar['flowProperties']=''
    ar['location']=str(region)
    ar['name'] = str(fl);
    if comp!=None:
     ar['category'] = 'Elementary Flows/'+str(comp)+'/unspecified'
    else:
     ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = ''
    del ar['']
    
    return ar




