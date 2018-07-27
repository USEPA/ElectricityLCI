#Data filtered from stewi and Stewi combo is written ina dictionary in this script. 
#The dictionary is basaed on the OLCA schema
#This dictionary can be used for writing json files or templates
import numpy as np
import math
import time
import pandas as pd
from electricitylci.process_exchange_aggregator_uncertainty import compilation,uncertainty
from electricitylci.globals import egrid_year


 


year = egrid_year

def data_transfer(database,fuelname_p,fuelheat_p,d_list_p,odd_year_p,odd_database_p):
   
   global region;
   global exchanges_list;  
   global fuelname
   global fuelheat
   global year
   global odd_year
   global odd_database

   
      
   exchanges_list = []
   fuelname = fuelname_p
   fuelheat = fuelheat_p
   odd_year = odd_year_p
   odd_database = odd_database_p

   
   region = pd.unique(database['Subregion'])[0]



def exchange_list_creator(region_p):
    global exchanges_list;
    global region 
    region = region_p
    exchanges_list = [] 




def exchange_table_creation_ref(data):
    
    
    global region; 
    region = data['Subregion'].iloc[0] 
    global exchanges_list; 
    exchanges_list = []
    
    
    #data=pd.DataFrame(columns = ['Electricity','Heat'])
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
    ar['location'] = location()
    
    #ar['uncertainty']=uncertainty_table_creation(data)   
    #ar['uncertainty'] = ''
    del ar['']
    return ar
    #ar['pedigreeUncertainty']=''
    #ar['uncertainty']=uncertainty_table_creation(data)      




def exchange_table_creation_input_genmix(database,fuelname):
    
    year = egrid_year 
    

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
    ar['amount']=database['Generation_Ratio'].iloc[0]
    ar['unit'] = unit('MWh')    
    ar['pedigreeUncertainty']=''
    ar['category']='22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+fuelname
    ar['comment']='eGRID '+str(year);
    ar['uncertainty'] = ''
    del ar['']
    
    return ar;

def process_table_creation_genmix():
    
    global region
    global exchanges_list                             
    ar = {'':''}
    
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=location()
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
    
    
    
def process_table_creation(fuelname):
    
    global exchanges_list;
    global region;
    
                              
    ar = {'':''}
    
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=location()
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']=''
    ar['name'] = 'Electricity; from '+str(fuelname)+' ; at generating facility'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+fuelname
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
    ar['copyright']=False
    ar['creationDate']=time.time()
    ar['dataDocumentor']='Wes Ingwersen'
    ar['dataGenerator']='Tapajyoti Ghosh'
    ar['dataSetOwner']=''
    ar['intendedApplication']=''
    ar['projectDescription']=''
    ar['publication']=''
    ar['geographyDescription']=''
    ar['exchangeDqSystem'] = exchangeDqsystem()
    del ar['']
    
    return ar;

def exchangeDqsystem():
    ar = {'':''}
    ar['@type'] = 'DQSystem'
    ar['name'] = 'US_EPA - Flow Pedigree Matrix'
    
    del ar['']
    return ar
    


def exchange_table_creation_input(data,fuelname,fuelheat):
    
    global region; 
    year = data['Year'].iloc[0]
    region = data['Subregion'].iloc[0]
   
    
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow'] = flow_table_creation(fuelname,'Input Fuel')
    ar['flowProperty']=''
    ar['input'] = True
    ar['baseUncertainty']=''
    ar['provider']=''
    
    if math.isnan(fuelheat) != True:
      ar['amount']=data['Emission_factor'].iloc[0]/fuelheat
    else:          
      ar['amount']=data['Emission_factor'].iloc[0]
    
    ar['amountFormula']='  '
    if math.isnan(fuelheat) != True:        
      ar['unit']=unit('kg');
    else:
      ar['unit']=unit('MJ')
    
    
    ar['dqEntry'] = ''
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

 
def exchange_table_creation_output(data):
    
    year = data['Year'].iloc[0]
    comp = data['Compartment'].iloc[0]
    source = data['Source'].iloc[0]
    
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(data['FlowName'].iloc[0],comp)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=False
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=data['Emission_factor'].iloc[0]
    ar['amountFormula']=''
    ar['unit']=unit('kg');
    ar['pedigreeUncertainty']=''  
    ar['dqEntry'] = '('+str(data['ReliabilityScoreAvg'].iloc[0])+';1;1;1)'
    ar['uncertainty']=uncertainty_table_creation(data)
    ar['comment'] = str(source)+' '+str(year);


    del ar['']
    return ar;    

  
        
def uncertainty_table_creation(data):
    
  
    
    ar = {'':''}
    

    ar['geomMean'] = data['GeomMean'].iloc[0]
    ar['geomSd']= data['GeomSD'].iloc[0] 

    
    
    ar['distributionType']='Logarithmic Normal Distribution'
    ar['mean']=''
    ar['meanFormula']=''
    
    ar['geomMeanFormula']=''

    ar['minimum']=data['Maximum'].iloc[0]
    ar['maximum']=data['Minimum'].iloc[0]
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

def ref_flow_creator():
    
    
    #data=pd.DataFrame(columns = ['Electricity','Heat'])
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
    ar['location'] = location()
    
    #ar['uncertainty']=uncertainty_table_creation(data)   
    #ar['uncertainty'] = ''
    del ar['']
    return ar
    #ar['pedigreeUncertainty']=''
    #ar['uncertainty']=uncertainty_table_creation(data)   

def exchange_table_creation_input_con_mix(generation,name,loc):
    
    global region;
    global year;
    global fuelheat; 
    

    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(name,None)
    ar['flowProperty']=''
    ar['input']=True
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount'] = generation
    ar['unit'] = unit('MWh')    
    ar['pedigreeUncertainty']=''
    ar['uncertainty']=''
    ar['comment']='eGRID '+str(year);
    ar['location'] = loc
    del ar['']
    
    return ar;


def process_table_creation_con_mix():
    
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
    ar['name'] = 'Electricity; at region '+str(region)+'; Consumption Mix'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = 'Electricity showing Consumption mix using power plants in the '+str(region)+' region'
    
    
    return ar;


def process_table_creation_surplus():
    
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
    ar['name'] = 'Electricity; at region '+str(region)+'; Surplus Pool'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = 'Electricity showing surplus in the '+str(region)+' region'
    
    
    return ar;


def process_table_creation_distribution():
    
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
    ar['name'] = 'Electricity; at region '+str(region)+'; Distribution Mix'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = 'Electricity from distribution mix in the '+str(region)+' region'
    
    
    return ar;

def process_table_creation_trade_mix():
    
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
    ar['name'] = 'Electricity; at region '+str(region)+'; Trade Mix'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = 'Electricity showing Trade mix using power plants in the '+str(region)+' region'

    
    return ar;           
            

            
            
            
            
            
        
        
    
     