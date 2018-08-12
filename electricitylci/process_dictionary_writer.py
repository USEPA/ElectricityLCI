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


def exchange_table_creation_ref(data):
    
    
    region = data['Subregion'].iloc[0] 

    
    
    #data=pd.DataFrame(columns = ['Electricity','Heat'])
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(region,'Electricity',None)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=True
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=1.0
    ar['amountFormula']=''
    ar['unit']=unit('MWh');
    ar['location'] = location(region)
    
    #ar['uncertainty']=uncertainty_table_creation(data)   
    #ar['uncertainty'] = ''
    del ar['']
    return ar
    #ar['pedigreeUncertainty']=''
    #ar['uncertainty']=uncertainty_table_creation(data)      




def exchange_table_creation_input_genmix(database,fuelname):
    
    year = egrid_year 
    region = database['Subregion'].iloc[0] 

    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(region,'Electricity from '+fuelname,None)
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

def process_table_creation_genmix(region,exchanges_list):
    
                        
    ar = {'':''}
    
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=location(region)
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']=''
    ar['name'] = 'Electricity; from Generation; at region '+str(region)+'; Production Mix'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'
    ar['description'] = 'Electricity from generation mix using power plants in the '+str(region)+' region'
    
    
    return ar;




def exchange(flw,exchanges_list):
   
    exchanges_list.append(flw)
    return exchanges_list
    
    
    
def process_table_creation(fuelname,exchanges_list,region):
    

                              
    ar = {'':''}
    
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=location(region)
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
def location(region):
    
    
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
    ar['validUntil']='12/31/2018'
    ar['validFrom']='1/1/2018'
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
    

    year = data['Year'].iloc[0]
    region = data['Subregion'].iloc[0]
   
    
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow'] = flow_table_creation(region,fuelname,'Input Fuel')
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
    ar['comment']='eGRID '+str(year)
    if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
      ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
      ar['category'] = 'Waste flows/'  
    else:
      ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+fuelname  
        
    
    
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
    region = data['Subregion'].iloc[0]
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(region,data['FlowName'].iloc[0],comp)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=False
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=data['Emission_factor'].iloc[0]
    ar['amountFormula']=''
    ar['unit']=unit('kg');
    ar['pedigreeUncertainty']=''  
    ar['dqEntry'] = '('+str(data['ReliabilityScore'].iloc[0])+\
                    ';'+str(data['TemporalCorrelation'].iloc[0])+\
                    ';' + str(data['GeographicalCorrelation'].iloc[0])+\
                    ';' + str(data['TechnologicalCorrelation'].iloc[0])+ \
                    ';' + str(data['DataCollection'].iloc[0])+')'
    ar['uncertainty']=uncertainty_table_creation(data)
    ar['comment'] = str(source)+' '+str(year)
    if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
      ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
      ar['category'] = 'Waste flows/'
    else:
      ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+data['FlowName'].iloc[0]

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


def flow_table_creation(region,fl,comp):
    
                   
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

def ref_flow_creator(region):
    
    
    #data=pd.DataFrame(columns = ['Electricity','Heat'])
    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(region,'Electricity',None)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=True
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=1.0
    ar['amountFormula']=''
    ar['unit']=unit('MWh');
    ar['location'] = location(region)
    
    #ar['uncertainty']=uncertainty_table_creation(data)   
    #ar['uncertainty'] = ''
    del ar['']
    return ar
    #ar['pedigreeUncertainty']=''
    #ar['uncertainty']=uncertainty_table_creation(data)   

def exchange_table_creation_input_con_mix(generation,name,loc):
    
    year = egrid_year
    

    ar = {'':''}
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(loc,name,None)
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


def process_table_creation_con_mix(region,exchanges_list):
    
                              
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


def process_table_creation_surplus(region,exchanges_list):
    
                              
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


def process_table_creation_distribution(region,exchanges_list):
    
                              
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

def process_table_creation_trade_mix(region,exchanges_list):
    

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
            

            
            
            
            
            
        
        
    
     