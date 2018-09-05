#Data filtered from stewi and Stewi combo is written ina dictionary in this script. 
#The dictionary is basaed on the OLCA schema
#This dictionary can be used for writing json files or templates
import math
import time
import pandas as pd
from electricitylci.globals import data_dir,egrid_year,electricity_flow_name_generation_and_distribution,electricity_flow_name_consumption

year = egrid_year

#Read in general metadata to be used by all processes
metadata = pd.read_csv(data_dir+'metadata.csv')
#Use only first row of metadata for all processes for now
metadata = metadata.iloc[0,]

#Read in process name info
process_name = pd.read_csv(data_dir+'processname_1.csv')
generation_name_parts = process_name[process_name['Stage']=='generation'].iloc[0]
generation_mix_name_parts = process_name[process_name['Stage']=='generation mix'].iloc[0]


def exchange_table_creation_ref(data):
    region = data['Subregion'].iloc[0]
    ar = dict()
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']= {'flowType':'PRODUCT_FLOW',
                 'flowProperties':'',
                 'name':electricity_flow_name_generation_and_distribution,
                 '@id':'',
                 'category':'22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
                 }
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeRefefrence']=True
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=1.0
    ar['amountFormula']=''
    ar['unit']=unit('MWh');
    #ar['location'] = location(region)
    return ar

def exchange_table_creation_input_genmix(database,fuelname):
    region = database['Subregion'].iloc[0]
    ar = dict()
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']={'flowType':'PRODUCT_FLOW',
                 'flowProperties':'',
                 'name':electricity_flow_name_generation_and_distribution,
                 '@id':'',
                 'category':'22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
                 }
    ar['flowProperty']=''
    ar['input']=True
    ar['quantitativeReference']='True'
    ar['baseUncertainty']=''
    ar['provider']=gen_process_ref(fuelname,region)
    ar['amount']=database['Generation_Ratio'].iloc[0]
    ar['unit'] = unit('MWh')    
    ar['pedigreeUncertainty']=''
    #ar['category']='22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+fuelname
    ar['comment']='from ' + fuelname;
    ar['uncertainty'] = ''
    return ar;

def gen_process_ref(fuel,reg):
    processref = dict()
    processref['name']=generation_name_parts['Base name'] + '; from ' + str(fuel) + '; ' + generation_name_parts['Location type']
    processref['location']=reg
    processref['processType']='UNIT_PROCESS'
    processref['categoryPath']=["22: Utilities","2211: Electric Power Generation, Transmission and Distribution",fuel]
    return processref

def process_table_creation_genmix(region,exchanges_list):
    ar = dict()
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=location(region)
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']='UNIT_PROCESS'
    ar['name'] = generation_mix_name_parts['Base name'] + '; ' + generation_mix_name_parts['Location type'] +  '; ' + generation_mix_name_parts['Mix type']
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
    ar['description'] = 'Electricity generation mix in the '+str(region)+' region'
    return ar;

def exchange(flw,exchanges_list):
   
    exchanges_list.append(flw)
    return exchanges_list

def process_table_creation_gen(fuelname, exchanges_list, region):
    ar = dict()
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=location(region)
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']='UNIT_PROCESS'
    ar['name'] = generation_name_parts['Base name'] + '; from ' + str(fuelname) + '; ' + generation_name_parts['Location type']
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+fuelname
    ar['description'] = 'Electricity from '+str(fuelname)+' produced at generating facilities in the '+str(region)+' region'
    return ar;

#Will be used later
# def category():
#
#     global fuelname;
#     ar = {'':''}
#     ar['@id'] = ''
#     ar['@type'] = 'Category'
#     ar['name'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+str(fuelname)
#     del ar['']
#     return ar
    

#Will be used later
def location(region):
    ar = dict()
    ar['@id'] = ''
    ar['@type'] = 'Location'
    ar['name'] = region;
    return ar

def process_doc_creation():
    
    global year;
    ar = dict()
    ar['timeDescription']=''
    ar['validUntil']='12/31/2018'
    ar['validFrom']='1/1/2018'
    ar['technologyDescription']='This is an aggregation of technology types for this fuel type within this eGRID subregion'
    ar['dataCollectionDescription']=metadata['DataCollectionPeriod']
    ar['completenessDescription']=metadata['DataCompleteness']
    ar['dataSelectionDescription']=metadata['DataSelection']
    ar['reviewDetails']=metadata['DatasetOtherEvaluation']
    ar['dataTreatmentDescription']=metadata['DataTreatment']
    ar['inventoryMethodDescription']=metadata['LCIMethod']
    ar['modelingConstantsDescription']=metadata['ModellingConstants']
    ar['reviewer']=metadata['Reviewer']
    ar['samplingDescription']=metadata['SamplingProcedure']
    ar['sources']=''
    ar['restrictionsDescription']=metadata['AccessUseRestrictions']
    ar['copyright']=False
    ar['creationDate']=time.time()
    ar['dataDocumentor']= metadata['DataDocumentor']
    ar['dataGenerator']= metadata['DataGenerator']
    ar['dataSetOwner']= metadata['DatasetOwner']
    ar['intendedApplication']= metadata['IntendedApplication']
    ar['projectDescription']= metadata['ProjectDescription']
    ar['publication']=''
    ar['geographyDescription']=''
    ar['exchangeDqSystem'] = exchangeDqsystem()
    ar['dqSystem'] = processDqsystem()
    #Temp place holder for process DQ scores
    ar['dqEntry'] = '(5;5)'
    return ar;

def exchangeDqsystem():
    ar = dict()
    ar['@type'] = 'DQSystem'
    ar['@id'] = 'd13b2bc4-5e84-4cc8-a6be-9101ebb252ff'
    ar['name'] = 'US EPA - Flow Pedigree Matrix'
    return ar

def processDqsystem():
    ar = dict()
    ar['@type'] = 'DQSystem'
    ar['@id'] = '70bf370f-9912-4ec1-baa3-fbd4eaf85a10'
    ar['name'] = 'US EPA - Process Pedigree Matrix'
    return ar

def exchange_table_creation_input(data,fuelname,fuelheat):
    year = data['Year'].iloc[0]
    ar = dict()
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow'] = flow_table_creation(data)
    ar['flowProperty']=''
    ar['input'] = True
    ar['baseUncertainty']=''
    ar['provider']='' 
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
    # if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
    #   ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    # elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
    #   ar['category'] = 'Waste flows/'
    # else:
    #   ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+fuelname
    return ar;

def unit(unt):
    ar = dict()
    ar['internalId']=''
    ar['@type']='Unit'
    ar['name'] = unt
    return ar

def exchange_table_creation_output(data):
    year = data['Year'].iloc[0]
    source = data['Source'].iloc[0]
    ar = dict()
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=flow_table_creation(data)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=False
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=data['Emission_factor'].iloc[0]
    ar['amountFormula']=''
    ar['unit']=unit('kg');
    ar['pedigreeUncertainty']=''  
    ar['dqEntry'] = '('+str(round(data['Reliability_Score'].iloc[0],1))+\
                    ';'+str(round(data['TemporalCorrelation'].iloc[0],1))+\
                    ';' + str(round(data['GeographicalCorrelation'].iloc[0],1))+\
                    ';' + str(round(data['TechnologicalCorrelation'].iloc[0],1))+ \
                    ';' + str(round(data['DataCollection'].iloc[0],1))+')'
    ar['uncertainty']=uncertainty_table_creation(data)
    ar['comment'] = str(source)+' '+str(year)
    #if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
    #  ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    #elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
    #  ar['category'] = 'Waste flows/'
    #else:
    #  ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+data['FlowName'].iloc[0]

    return ar;    

        
def uncertainty_table_creation(data):

    ar = dict()
    ar['geomMean'] = data['GeomMean'].iloc[0]
    ar['geomSd']= data['GeomSD'].iloc[0]    
    ar['distributionType']='Logarithmic Normal Distribution'
    ar['mean']=''
    ar['meanFormula']=''    
    ar['geomMeanFormula']=''
    ar['maximum']=data['Maximum'].iloc[0]
    ar['minimum']=data['Minimum'].iloc[0]
    ar['minimumFormula']=''
    ar['sd']=''
    ar['sdFormula']=''    
    ar['geomSdFormula']=''
    ar['mode']=''
    ar['modeFormula']=''   
    ar['maximumFormula']='';
    return ar;

def flow_table_creation(data):
    ar = dict()
    flowtype = data['FlowType'].iloc[0]
    ar['flowType'] = flowtype
    ar['flowProperties']=''
    ar['name'] = data['FlowName'].iloc[0][0:255] #cutoff name at length 255 if greater than that
    ar['@id'] = data['FlowUUID'].iloc[0]
    comp = str(data['Compartment'].iloc[0])
    if comp!=None:
     ar['category'] = 'Elementary Flows/'+comp
    else:
     ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
    if flowtype == 'ELEMENTARY_FLOW':
      ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+comp
    elif flowtype == 'WASTE_FLOW':
      ar['category'] = 'Waste flows/'
    else:
      ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
    return ar

# def ref_flow_creator(region):
#     ar = dict()
#     ar['internalId']=''
#     ar['@type']='Exchange'
#     ar['avoidedProduct']=False
#     ar['flow']=flow_table_creation('PRODUCT_FLOW',electricity_flow_name_generation_and_distribution,None)
#     ar['flowProperty']=''
#     ar['input']=False
#     ar['quantitativeReference']=True
#     ar['baseUncertainty']=''
#     ar['provider']=''
#     ar['amount']=1.0
#     ar['amountFormula']=''
#     ar['unit']=unit('MWh');
#     ar['location'] = ''
#     return ar


def exchange_table_creation_input_con_mix(generation,name,loc):
    
    year = egrid_year
    

    ar = dict()
    
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']={'flowType':'PRODUCT_FLOW',
                 'flowProperties':'',
                 'name':electricity_flow_name_generation_and_distribution,
                 '@id':'',
                 'category':'22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
                 }
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
    return ar;


def process_table_creation_con_mix(region,exchanges_list):
    
                              
    ar = dict()
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']='US-eGRID-'+region
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']=''
    ar['name'] = 'Electricity; at region '+str(region)+'; Consumption Mix'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
    ar['description'] = 'Electricity showing Consumption mix using power plants in the '+str(region)+' region'
    return ar;


def process_table_creation_surplus(region,exchanges_list):
    
                              
    ar = dict()
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=region
    ar['parameters']=''
    ar['processDocumentation']=process_doc_creation();
    ar['processType']=''
    ar['name'] = 'Electricity; at region '+str(region)+'; Surplus Pool'
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
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
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
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
    ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
    ar['description'] = 'Electricity showing Trade mix using power plants in the '+str(region)+' region'

    
    return ar;           
