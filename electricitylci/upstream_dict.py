# -*- coding: utf-8 -*-

import pandas as pd
from electricitylci.globals import output_dir, data_dir
from electricitylci.coal_upstream import (
        coal_type_codes,
        mine_type_codes,
        basin_codes)
from electricitylci import write_process_dicts_to_jsonld

def _unit(unt):
    ar = dict()
    ar['internalId']=''
    ar['@type']='Unit'
    ar['name'] = unt
    return ar

def _process_table_creation_gen(process_name, exchanges_list, fuel_type):
    fuel_category_dict={
            'Coal':'21: Mining, Quarrying, and Oil and Gas Extraction/2121: Coal Mining',
            'Natural gas':'21: Mining, Quarrying, and Oil and Gas Extraction/2111: Oil and Gas Extraction',
            'Petroleum':'21: Mining, Quarrying, and Oil and Gas Extraction/2111: Oil and Gas Extraction',
            'Nuclear':'21: Mining, Quarrying, and Oil and Gas Extraction/2122: Metal Ore Mining',
            'Geothermal':'22: Utilities/2211: Electric Power Generation Transmission and Distribuion'
    }    
    ar = dict()
    ar['@type'] = 'Process'
    ar['allocationFactors']=''
    ar['defaultAllocationMethod']=''
    ar['exchanges']=exchanges_list;
    ar['location']=''#location(region)
    ar['parameters']=''
    #ar['processDocumentation']=process_doc_creation();
    ar['processType']='UNIT_PROCESS'
    ar['name'] = process_name
    ar['category'] = fuel_category_dict[fuel_type]
    ar['description'] = 'Fuel produced in stated region'
    return ar;

def _exchange_table_creation_ref(fuel_type):
    #region = data['Subregion'].iloc[0]
    natural_gas_flow={
            'flowType':'PRODUCT_FLOW',
            'flowProperties':'',
            'name':'natural gas, through transmission',
            'id':'',
            'category':'21: Mining, Quarrying, and Oil and Gas Extraction',
    }
    
    coal_flow={
            'flowType':'PRODUCT_FLOW',
            'flowProperties':'',
            'name':'coal, through cleaning',
            'id':'',
            'category':'21: Mining, Quarrying, and Oil and Gas Extraction',
    }
    
    petroleum_flow={
            'flowType':'PRODUCT_FLOW',
            'flowProperties':'',
            'name':'petroleum fuel, through transportation',
            'id':'',
            'category':'21: Mining, Quarrying, and Oil and Gas Extraction',
    }
    
    transport_flow={
            'flowType':'PRODUCT_FLOW',
            'flowProperties':'',
            'name':'coal, transported',
            'id':'',
            'category':'21: Mining, Quarrying, and Oil and Gas Extraction'
    }
    nuclear_flow={
            'flowType':'PRODUCT_FLOW',
            'flowProperties':'',
            'name':'nuclear fuel, through transportation',
            'id':'',
            'category':'21: Mining, Quarrying, and Oil and Gas Extraction'
    }
    geothermal_flow={
            'flowType':'PRODUCT_FLOW',
            'flowProperties':'',
            'name':'geothermal, upstream and plant',
            'id':'',
            'category':'22: Utilities'
    }
    
    ar = dict()
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    if fuel_type == 'Coal':
        ar['flow']=coal_flow
        ar['unit']= _unit('sh tn')
        ar['amount']=1.0
    elif fuel_type == 'Natural gas':
        ar['flow']=natural_gas_flow
        ar['unit'] = _unit('btu')
        ar['amount']=10**6
    elif fuel_type == 'Petroleum':
        ar['flow']=petroleum_flow
        ar['unit'] = _unit('MJ')
        ar['amount']=1
    elif fuel_type == 'Coal transport':
        ar['flow']=transport_flow
        ar['unit']=_unit('lb*mi')
        ar['amount']=2000
    elif fuel_type == 'Nuclear':
        ar['flow']=nuclear_flow
        ar['unit']=_unit('MWh')
        ar['amount']=1
    elif fuel_type == 'Geothermal':
        ar['flow']=geothermal_flow
        ar['unit']=_unit('MWh')
        ar['amount']=1
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=True
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amountFormula']=''
    return ar

def _flow_table_creation(data):
    ar = dict()
    ar['flowType'] = 'ELEMENTARY_FLOW'
    ar['flowProperties']=''
    ar['name'] = data['FlowName'][0:255] #cutoff name at length 255 if greater than that
    ar['id'] = data['FlowUUID']
    comp = str(data['Compartment'])
    if (ar['flowType']=='ELEMENTARY_FLOW')&(comp!=''):
        ar['category'] = 'Elementary flows/' + 'emission' + '/' + comp
    elif (ar['flowType'] == 'PRODUCT_FLOW') & (comp!=''):
        ar['category'] = comp
    elif ar['flowType'] == 'WASTE_FLOW':
        ar['category'] = 'Waste flows/'
    else:
        ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'
    return ar

def _exchange_table_creation_output(data):
    #year = data['Year'].iloc[0]
    #source = data['Source'].iloc[0]
    ar = dict()
    ar['internalId']=''
    ar['@type']='Exchange'
    ar['avoidedProduct']=False
    ar['flow']=_flow_table_creation(data)
    ar['flowProperty']=''
    ar['input']=False
    ar['quantitativeReference']=False
    ar['baseUncertainty']=''
    ar['provider']=''
    ar['amount']=data['emission_factor']
    ar['amountFormula']=''
    ar['unit']=_unit('kg');
    ar['pedigreeUncertainty']=''
#    ar['dqEntry'] = '('+str(round(data['Reliability_Score'].iloc[0],1))+\
#                    ';'+str(round(data['TemporalCorrelation'].iloc[0],1))+\
#                    ';' + str(round(data['GeographicalCorrelation'].iloc[0],1))+\
#                    ';' + str(round(data['TechnologicalCorrelation'].iloc[0],1))+ \
#                    ';' + str(round(data['DataCollection'].iloc[0],1))+')'
#    ar['uncertainty']=uncertainty_table_creation(data)
    #ar['comment'] = str(source)+' '+str(year)
    #if data['FlowType'].iloc[0] == 'ELEMENTARY_FLOW':
    #  ar['category'] = 'Elementary flows/'+str(data['ElementaryFlowPrimeContext'].iloc[0])+'/'+str(data['Compartment'].iloc[0])
    #elif data['FlowType'].iloc[0] == 'WASTE_FLOW':
    #  ar['category'] = 'Waste flows/'
    #else:
    #  ar['category'] = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution'+data['FlowName'].iloc[0]
    if type(ar)=='DataFrame':
        print(data)
    return ar;

def olcaschema_genupstream_processes(merged):
    """
    Generate olca-schema dictionaries for upstream processes for the inventory
    provided in the given dataframe.
    
    Parameters
    ----------
    merged: dataframe
        Dataframe containing the inventory for upstream processes used by
        eletricity generation.
    
    Returns
    ----------
    dictionary
        Dictionary containing all of the unit processes to be written to 
        JSON-LD for import to openLCA
    """
    mapped_column_dict={
        'UUID (EPA)':'FlowUUID',
        'FlowName':'model_flow_name',
        'Flow name (EPA)':'FlowName'
    }
    
    #This is a mapping of various NETL flows to federal lca commons flows
    netl_epa_flows = pd.read_csv(
            data_dir+'/Elementary_Flows_NETL.csv',
            skiprows=2,
            usecols=[0,1,2,6,7,8]
    )
    netl_epa_flows['Category']=netl_epa_flows['Category'].str.replace(
            'Emissions to ','',).str.lower()
    netl_epa_flows['Category']=netl_epa_flows['Category'].str.replace(
            'emission to ','',).str.lower()
    
    coal_type_codes_inv = dict(map(reversed, coal_type_codes.items()))
    mine_type_codes_inv = dict(map(reversed, mine_type_codes.items()))
    basin_codes_inv = dict(map(reversed, basin_codes.items()))
    coal_transport=['Barge',
                    'Lake Vessel',
                    'Ocean Vessel',
                    'Railroad',
                    'Truck']
#    merged_summary = merged.groupby([
#            'fuel_type','stage_code','FlowName','Compartment'],as_index=False
#            )['quantity','FlowAmount'].sum()
    #First going to keep plant IDs to account for possible emission repeats
    #for the same compartment, leading to erroneously low emission factors
    merged_summary = merged.groupby(
            ['fuel_type','stage_code','FlowName','Compartment','plant_id'],as_index=False
            ).agg({
                    'FlowAmount':'sum',
                    'quantity':'mean'
                    })
    merged_summary = merged_summary.groupby(
            ['fuel_type','stage_code','FlowName','Compartment'], 
            as_index=False)['quantity','FlowAmount'].sum()
    #ng_rows = merged_summary['fuel_type']=='Natural gas'
    
    #For natural gas extraction there are extraction and transportation stages
    #that will get lumped together in the groupby which will double
    #the quantity and erroneously lower emission rates.
    #merged_summary.loc[ng_rows,'quantity']=merged_summary.loc[ng_rows,'quantity']/2
    merged_summary['emission_factor']=(
            merged_summary['FlowAmount']/merged_summary['quantity']
    )
    upstream_list = list(
            x for x in merged_summary['stage_code'].unique() 
            #if x not in coal_transport
    )
    
    merged_summary['FlowDirection']='output'
    upstream_process_dict=dict()
    #upstream_list=['Appalachian']
    for upstream in upstream_list:
        print(f'Building diciontary for {upstream}', end ='...')
        exchanges_list=list()
        #upstream = upstream_list[0]
        upstream_filter = merged_summary['stage_code']==upstream
        merged_summary_filter = merged_summary.loc[upstream_filter,:]
        merged_summary_filter_mapped = pd.merge(
                left=merged_summary_filter,
                right=netl_epa_flows,
                left_on=['FlowName','Compartment'],
                right_on=['NETL Flows','Category'],
                how='left'
        )
        merged_summary_filter_mapped = merged_summary_filter_mapped.rename(
                columns=mapped_column_dict,copy=False)
        merged_summary_filter_mapped.drop_duplicates(
                subset=['FlowName','Compartment','FlowAmount'], inplace=True)
        merged_summary_filter_mapped.dropna(subset=['FlowName'],inplace=True)
        garbage=merged_summary_filter_mapped.loc[
                merged_summary_filter_mapped['FlowName']=='[no match]',:].index
        merged_summary_filter_mapped.drop(garbage, inplace=True)   
        ra = merged_summary_filter_mapped.apply(
                _exchange_table_creation_output,axis=1).tolist()
        exchanges_list.extend(ra)
        first_row=min(merged_summary_filter_mapped.index)
        fuel_type=merged_summary_filter_mapped.loc[first_row,'fuel_type']
        stage_code=merged_summary_filter_mapped.loc[first_row,'stage_code']
        if (fuel_type=='Coal') & (stage_code not in coal_transport):
            split_name=merged_summary_filter_mapped.loc[
                    first_row,'stage_code'].split('-')
            combined_name=(
                    'coal extraction and processing - '  
                    + basin_codes_inv[split_name[0]]+', '
                    + coal_type_codes_inv[split_name[1]]+', '
                    + mine_type_codes_inv[split_name[2]]
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif (fuel_type=='Coal') & (stage_code in coal_transport):
            combined_name=(
                    'coal transport - '
                    + stage_code
            )
            exchanges_list.append(_exchange_table_creation_ref('Coal transport'))
        elif fuel_type=='Natural gas':
            combined_name=(
                    'natural gas extraction and processing - '
                    + merged_summary_filter_mapped.loc[first_row,'stage_code']
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type=='Petroleum':
            split_name=merged_summary_filter_mapped.loc[
                    first_row,'stage_code'].split('_')
            combined_name=(
                    'petroleum extraction and processing - '
                    + split_name[0] 
                    +' '
                    f'PADD {split_name[1]}'                
            )
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type=='Nuclear':
            combined_name=('nuclear fuel extraction, prococessing, and transport')
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        elif fuel_type=='Geothermal':
            combined_name=(f'geothermal upstream and operation - {stage_code}')
            exchanges_list.append(_exchange_table_creation_ref(fuel_type))
        process_name=f'{combined_name}'
        final = _process_table_creation_gen(
                process_name, 
                exchanges_list, 
                fuel_type
        )
        upstream_process_dict[
                merged_summary_filter_mapped.loc[first_row,'stage_code']]=final
        print('complete')
    return upstream_process_dict

if __name__=='__main__':
    year=2016
    coal=pd.read_csv(output_dir+f'/coal_emissions_{year}.csv')
    naturalgas=pd.read_csv(output_dir+f'/ng_emissions_{year}.csv')
    petroleum=pd.read_csv(output_dir+f'/petroleum_emissions_{year}.csv')
    nuclear=pd.read_csv(output_dir+f'/nuclear_emissions_{year}.csv',
                        usecols=[1,2,5,6,7,8,9,10])
    geothermal=pd.read_csv(output_dir+f'/geothermal_emissions_{year}.csv')
    merged = pd.concat([coal,naturalgas,petroleum,nuclear,geothermal],ignore_index=True)
    upstream_process_dict=olcaschema_genupstream_processes(merged)
    write_process_dicts_to_jsonld(upstream_process_dict)
    