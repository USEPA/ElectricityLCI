#Dictionary Creator
#This is the main file that creates the dictionary with all the regions and fuel. This is essentially the database generator in a dictionary format.
import numpy as np
import warnings
warnings.filterwarnings("ignore")

from electricitylci.process_dictionary_writer import *
from electricitylci.egrid_facilities import egrid_facilities,egrid_subregions
from electricitylci.egrid_emissions_and_waste_by_facility import years_in_emissions_and_wastes_by_facility
from electricitylci.globals import output_dir, join_with_underscore
from electricitylci.model_config import (
    egrid_year,
    use_primaryfuel_for_coal,
    fuel_name,
    replace_egrid,
    eia_gen_year,
)
# from electricitylci.eia923_generation import eia_download_extract
from electricitylci.process_exchange_aggregator_uncertainty import compilation,uncertainty,max_min
from electricitylci.elementaryflows import map_emissions_to_fedelemflows,map_renewable_heat_flows_to_fedelemflows,map_compartment_to_flow_type,add_flow_direction
from electricitylci.dqi import lookup_score_with_bound_key
from electricitylci.technosphereflows import map_heat_inputs_to_fuel_names
#Get a subset of the egrid_facilities dataset
egrid_facilities_w_fuel_region = egrid_facilities[['FacilityID','Subregion','PrimaryFuel','FuelCategory','NERC','PercentGenerationfromDesignatedFuelCategory','Balancing Authority Name','Balancing Authority Code']]

from electricitylci.egrid_energy import ref_egrid_subregion_generation_by_fuelcategory

from electricitylci.eia923_generation import eia923_primary_fuel
from electricitylci.eia860_facilities import eia860_balancing_authority


def eia_facility_fuel_region(year):

    primary_fuel = eia923_primary_fuel(year=year)
    ba_match = eia860_balancing_authority(year)

    combined = primary_fuel.merge(ba_match, on='Plant Id')

    combined.rename(
        columns={
            'primary fuel percent gen': 'PercentGenerationfromDesignatedFuelCategory',
            'Plant Id': 'FacilityID',
            'fuel category': 'FuelCategory',
            'NERC Region': 'NERC',
        },
        inplace=True
    )

    return combined


def create_generation_process_df(generation_data,emissions_data,subregion):

    emissions_data = emissions_data.drop(columns = ['FacilityID'])
    combined_data = generation_data.merge(
        emissions_data,
        left_on=['FacilityID', 'Year'],
        right_on=['eGRID_ID', 'Year'],
        how='right'
    )

    # # Checking the odd year to determine if emissions are from a year other than
    # # generation - need to normalize emissions data with generation from the
    # # corresponding data.
    # odd_year = None
    # for year in years_in_emissions_and_wastes_by_facility:

    #     if year != egrid_year:
    #         odd_year = year;
    #         #Code below not being used
    #         #checking if any of the years are odd. If yes, we need EIA data.
    #         #non_egrid_emissions_odd_year = combined_data[combined_data['Year'] == odd_year]
    #         #odd_database = pd.unique(non_egrid_emissions_odd_year['Source'])

    cols_to_drop_for_final = ['FacilityID']
    
    # #Downloading the required EIA923 data
    # # Annual facility generation from the same year as the emissions data
    # # is needed to normalize total facility emissions.
    # if odd_year != None:
    #     EIA_923_gen_data = eia_download_extract(odd_year)
    
    #     #Merging database with EIA 923 data
    #     combined_data = combined_data.merge(EIA_923_gen_data, left_on = ['eGRID_ID'],right_on = ['Plant Id'],how = 'left')
    #     combined_data['Year'] = combined_data['Year'].astype(str)
    #     combined_data = combined_data.sort_values(by = ['Year'])
    #     #Replacing the odd year Net generations with the EIA net generations.
    #     combined_data['Electricity']= np.where(combined_data['Year'] == int(odd_year), combined_data['Net Generation (Megawatthours)'],combined_data['Electricity'])
    #     cols_to_drop_for_final = cols_to_drop_for_final+['Plant Id','Plant Name','State','YEAR','Net Generation (Megawatthours)','Total Fuel Consumption MMBtu']

    #Dropping unnecessary columns
    emissions_gen_data = combined_data.drop(columns = cols_to_drop_for_final)

    if replace_egrid:
        year = eia_gen_year

        # This will only add BA labels, not eGRID subregions
        fuel_region = eia_facility_fuel_region(year)
        final_data = pd.merge(fuel_region, emissions_gen_data, 
                              left_on=['FacilityID'], right_on=['eGRID_ID'],
                              how='right')
    else:
        #Merging with the egrid_facilites file to get the subregion information in the database!!!
        final_data = pd.merge(egrid_facilities_w_fuel_region,
                              emissions_gen_data, left_on=['FacilityID'],
                              right_on=['eGRID_ID'], how='right')

    #Add in reference electricity for subregion and fuel category
    if not replace_egrid:
        final_data = pd.merge(final_data,ref_egrid_subregion_generation_by_fuelcategory,on=['Subregion','FuelCategory'],how='left')
    
    if replace_egrid:
        # Subregion shows up all over the place below. If not using egrid
        # sub in the BA name because we don't have the eGRID subergion.
        final_data['Subregion'] = final_data['Balancing Authority Name']

        subregion_fuel_year_gen = (
            final_data.groupby(
                ['Subregion', 'FuelCategory', 'Year'], as_index=False
            )['Electricity']
                      .sum()
        )
        subregion_fuel_year_gen.rename(columns={
            'Electricity': 'Ref_Electricity_Subregion_FuelCategory'
        }, inplace=True)
        final_data = pd.merge(final_data, subregion_fuel_year_gen,
                              on=['Subregion', 'FuelCategory', 'Year'])
    
    #store the total elci data in a csv file just for checking
    #final_data.to_excel('elci_summary.xlsx')

    # Need to drop rows with NaN electricity generation
    # They currently exist when generation from a facility has been omitted
    # because of some filter (e.g. generation from pirmary fuel < 90%)
    # but we still have emissions data.
    final_data.dropna(subset=['Electricity'], inplace=True)
    
    if subregion == 'all':
        regions = egrid_subregions
    elif subregion == 'NERC':
        regions = list(pd.unique(final_data['NERC']))
    elif subregion == 'BA':
        regions = list(pd.unique(final_data['Balancing Authority Name']))
    else:
        regions = [subregion]

    #final_data.to_excel('Main_file.xlsx')
    final_data = final_data.drop(columns = ['FacilityID'])
    
    #THIS CHECK AND STAMENT IS BEING PUT BECAUSE OF SAME FLOW VALUE ERROR STILL BEING THERE IN THE DATA
    dup_cols_check = [
        'Subregion',
        'PrimaryFuel',
        'FuelCategory',
        'FlowName',
        'FlowAmount',
        'Compartment',
    ]

    final_data = final_data.drop_duplicates(subset=dup_cols_check)
     
    final_data = final_data[final_data['FlowName'] != 'Electricity']

    # Map emission flows to fed elem flows
    final_database = map_emissions_to_fedelemflows(final_data)
    # Create dfs for storing the output
    result_database = pd.DataFrame()
    total_gen_database = pd.DataFrame()
    # Looping through different subregions to create the files

    # Columns to keep in datbase_f2
    database_f2_cols = [
        'Subregion', 'FuelCategory', 'PrimaryFuel', 'eGRID_ID', 
        'Electricity', 'FlowName', 'FlowAmount', 'FlowUUID',
        'Compartment', 'Year', 'Source', 'ReliabilityScore', 'Unit',
        'NERC', 'PercentGenerationfromDesignatedFuelCategory',
        'Balancing Authority Name','ElementaryFlowPrimeContext',
        'Balancing Authority Code', 'Ref_Electricity_Subregion_FuelCategory'
    ]

    for reg in regions:

        print("Creating generation process database for " + reg + " ...")
        # Cropping out based on regions
        if subregion == 'all':
            database = final_database[final_database['Subregion'] == reg]
        elif subregion == 'NERC':
            database = final_database[final_database['NERC'] == reg]
        elif subregion == 'BA':
            database = final_database[final_database['Balancing Authority Name'] == reg]
        elif subregion == 'US':
            # For entire US use full database
            database = final_database
        else:
            # This should be a egrid subregion
            database = final_database[final_database['Subregion'] == reg]

        df_list = []
        for index, row in fuel_name.iterrows():
            # Reading complete fuel name and heat content information
            fuelname = row['FuelList']
            fuelheat = float(row['Heatcontent'])
            # croppping the database according to the current fuel being considered
            database_f1 = database[database['FuelCategory'] == fuelname]

            if database_f1.empty == True:
                database_f1 = database[database['PrimaryFuel'] == fuelname]
            if database_f1.empty != True:

                database_f1 = database_f1.sort_values(by='Source', ascending=False)
                exchange_list = list(pd.unique(database_f1['FlowName']))
                if use_primaryfuel_for_coal:
                    database_f1['FuelCategory'].loc[database_f1['FuelCategory'] == 'COAL'] = database_f1['PrimaryFuel']

                for exchange in exchange_list:
                    database_f2 = database_f1[database_f1['FlowName'] == exchange]
                    database_f2 = database_f2[database_f2_cols]

                    compartment_list = list(pd.unique(database_f2['Compartment']))
                    for compartment in compartment_list:
                        database_f3 = database_f2[database_f2['Compartment'] == compartment]

                        database_f3 = database_f3.drop_duplicates(
                            subset=['Subregion', 'FuelCategory', 'PrimaryFuel', 'eGRID_ID', 'Electricity', 'FlowName',
                                    'Compartment', 'Year', 'Unit'])
                        sources = list(pd.unique(database_f3['Source']))
                        # if len(sources) >1:
                        #    print('Error occured. Duplicate emissions from Different source. Writing an error file error.csv')
                        #    database_f3.to_csv(output_dir+'error'+reg+fuelname+exchange+'.csv')

                        # Get electricity relevant for this exchange for the denominator in the emissions factors calcs
                        electricity_source_by_facility_for_region_fuel = database_f1[
                            ['eGRID_ID', 'Electricity', 'Source']].drop_duplicates()
                        total_gen, mean, total_facility_considered = total_generation_calculator(sources,
                                                                                                 electricity_source_by_facility_for_region_fuel)

                        # Add data quality scores

                        database_f3 = add_flow_representativeness_data_quality_scores(database_f3, total_gen)
                        # Can now drop this
                        database_f3 = database_f3.drop(columns='Ref_Electricity_Subregion_FuelCategory')

                        # Add scores for regions to
                        sources_str = join_with_underscore(sources)
                        exchange_total_gen = pd.DataFrame(
                            [[reg, fuelname, exchange, compartment, sources_str, total_gen]],
                            columns=['Subregion', 'FuelCategory', 'FlowName', 'Compartment', 'Source',
                                     'Total Generation'])
                        total_gen_database = total_gen_database.append(exchange_total_gen, ignore_index=True)

                        if exchange == 'Heat' and str(fuelheat) != 'nan':
                            # Getting Emisssion_factor
                            database_f3['Emission_factor'] = compilation(database_f3[['Electricity', 'FlowAmount']],
                                                                         total_gen) / fuelheat
                            database_f3['Unit'] = 'kg'

                        else:
                            database_f3['Emission_factor'] = compilation(database_f3[['Electricity', 'FlowAmount']],
                                                                         total_gen)

                        # Data Quality Scores
                        database_f3['Reliability_Score'] = np.average(database_f3['ReliabilityScore'],
                                                                      weights=database_f3['FlowAmount'])
                        database_f3['TemporalCorrelation'] = np.average(database_f3['TemporalCorrelation'],
                                                                        weights=database_f3['FlowAmount'])
                        # Set GeographicalCorrelation 1 for now only
                        database_f3['GeographicalCorrelation'] = 1
                        database_f3['TechnologicalCorrelation'] = np.average(database_f3['TechnologicalCorrelation'],
                                                                             weights=database_f3['FlowAmount'])
                        database_f3['DataCollection'] = np.average(database_f3['DataCollection'],
                                                                   weights=database_f3['FlowAmount'])

                        # Uncertainty Calcs
                        uncertainty_info = uncertainty_creation(database_f3[['Electricity', 'FlowAmount']], exchange,
                                                                fuelheat, mean, total_gen, total_facility_considered)

                        database_f3['GeomMean'] = uncertainty_info['geomMean']
                        database_f3['GeomSD'] = uncertainty_info['geomSd']
                        database_f3['Maximum'] = uncertainty_info['maximum']
                        database_f3['Minimum'] = uncertainty_info['minimum']

                        database_f3['Source'] = sources_str

                        # Optionally write out electricity
                        # database_f3['Electricity'] = total_gen

                        # frames = [result_database, database_f3]
                        # result_database = pd.concat(frames)
                        df_list.append(database_f3)
    
    result_database = pd.concat(df_list)

    if subregion == 'all':
        result_database = result_database.drop(
            columns=['eGRID_ID', 'FlowAmount', 'Electricity', 'ReliabilityScore', 'PrimaryFuel', 'NERC',
                     'Balancing Authority Name', 'Balancing Authority Code'])
    elif subregion == 'NERC':
        result_database = result_database.drop(
            columns=['eGRID_ID', 'FlowAmount', 'Electricity', 'ReliabilityScore', 'PrimaryFuel',
                     'Balancing Authority Name', 'Balancing Authority Code', 'Subregion'])
    elif subregion == 'BA':
        result_database = result_database.drop(
            columns=['eGRID_ID', 'FlowAmount', 'Electricity', 'ReliabilityScore', 'PrimaryFuel', 'NERC',
                     'Balancing Authority Code', 'Subregion'])
    elif subregion == 'US':
        result_database = result_database.drop(
            columns=['eGRID_ID', 'FlowAmount', 'Electricity', 'ReliabilityScore', 'PrimaryFuel', 'NERC',
                     'Balancing Authority Name', 'Balancing Authority Code', 'Subregion'])

    result_database = result_database.drop_duplicates()
    # Drop duplicated in total gen database
    #total_gen_database = total_gen_database.drop_duplicates()


    print("Generation process database for " + subregion + " complete.")
    return result_database

    # return b
    


def total_generation_calculator(source_list,electricity_source_db):
    electricity_source_by_region = electricity_source_db[electricity_source_db['Source'].isin(source_list)]
    #drop duplicate facilities
    electricity_source_by_region = electricity_source_by_region.drop_duplicates(subset='eGRID_ID')
    total_gen = electricity_source_by_region['Electricity'].sum()
    mean = electricity_source_by_region['Electricity'].mean()
    total_facility_considered = len(electricity_source_by_region)
    
    
    return total_gen,mean,total_facility_considered
    

def uncertainty_creation(data,name,fuelheat,mean,total_gen,total_facility_considered):
    
    ar = {'':''}
    
    if name == 'Heat':
        
            temp_data = data
            #uncertianty calculations only if database length is more than 3
            l,b = temp_data.shape
            minimum,maximum = max_min(temp_data,mean,total_gen,total_facility_considered)
            if l > 3:
               u,s = uncertainty(temp_data,mean,total_gen,total_facility_considered)
               
               
               if str(fuelheat)!='nan':                  
                   
                  ar['geomMean'] = str(round(math.exp(u),12)/fuelheat);
                  ar['geomSd']=str(round(math.exp(s),12)/fuelheat); 
               else:
                  ar['geomMean'] = str(round(math.exp(u),12)); 
                  ar['geomSd']=str(round(math.exp(s),12)); 
                  
            else:
                                    
                  ar['geomMean'] = None
                  ar['geomSd']= None
                  
            if math.isnan(fuelheat) != True:                   
                 
                  ar['minimum']=minimum/fuelheat;
                  ar['maximum']=maximum/fuelheat;
                  
            else:
                  ar['minimum']=minimum
                  ar['maximum']=maximum
    
    else:
                    minimum,maximum = max_min(data,mean,total_gen,total_facility_considered)
                    #uncertianty calculations
                    l,b = data.shape
                    if l > 3:
                       
                       u,s = (uncertainty(data,mean,total_gen,total_facility_considered))
                       
                       ar['geomMean'] = str(round(math.exp(u),12)); 
                       ar['geomSd']=str(round(math.exp(s),12)); 
                    else:
                       ar['geomMean'] = None
                       ar['geomSd']= None 
                       
                       
                    ar['minimum']=minimum
                    ar['maximum']=maximum
    
    
    ar['distributionType']='Logarithmic Normal Distribution'
    ar['mean']=''
    ar['meanFormula']=''
    
    ar['geomMeanFormula']=''    

    ar['minimumFormula']=''
    ar['sd']=''
    ar['sdFormula']=''    
    ar['geomSdFormula']=''
    ar['mode']=''
    ar['modeFormula']=''
   
    ar['maximumFormula']='';
    del ar['']
    
    return ar;

def add_flow_representativeness_data_quality_scores(db,total_gen):
    db = add_technological_correlation_score(db)
    db = add_temporal_correlation_score(db)
    db = add_data_collection_score(db,total_gen)
    return db

def add_technological_correlation_score(db):
    #Create col, set to 5 by default
    db['TechnologicalCorrelation'] = 5
    from electricitylci.dqi import technological_correlation_lower_bound_to_dqi
    #convert PercentGen to fraction
    db['PercentGenerationfromDesignatedFuelCategory'] = db['PercentGenerationfromDesignatedFuelCategory']/100
    db['TechnologicalCorrelation'] = db['PercentGenerationfromDesignatedFuelCategory'].apply(lambda x: lookup_score_with_bound_key(x,technological_correlation_lower_bound_to_dqi))
    db = db.drop(columns='PercentGenerationfromDesignatedFuelCategory')
    return db

def add_temporal_correlation_score(db):
    db['TemporalCorrelation'] = 5
    from electricitylci.dqi import temporal_correlation_lower_bound_to_dqi
    from electricitylci.model_config import electricity_lci_target_year

    #Could be more precise here with year
    db['Age'] =  electricity_lci_target_year - pd.to_numeric(db['Year'])
    db['TemporalCorrelation'] = db['Age'].apply(
        lambda x: lookup_score_with_bound_key(x, temporal_correlation_lower_bound_to_dqi))
    db = db.drop(columns='Age')
    return db

def add_data_collection_score(db,total_gen):
    from electricitylci.dqi import data_collection_lower_bound_to_dqi
    #Define data collection score based on percentage of the generation as generation for each factor over the total gen for that fuel category
    db['DataCollection'] = 5
    db['Percent_of_Gen_in_EF_Denominator'] = (total_gen/db['Ref_Electricity_Subregion_FuelCategory'])/100
    db['DataCollection'] = db['Percent_of_Gen_in_EF_Denominator'].apply(
        lambda x: lookup_score_with_bound_key(x, data_collection_lower_bound_to_dqi))
    db = db.drop(columns='Percent_of_Gen_in_EF_Denominator')
    return db

#HAVE THE CHANGE FROM HERE TO WRITE DICTIONARY

def olcaschema_genprocess(database,subregion):   

   generation_process_dict = {}

   #Map heat flows for renewable fuels to energy elementary flows. This must be applied after emission mapping
   database = map_renewable_heat_flows_to_fedelemflows(database)
   #Add FlowType to the database
   database = map_compartment_to_flow_type(database)
   #Add FlowDirection, muist be applied before fuel mapping!
   database = add_flow_direction(database)


   #Map input flows to
   database = map_heat_inputs_to_fuel_names(database)


   if subregion == 'all':
        region = egrid_subregions
   elif subregion == 'NERC':
        region = list(pd.unique(database['NERC']))
   elif subregion == 'BA':
        region = list(pd.unique(database['Balancing Authority Name']))  
   else:
        region = [subregion]

   for reg in region:

        print("Writing generation process dictionary for " + reg + " ...")
        # This makes sure that the dictionary writer works fine because it only works with the subregion column. So we are sending the
        #correct regions in the subregion column rather than egrid subregions if rquired.
        #This makes it easy for the process_dictionary_writer to be much simpler.
        if subregion == 'all':
           database['Subregion'] = database['Subregion']
        elif subregion == 'NERC':
           database['Subregion'] = database['NERC']
        elif subregion == 'BA':
           database['Subregion'] = database['Balancing Authority Name']  
        
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
                    ra1 = exchange_table_creation_input(database2)
                    exchanges_list = exchange(ra1,exchanges_list)
                
                database_f2 = database_f1[database_f1['FlowDirection'] == 'output']
                exchg_list = list(pd.unique(database_f2['FlowName']))
                 
                for exchange_emissions in exchg_list:
                    database_f3 = database_f2[database_f2['FlowName']== exchange_emissions]
                    compartment_list = list(pd.unique(database_f3['Compartment']))
                    for compartment in compartment_list:
                        database_f4 = database_f3[database_f3['Compartment'] == compartment]
                        ra = exchange_table_creation_output(database_f4)
                        exchanges_list = exchange(ra,exchanges_list)

                        #if len(database_f4) > 1:
                            #print('THIS CHECK DIS DONE TO SEE DUPLICATE FLOWS. DELETE THIS IN LINE 333 to LINE 338\n')
                            #print(database_f4[['FlowName','Source','FuelCategory','Subregion']])
                            #print('\n')

                final = process_table_creation_gen(fuelname, exchanges_list, reg)
                generation_process_dict[reg+"_"+fuelname] = final

   print("Generation process dictionaries complete.")
   return generation_process_dict
