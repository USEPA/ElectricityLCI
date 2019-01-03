""" 
The functions in this script calculate the fraction of each generating source
(either from generation data or straight from eGRID)
"""

import numpy as np
from electricitylci.process_dictionary_writer import *
from electricitylci.egrid_facilities import egrid_facilities,egrid_subregions
from electricitylci.model_config import use_primaryfuel_for_coal, fuel_name

#Get a subset of the egrid_facilities dataset
egrid_facilities_w_fuel_region = egrid_facilities[['FacilityID','Subregion','PrimaryFuel','FuelCategory','NERC','PercentGenerationfromDesignatedFuelCategory','Balancing Authority Name','Balancing Authority Code']]

#Get reference regional generation data by fuel type, add in NERC
from electricitylci.egrid_energy import ref_egrid_subregion_generation_by_fuelcategory
egrid_subregions_NERC = egrid_facilities[['Subregion','FuelCategory','NERC']]
egrid_subregions_NERC = egrid_subregions_NERC.drop_duplicates()
len(egrid_subregions_NERC)
egrid_subregions_NERC = egrid_subregions_NERC[egrid_subregions_NERC['NERC'].notnull()]
ref_egrid_subregion_generation_by_fuelcategory_with_NERC = pd.merge(ref_egrid_subregion_generation_by_fuelcategory,egrid_subregions_NERC,on=['Subregion','FuelCategory'])

ref_egrid_subregion_generation_by_fuelcategory_with_NERC = ref_egrid_subregion_generation_by_fuelcategory_with_NERC.rename(columns={"Ref_Electricity_Subregion_FuelCategory":"Electricity"})


def create_generation_mix_process_df_from_model_generation_data(generation_data,
    subregion):
    """
    Creates fuel generation mix by subregion. Currently uses a dataframe
    'egrid_facilities_w_fuel_region' that is not an input. This should be changed
    to an input so that the function can accommodate another data source.
    
    Parameters
    ----------
    generation_data : DataFrame
        [description]
    subregion : str
        Description of single region or group of regions. Options include 'all' for
        all eGRID subregions, 'NERC' for all NERC regions, 'BA' for all balancing
        authorities, or a single region (unclear if single region will work).
    
    Returns
    -------
    DataFrame
        [description]
    """
    # Converting to numeric for better stability and merging
    generation_data['FacilityID'] = generation_data['FacilityID'].astype(str)

    database_for_genmix_final = pd.merge(generation_data ,egrid_facilities_w_fuel_region, on='FacilityID')

    if subregion == 'all':
        regions = egrid_subregions
    elif subregion == 'NERC':
        regions = list(pd.unique(database_for_genmix_final['NERC']))
    elif subregion == 'BA':
        regions = list(pd.unique(database_for_genmix_final['Balancing Authority Name']))
    else:
        regions = [subregion]

    result_database = pd.DataFrame()

    for reg in regions:

        if subregion == 'all':
            database = database_for_genmix_final[database_for_genmix_final['Subregion'] == reg]
        elif subregion == 'NERC':
            database = database_for_genmix_final[database_for_genmix_final['NERC'] == reg]
        elif subregion == 'BA':
            database = database_for_genmix_final[database_for_genmix_final['Balancing Authority Name'] == reg]

            # This makes sure that the dictionary writer works fine because it only works with the subregion column. So we are sending the
        # correct regions in the subregion column rather than egrid subregions if rquired.
        # This makes it easy for the process_dictionary_writer to be much simpler.
        if subregion == 'all':
            database['Subregion'] = database['Subregion']
        elif subregion == 'NERC':
            database['Subregion'] = database['NERC']
        elif subregion == 'BA':
            database['Subregion'] = database['Balancing Authority Name']


    # This looks like it is pretty slow.
        total_gen_reg = np.sum(database['Electricity'])
        for index ,row in fuel_name.iterrows():
            # Reading complete fuel name and heat content information
            fuelname = row['FuelList']
            fuelheat = float(row['Heatcontent'])
            # croppping the database according to the current fuel being considered
            database_f1 = database[database['FuelCategory'] == row['FuelList']]
            if database_f1.empty == True:
                database_f1 = database[database['PrimaryFuel'] == row['FuelList']]
            if database_f1.empty != True:
                if use_primaryfuel_for_coal:
                    database_f1['FuelCategory'].loc[database_f1['FuelCategory'] == 'COAL'] = database_f1['PrimaryFuel']
                database_f2 = database_f1.groupby(by = ['Subregion' ,'FuelCategory'])['Electricity'].sum()
                database_f2 = database_f2.reset_index()
                generation = np.sum(database_f2['Electricity'])
                database_f2['Generation_Ratio'] = generation/ total_gen_reg
                frames = [result_database, database_f2]
                result_database  = pd.concat(frames)

    return result_database

#Creates gen mix from reference data
#Only possible for a subregion, NERC region, or total US
def create_generation_mix_process_df_from_egrid_ref_data(subregion):
    """
    [summary]
    
    Parameters
    ----------
    subregion : [type]
        [description]
    
    Returns
    -------
    [type]
        [description]
    """

    # Converting to numeric for better stability and merging
    if subregion == 'all':
        regions = egrid_subregions
    elif subregion == 'NERC':
        regions = list(pd.unique(ref_egrid_subregion_generation_by_fuelcategory_with_NERC['NERC']))
    else:
        regions = [subregion]

    result_database = pd.DataFrame()

    for reg in regions:
        if subregion == 'NERC':
            # This makes sure that the dictionary writer works fine because it only works with the subregion column.
            database = ref_egrid_subregion_generation_by_fuelcategory_with_NERC[ref_egrid_subregion_generation_by_fuelcategory_with_NERC['NERC'] == reg]
            database['Subregion'] = database['NERC']
        elif subregion == 'US':
            #for all US use entire database
            database = ref_egrid_subregion_generation_by_fuelcategory_with_NERC
        else:
            #assume the region is an egrid subregion
            database = ref_egrid_subregion_generation_by_fuelcategory_with_NERC[ref_egrid_subregion_generation_by_fuelcategory_with_NERC['Subregion'] == reg]

        #Get fuel typoes for each region
        #region_fuel_categories = list(pd.unique(database['FuelCategory']))
        total_gen_reg = np.sum(database['Electricity'])
        database['Generation_Ratio'] =  database['Electricity']/total_gen_reg
        # for index,row in database.iterrows():
        #     # cropping the database according to the current fuel being considered
        #     row['Generation_Ratio'] = row['Electricity']/total_gen_reg
        result_database = pd.concat([result_database,database])

    return result_database



        ##MOVE TO NEW FUNCTION
    # if database_for_genmix_reg_specific.empty != True:
    # data_transfer(database_for_genmix_reg_specific, fuelname, fuelheat)
    # Move to separate function
    # generation_mix_dict[reg] = olcaschema_genmix(database_for_genmix_reg_specific)
    # return generation_mix_dict


def olcaschema_genmix(database, subregion):
    generation_mix_dict = {}

    region = list(pd.unique(database['Subregion']))

    for reg in region:

        database_reg = database[database['Subregion'] == reg]
        exchanges_list = []

        # Creating the reference output
        exchange(exchange_table_creation_ref(database_reg), exchanges_list)

        for index, row in fuel_name.iterrows():
            # Reading complete fuel name and heat content information
            fuelname = row['Fuelname']
            # croppping the database according to the current fuel being considered
            database_f1 = database_reg[database_reg['FuelCategory'] == row['FuelList']]
            if database_f1.empty != True:
                ra = exchange_table_creation_input_genmix(database_f1, fuelname)
                exchange(ra, exchanges_list)
                # Writing final file
        final = process_table_creation_genmix(reg, exchanges_list)

        # print(reg +' Process Created')
        generation_mix_dict[reg] = final
    return generation_mix_dict

