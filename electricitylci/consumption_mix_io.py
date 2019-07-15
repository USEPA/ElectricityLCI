# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from electricitylci.process_dictionary_writer import *
from electricitylci.model_config import (
    use_primaryfuel_for_coal,
    fuel_name,
    replace_egrid,
    eia_gen_year,
    region_column_name,
)
from electricitylci.generation import eia_facility_fuel_region
from electricitylci.globals import data_dir, output_dir




def olcaschema_consmix(database, cons_dict, subregion):
    consumption_mix_dict = {}


#Placeholder code
database = pd.read_csv(data_dir + '/BAA_final_trade_2016.csv')
subregion = 'BA'

region = list(pd.unique(database['import BAA']))

    for reg in region:


##Placeholder code
        reg = 'CISO'
        
        database_reg = database[database['import BAA'] == reg]
        exchanges_list = []

        # Creating the reference output
        exchange(exchange_table_creation_ref_cons(database_reg), exchanges_list)
        exporter = 'CISO'
        for exporter in list(database["export BAA"].unique()):
               database_f1 = database_reg[database_reg['export BAA'] == exporter]
            if database_f1.empty != True:
                ra = exchange_table_creation_input_con_mix_io(database_f1, exporter)
                ra["quantitativeReference"]=False
                matching_dict = None
                for generator in gen_dict:
                    if gen_dict[generator]["name"] == "Electricity - " + fuelname+" - " + reg:
                        matching_dict = gen_dict[generator]
                        break
                if matching_dict is None:
                    print(f"Trouble matching dictionary for {fuelname} - {reg}")
                else:
                    ra["provider"] = {
                        "name": matching_dict["name"],
                        "@id": matching_dict["uuid"],
                        "category":matching_dict["category"].split("/")
                    }    
                exchange(ra, exchanges_list)
                # Writing final file
        final = process_table_creation_genmix(reg, exchanges_list)

        # print(reg +' Process Created')
        consumption_mix_dict[reg] = final
    return consumption_mix_dict
