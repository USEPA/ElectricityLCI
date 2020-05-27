
import pandas as pd
from electricitylci.model_config import fuel_name


def map_heat_inputs_to_fuel_names(generation_df):
    fuel_info_tech_flows = fuel_name[fuel_name["ElementaryFlowInput"]==0]
    fuel_info_tech_flows = fuel_info_tech_flows.rename(columns={"Fuelname":"FuelName","FuelList":"FuelCategory"})
    fuel_cols_to_use = ["FuelCategory","FuelName","Heatcontent","Category","Subcategory"]
    fuel_info_tech_flows = fuel_info_tech_flows[fuel_cols_to_use]
    fuel_info_tech_flows['FlowName']='Heat'
    fuel_info_tech_flows['Category'] = fuel_info_tech_flows['Category'].apply(lambda x:str(x)) +'/'+fuel_info_tech_flows['Subcategory'].apply(lambda x:str(x))
    generation_df = pd.merge(generation_df,fuel_info_tech_flows,on=['FlowName','FuelCategory'],how='left')
    generation_df.loc[generation_df['FlowName'] == 'Heat', 'Compartment'] = generation_df['Category']

    generation_df.loc[(generation_df['FlowName'] == 'Heat')&(generation_df['Heatcontent'].notnull()), 'Emission_factor'] = generation_df['Emission_factor']/generation_df['Heatcontent']
    generation_df.loc[(generation_df['FlowName'] == 'Heat')&(generation_df['Heatcontent'].notnull()), 'Minimum'] = generation_df['Minimum']/generation_df['Heatcontent']
    generation_df.loc[(generation_df['FlowName'] == 'Heat')&(generation_df['Heatcontent'].notnull()), 'Maximum'] = generation_df['Maximum']/generation_df['Heatcontent']
    generation_df.loc[(generation_df['FlowName'] == 'Heat')&(generation_df['Heatcontent'].notnull()), 'Unit'] = 'kg'

    # Finally use fuel name for flowname
    generation_df.loc[generation_df['FlowName'] == 'Heat', 'FlowName'] = generation_df['FuelName']
    fuel_cols_to_drop = ["FuelName", "Heatcontent", "Category", "Subcategory"]
    generation_df = generation_df.drop(columns=fuel_cols_to_drop)
    return generation_df
