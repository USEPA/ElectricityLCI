import pandas as pd
import stewi
from electricitylci.globals import egrid_year,data_dir
from electricitylci.globals import min_plant_percent_generation_from_primary_fuel_category

#get egrid facility file from stewi
egrid_facilities = stewi.getInventoryFacilities("eGRID",egrid_year)
egrid_facilities.rename(columns={'Plant primary coal/oil/gas/ other fossil fuel category':'FuelCategory','Plant primary fuel':'PrimaryFuel','eGRID subregion acronym':'Subregion','NERC region acronym':'NERC'},inplace=True)

#Remove NERC from original egrid output in stewi because there are mismatches in the original data with more than 1 NERC per egrid subregion
egrid_facilities = egrid_facilities.drop(columns='NERC')
#Bring in eGRID subregion-NERC mapping
egrid_nerc = pd.read_csv(data_dir+'egrid_subregion_to_NERC.csv')
egrid_facilities = pd.merge(egrid_facilities,egrid_nerc,on='Subregion',how='left')

len(egrid_facilities)
#2016:9709

egrid_subregions = list(pd.unique(egrid_facilities['Subregion']))
#Remove nan if present
egrid_subregions = [x for x in egrid_subregions if str(x) != 'nan']
len(egrid_subregions)

#2016: 26
#egrid_subregions = ['AZNM']

egrid_primary_fuel_categories = sorted(pd.unique(egrid_facilities['FuelCategory'].dropna()))

#correspondence between fuel category and percent_gen
fuel_cat_to_per_gen = {'BIOMASS':'Plant biomass generation percent (resource mix)',
                       'COAL':'Plant coal generation percent (resource mix)',
                       'GAS':'Plant gas generation percent (resource mix)',
                       'GEOTHERMAL':'Plant geothermal generation percent (resource mix)',
                       'HYDRO':'Plant  hydro generation percent (resource mix)',
                       'NUCLEAR':'Plant nuclear generation percent (resource mix)',
                       'OFSL':'Plant other fossil generation percent (resource mix)',
                       'OIL':'Plant oil generation percent (resource mix)',
                       'OTHF':'Plant other unknown / purchased fuel generation percent (resource mix)',
                       'SOLAR':'Plant solar generation percent (resource mix)',
                       'WIND':'Plant wind generation percent (resource mix)'}
#get subset of facility file with only these data
list(fuel_cat_to_per_gen.values())
cols_to_keep = ['FacilityID','FuelCategory']
per_gen_cols = list(fuel_cat_to_per_gen.values())
cols_to_keep = cols_to_keep + per_gen_cols
egrid_facilities_fuel_cat_per_gen = egrid_facilities[cols_to_keep]
egrid_facilities_fuel_cat_per_gen = egrid_facilities_fuel_cat_per_gen[egrid_facilities_fuel_cat_per_gen['FuelCategory'].notnull()]

def add_percent_generation_from_primary_fuel_category_col(x):
    plant_fuel_category = x['FuelCategory']
    x['PercentGenerationfromDesignatedFuelCategory'] = x[fuel_cat_to_per_gen[plant_fuel_category]]
    return x

def list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min():
    passing_facilties = egrid_facilities_fuel_cat_per_gen[egrid_facilities_fuel_cat_per_gen['PercentGenerationfromDesignatedFuelCategory'] > min_plant_percent_generation_from_primary_fuel_category]
    #Delete duplicates by creating a set
    facility_ids_passing = list(set(passing_facilties['FacilityID']))
    return facility_ids_passing

#Add the percent generation from primary fuel cat to its own column
egrid_facilities_fuel_cat_per_gen['PercentGenerationfromDesignatedFuelCategory'] = 0
egrid_facilities_fuel_cat_per_gen = egrid_facilities_fuel_cat_per_gen.apply(add_percent_generation_from_primary_fuel_category_col,axis=1)
egrid_facilities_fuel_cat_per_gen = egrid_facilities_fuel_cat_per_gen.drop(columns=per_gen_cols)
egrid_facilities = egrid_facilities.drop(columns=per_gen_cols)




#Merge back into facilities
egrid_facilities = pd.merge(egrid_facilities,egrid_facilities_fuel_cat_per_gen,on=['FacilityID','FuelCategory'],how='left')

