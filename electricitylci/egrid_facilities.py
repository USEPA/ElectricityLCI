import pandas as pd
import stewi
from electricitylci.globals import egrid_year
from electricitylci.globals import min_plant_percent_generation_from_primary_fuel_category

#get egrid facility file from stewi
egrid_facilities = stewi.getInventoryFacilities("eGRID",egrid_year)
egrid_facilities.rename(columns={'Plant primary coal/oil/gas/ other fossil fuel category':'FuelCategory','Plant primary fuel':'PrimaryFuel','eGRID subregion acronym':'Subregion'},inplace=True)
egrid_facilities.head()
len(egrid_facilities)
#2016:9709

egrid_subregions = list(pd.unique(egrid_facilities['Subregion']))
#Remove nan if present
egrid_subregions = [x for x in egrid_subregions if str(x) != 'nan']
len(egrid_subregions)
#2016: 26

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

def plant_fuel_check(x):
    plant_fuel_category = x['FuelCategory']
    percent_gen_from_fuel = x[fuel_cat_to_per_gen[plant_fuel_category]]
    if percent_gen_from_fuel >= min_plant_percent_generation_from_primary_fuel_category:
        facility_ids_passing.append(x['FacilityID'])

def list_facilities_w_percent_generation_from_primary_fuel_category_greater_than_min():
    global facility_ids_passing
    facility_ids_passing=[]
    #Apply plant fuel check to every row of the table
    egrid_facilities_fuel_cat_per_gen.apply(plant_fuel_check,axis=1)
    #Delete duplicates
    facility_ids_passing = list(set(facility_ids_passing))
    return facility_ids_passing
