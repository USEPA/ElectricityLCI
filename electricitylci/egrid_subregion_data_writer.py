#Import egrid subregion data from egrid_data file, converts them all to use names and units used in this project,
#writes them to a csv file
import pandas as pd
from electricitylci.globals import data_dir
from electricitylci.model_config import egrid_year

egrid_year_str = str(egrid_year)

##Read in subregion sheet from egrid data file
year_last2 = egrid_year_str[2:]
#filepath
eGRIDfilepath = '../eGRID/'
egrid_file_begin = {"2014":"eGRID2014", "2016":"egrid2016"}
egrid_file_version = {"2014":"_v2","2016":""}
#filename for 2014
eGRIDfile = eGRIDfilepath + egrid_file_begin[egrid_year_str] + '_Data' + egrid_file_version[egrid_year_str] + '.xlsx'
regionsheetname = 'SRL'+ year_last2
required_fields_map = {'eGRID subregion acronym':'Subregion',
                       'eGRID subregion total annual heat input (MMBtu)':'Heat',
                       'eGRID subregion annual net generation (MWh)':'Electricity',
                       'eGRID subregion annual NOx emissions (tons)':'Nitrogen oxides',
                       'eGRID subregion annual SO2 emissions (tons)':'Sulfur dioxide',
                       'eGRID subregion annual CO2 emissions (tons)':'Carbon dioxide',
                       'eGRID subregion annual CH4 emissions (lbs)':'Methane',
                       'eGRID subregion annual N2O emissions (lbs)':'Nitrous oxide',
                       'eGRID subregion coal generation percent (resource mix)': 'Percent_COAL',
                       'eGRID subregion oil generation percent (resource mix)' : 'Percent_OIL',
                       'eGRID subregion gas generation percent (resource mix)' : 'Percent_GAS',
                       'eGRID subregion nuclear generation percent (resource mix)' : 'Percent_NUCLEAR',
                       'eGRID subregion hydro generation percent (resource mix)' : 'Percent_HYDRO',
                       'eGRID subregion biomass generation percent (resource mix)' : 'Percent_BIOMASS',
                       'eGRID subregion wind generation percent (resource mix)': 'Percent_WIND',
                       'eGRID subregion solar generation percent (resource mix)': 'Percent_SOLAR',
                       'eGRID subregion geothermal generation percent (resource mix)': 'Percent_GEOTHERMAL',
                       'eGRID subregion other fossil generation percent (resource mix)' : 'Percent_OFSL',
                       'eGRID subregion other unknown/ purchased fuel generation percent (resource mix)' : 'Percent_OTHF'}
required_fields = list(required_fields_map.keys())
subregion_totals_egrid_reference = pd.read_excel(eGRIDfile, sheet_name=regionsheetname, skipinitialspace = True)
#drop first row which are column name abbreviations
subregion_totals_egrid_reference = subregion_totals_egrid_reference.drop([0])
subregion_totals_egrid_reference = subregion_totals_egrid_reference[required_fields]
#rename fields
subregion_totals_egrid_reference = subregion_totals_egrid_reference.rename(columns=required_fields_map)

#convert the units
#Electricity already in MWh, don't convert
MMBtu_MJ = 1055.056
USton_kg = 907.18474
lb_kg = 0.4535924
subregion_totals_egrid_reference['Heat']=subregion_totals_egrid_reference['Heat']*MMBtu_MJ
subregion_totals_egrid_reference['Nitrogen oxides']=subregion_totals_egrid_reference['Nitrogen oxides']*USton_kg
subregion_totals_egrid_reference['Sulfur dioxide']=subregion_totals_egrid_reference['Sulfur dioxide']*USton_kg
subregion_totals_egrid_reference['Carbon dioxide']=subregion_totals_egrid_reference['Carbon dioxide']*USton_kg
subregion_totals_egrid_reference['Methane']=subregion_totals_egrid_reference['Methane']*lb_kg
subregion_totals_egrid_reference['Nitrous oxide']=subregion_totals_egrid_reference['Nitrous oxide']*lb_kg
#Melt it into the format compatible with final databases
subregion_totals_egrid_reference_melted = subregion_totals_egrid_reference.melt(id_vars=['Subregion'],
                                                                                value_vars=list(subregion_totals_egrid_reference.columns[1:]),
                                                                                var_name='FlowName',
                                                                                value_name='FlowAmount')

subregion_totals_egrid_reference_melted.to_csv(data_dir+'egrid_subregion_emission_totals_' + 'reference_' + egrid_year_str + '.csv',index=False)

#Use Percent Gen columns
import re
percent_gen_columns = []
new_gen_columns = []
old_to_new_corr = {}
for c in subregion_totals_egrid_reference.columns:
    match = re.match('Percent*',c)
    if re.match('Percent*',c) is not None:
        percent_gen_columns.append(c)
        new_gen = c[len(match[0])+1:]
        new_gen_columns.append(new_gen)
        old_to_new_corr[c] = new_gen

cols_to_keep = list(percent_gen_columns)
cols_to_keep.append('Subregion')
cols_to_keep.append('Electricity')

egrid_subregion_reference_generation = subregion_totals_egrid_reference[cols_to_keep]

#First create the new columns
for c in new_gen_columns:
    egrid_subregion_reference_generation[c] = None

#Now calculate the new cols
for i,r in egrid_subregion_reference_generation.iterrows():
    for k,v in old_to_new_corr.items():
        r[v] = (r[k]/100)*r['Electricity']


cols_to_keep_2 = list(new_gen_columns)
cols_to_keep_2.append('Subregion')


egrid_subregion_reference_generation_by_fuelcategory = egrid_subregion_reference_generation[cols_to_keep_2]

egrid_subregion_reference_generation_by_fuelcategory_melted = egrid_subregion_reference_generation_by_fuelcategory.melt(id_vars=['Subregion'],
                                                                                value_vars=list(new_gen_columns),
                                                                                var_name='FuelCategory',
                                                                                value_name='Electricity')

egrid_subregion_reference_generation_by_fuelcategory_melted.to_csv(data_dir+'egrid_subregion_generation_by_fuelcategory_reference_'+str(egrid_year)+'.csv',index=False)
