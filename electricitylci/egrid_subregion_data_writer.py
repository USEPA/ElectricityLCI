#Import egrid subregion data from egrid_data file, converts them all to use names and units used in this project,
#writes them to a csv file

from electricitylci.globals import egrid_year, data_dir

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
                       'eGRID subregion annual N2O emissions (lbs)':'Nitrous oxide'}
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

subregion_totals_egrid_reference_melted.to_csv(data_dir+'egrid_subregion_totals_' + 'reference_' + egrid_year_str + '.csv',index=False)