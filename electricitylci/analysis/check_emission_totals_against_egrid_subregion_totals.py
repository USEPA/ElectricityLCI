import pandas as pd

#Check
from electricitylci.globals import output_dir,data_dir,egrid_year,model_name

#Bring in model gen db and gen mix db
gen = pd.read_csv(output_dir+model_name+'_all_gen_db.csv')
genmix = pd.read_csv(output_dir+model_name+'_all_gen_mix_db.csv')

#join on Subregion, FuelCategory
genwithgenmix = pd.merge(gen,genmix,on=['Subregion','FuelCategory'])
genwithgenmix.head()

#Totals are EF*generation
genwithgenmix['Total_Emission']=genwithgenmix['Emission_factor']*genwithgenmix['Electricity']

#Now these must be grouped by subregion
model_subregion_totals = genwithgenmix.groupby(['Subregion','FlowName','Compartment'])['Total_Emission'].sum().reset_index()

#Just preserve air totals for egrid comparison. Note these have all air emissions and not just egrid emissions
model_subregion_totals_air = model_subregion_totals[model_subregion_totals['Compartment']=='air']
model_subregion_totals_air = model_subregion_totals_air.drop(columns='Compartment')

#Bring in egrid_ref data
ref_egrid_subregion_emission_totals = pd.read_csv(data_dir+'egrid_subregion_totals_reference_'+ str(egrid_year)+ '.csv')

#Merge to compare. Non-egrid emission from the model get dropped here
subregion_emission_comparison = pd.merge(ref_egrid_subregion_emission_totals,model_subregion_totals_air,on=['Subregion','FlowName'])
subregion_emission_comparison = subregion_emission_comparison.rename(columns={"Total_Emission":"Model_Calculated_Emission","FlowAmount":"Reference_Emission"})
subregion_emission_comparison['FracDiff_Model_to_Ref'] = (subregion_emission_comparison['Model_Calculated_Emission']-subregion_emission_comparison['Reference_Emission'])/subregion_emission_comparison['Reference_Emission']

#Export result
subregion_emission_comparison.to_csv(output_dir+model_name+'_egrid_subregion_emission_validation_check.csv',index=False)

