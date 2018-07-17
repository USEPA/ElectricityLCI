import pandas as pd




emissions_corrected_final_data = pd.read_csv('elci_summary.csv')
elec_list = emissions_corrected_final_data[['eGRID_ID','Electricity']]
#elec_list = elec_list.drop_duplicates()
check = emissions_corrected_final_data.groupby(['eGRID_ID','Electricity','FuelCategory']).sum()

check = check.reset_index()
check.to_csv('check.csv',index = False)