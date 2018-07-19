import pandas as pd




emissions_corrected_final_data = pd.read_excel('elci_summary.xlsx')
elec_list = emissions_corrected_final_data[['eGRID_ID','Electricity']]
#elec_list = elec_list.drop_duplicates()
check = emissions_corrected_final_data.groupby(['eGRID_ID','Electricity','FuelCategory','Subregion','Source']).sum()

check = check.reset_index()
check = check.drop_duplicates()
check.to_excel('check.xlsx',index = False)