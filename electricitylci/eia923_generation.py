import pandas as pd


#Replace this read in with a check to see if EIA-923 has been processed
EIA_923 = pd.read_csv('EIA923_Schedules_2_3_4_5_M_12_2015_Final_Revision.csv',skipinitialspace = True)

EIA_923 = eia[['Plant Id', 'Plant Name', 'Plant State', 'Total Fuel ConsumptionMMBtu', 'Net Generation']]

# Grouping similar faciliteis together.

EIA_923_generation = EIA_923.groupby(['Plant Id', 'Plant Name','Plant State'], as_index=False)['Total Fuel ConsumptionMMBtu', 'Net Generation'].sum()