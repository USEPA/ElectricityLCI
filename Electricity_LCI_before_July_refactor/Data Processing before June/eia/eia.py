#Creating the EIA aggregated file from EIA raw data. 

import pandas as pd 

# Import eia file
eia = pd.read_excel('EIA923_Schedules_2_3_4_5_M_12_2015_Final_Revision.xlsx', sheet_name='Page 1 Generation and Fuel Data', skipinitialspace = True)
#drop few row which are not required
eia = eia.drop([0])
eia = eia.drop([1])
eia = eia.drop([2])
eia = eia.drop([3])
eia.columns = eia.iloc[0]
eia = eia.drop([4])

#We just need few columns
eia_w = eia[['Plant Id','Plant Name','Plant State','Total Fuel ConsumptionMMBtu','Net Generation']]

#Grouping similar faciliteis together. 
eia_w1 = eia_w.groupby(['Plant Id','Plant Name','Plant State'],as_index = False)['Total Fuel ConsumptionMMBtu','Net Generation'].sum()

eia_w1.to_excel('eia923.xlsx')

