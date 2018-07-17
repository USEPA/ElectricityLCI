import pandas as pd
import numpy as np

tri = pd.read_pickle('TRI_2014.pk')
tri.to_csv('chk.csv')
#tri.columns
#len(tri)
#994032

#tri['FlowAmount'].dtype
#dtype('float64')

#print(np.average(tri['FlowAmount']))
#nan

tri = tri.dropna(subset=['FlowAmount'])
len(tri)
#553481 for 2014

'''
#tri[tri['FlowAmount'] == 0]
#See the basis of estimate columns starts to get the Releasetype in there..
#tri.to_csv('chk.csv')
tri['Basis of Estimate'] = tri['Basis of Estimate'].astype(str)
tri = tri.sort_values(by = 'Basis of Estimate',ascending = False)

tri1 = tri[tri['Basis of Estimate'] == 'offsiteother']
tri2 = tri[tri['Basis of Estimate'] == 'offsiteland']
tri1[['ReleaseType']] = 'offsightother';
tri2[['ReleaseType']] = 'offsightland';
tri1[['Basis of Estimate']] = None;
tri2[['Basis of Estimate']] = None;


tri4 = tri[np.logical_and((tri['Basis of Estimate'] != "offsiteother"),(tri['Basis of Estimate'] != "offsiteland"))]


frames = [tri4,tri1,tri2]
tri = pd.concat(frames,axis = 0,ignore_index = False)




#tri[['FlowAmount']] = tri[['FlowAmount']].astype(float)

r = 0;
for row in tri.itertuples():
    r = r+1;
    #print(row[5]=='C')
    #break;
    print(r)
    if(row[5] == 'offsiteother'):    
        
        tri.iloc[r,5] = row[5];
        tri.iloc[r,4] = None;
  #Something is off in the Basis of Estimate
#pd.unique(tri['Basis of Estimate'])
#tri[tri['Basis of Estimate']=='offsiteland']

#Need to find out what row this problem starts in and figure out a workaround
'''