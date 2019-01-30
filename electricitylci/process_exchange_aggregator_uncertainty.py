#Uncertainty calculations based on Log Normal distribution 
#Comopilation based on weight factor

import math
import sys
import numpy as np
from scipy.stats import t
from sympy import var,solve
import pandas as pd



#TRoy weight based method to compute emissions factors.  Rename calculation of emission factors. descriptive of the varaible.  
def compilation(db,total_gen):
        #Troy Method
        #Creating copy of database by substitution the NA emissions with zero
        db1 = db.fillna(value = 0)
        
        #Removing all rows here emissions are not reported for second dataframe
        db2 = db.dropna()
        
        
        #keeping the unreported emissions and facilities in separate database

        #This check is to make sure that the second database is not empt after droppins all NA. if empty, then we only use first database.  
        if db2.empty == True:
            ef1 = np.sum(db1.iloc[:,1])/total_gen
            return ef1
    
        ef1 = np.sum(db1.iloc[:,1])/total_gen
        
        ef2 = np.sum(db2.iloc[:,1])/total_gen
        
        #weight formula.
        weight = total_gen/total_gen
        final_ef = ef2*weight + (1-weight)*ef1
        
        return final_ef


#This is the function for calculating log normal distribution parameters. 
def uncertainty(db,mean_gen,total_gen,total_facility_considered):
        #Troy Method
        #Creating copy of database by substitution the NA emissions with zero
        #db1 = db.fillna(value = 0)
        
        #Removing all rows here emissions are not reported for second dataframe
        #db2 = db.dropna()
        #frames = [db1,db2]
        #Here we doubled up the database by combining two databases together
        #data_1 = pd.concat(frames,axis = 0)
        data_1 = db
        
        df2 = pd.DataFrame([[0, 0]],columns = ['Electricity','FlowAmount'])
        
        for i in range(len(data_1),total_facility_considered):
            
            data = data_1.append(df2,ignore_index = True)
            data_1 = data
            
        
        data = data_1
        mean = np.mean(data.iloc[:,1])
        l,b = data.shape
        sd = np.std(data.iloc[:,1])/np.sqrt(l)
        #mean_gen = np.mean(data.iloc[:,0])
        #obtaining the emissions factor from the weight based method
        ef = compilation(db,total_gen)
        
        #Endpoints of the range that contains alpha percent of the distribution
        pi1,pi2 = t.interval(alpha = 0.90,df = l-2, loc = mean, scale = sd)
        #Converting prediction interval to emission factors
        pi2 = pi2/mean_gen
        pi1 = pi1/mean_gen
        pi3 = (pi2-ef)/ef;
        x = var('x')
        
        
    
        
        if math.isnan(pi3) == True:
            return None,None;
        
        elif math.isnan(pi3) == False:
            
            #This method will not work with the interval limits are more than 280% of the mean. 
            if pi3 < 2.8:
                # sd1,sd2 = solve(0.5*x*x -(1.16308*np.sqrt(2))*x + (np.log(1+pi3)),x)
              
                a = 0.5
                b = -(1.16308*np.sqrt(2))
                c = np.log(1+pi3)
                sd1 = (-b + np.sqrt(b**2 - (4 * a * c))) / (2 * a)
                sd2 = (-b - np.sqrt(b**2 - (4 * a * c))) / (2 * a)

            else:#This is a wrong mathematical statement. However, we have to use it if something fails. 
              sd1,sd2 = solve(0.5*x*x -(1.36*np.sqrt(2))*x + (np.log(1+pi3)),x)
           
            #if type(sd1) != float or type(sd2) != float:
            #   return 0,0
            #always choose lower standard deviation from solving the square root equation. 
            if sd1 < sd2:
               log_mean = np.log(ef)-0.5*(sd1**2)
               return round(log_mean,12),round(sd1,12)
            else:
               log_mean = np.log(ef)-0.5*(sd2**2)
               return round(log_mean,12),round(sd2,12)
           
            
            
            
def max_min(db,mean_gen,total_gen,total_facility_considered):
        #Troy Method
        #Creating copy of database by substitution the NA emissions with zero
        #db1 = db.fillna(value = 0)
        
        #Removing all rows here emissions are not reported for second dataframe
        #db2 = db.dropna()
        #frames = [db1,db2]
        #Here we doubled up the database by combining two databases together
        #data_1 = pd.concat(frames,axis = 0)
        
        #Filling up the electriity with a random value so that the 0 EF plants can be obtained and calculated. 
        #We need to find the max and minimum of the plant wise emissions factors. 
        
        #df2 = pd.DataFrame([[0.1, 0]],columns = ['Electricity','FlowAmount'])
        
        #for i in range(len(data_1),total_facility_considered):
            
         #   data = data_1.append(df2,ignore_index = True)
         #  data_1 = data
            
        data = db
        maximum = (data.iloc[:,1]/data.iloc[:,0]).max();
        minimum = (data.iloc[:,1]/data.iloc[:,0]).min();
        
        return minimum,maximum
