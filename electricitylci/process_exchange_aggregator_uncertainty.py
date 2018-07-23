#Uncertainty calculations based on Log Normal distribution 
#Comopilation based on weight factor

import math
import numpy as np
from scipy.stats import t
from sympy import var,solve
import pandas as pd



#TRoy weight based method to compute emissions factors.  Rename calculation of emission factors. descriptive of the varaible.  
def compilation(db):
        #Troy Method
        #Creating copy of database by substitution the NA emissions with zero
        db1 = db.fillna(value = 0)
        
        #Removing all rows here emissions are not reported for second dataframe
        db2 = db.dropna()
        
        
        #keeping the unreported emissions and facilities in separate database

        #This check is to make sure that the second database is not empt after droppins all NA. if empty, then we only use first database.  
        if db2.empty == True:
            ef1 = np.sum(db1.iloc[:,1])/np.sum(db1.iloc[:,0])
            return ef1
    
        ef1 = np.sum(db1.iloc[:,1])/np.sum(db1.iloc[:,0])
        
        ef2 = np.sum(db2.iloc[:,1])/np.sum(db2.iloc[:,0])
        
        #weight formula.
        weight = np.sum(db2.iloc[:,0])/np.sum(db1.iloc[:,0])
        final_ef = ef2*weight + (1-weight)*ef1
        
        return final_ef


#This is the function for calculating log normal distribution parameters. 
def uncertainty(db):
        #Troy Method
        #Creating copy of database by substitution the NA emissions with zero
        db1 = db.fillna(value = 0)
        
        #Removing all rows here emissions are not reported for second dataframe
        db2 = db.dropna()
        frames = [db1,db2]
        #Here we doubled up the database by combining two databases together
        data = pd.concat(frames,axis = 0)
        
        
        mean = np.mean(data.iloc[:,1])
        l,b = data.shape
        sd = np.std(data.iloc[:,1])/np.sqrt(l)
        mean_gen = np.mean(data.iloc[:,0])
        #obtaining the emissions factor from the weight based method
        ef = compilation(db)
        
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
              sd1,sd2 = solve(0.5*x*x -(1.16308*np.sqrt(2))*x + (np.log(1+pi3)),x)

            else:#This is a wrong mathematical statement. However, we have to use it if something fails. 
              sd1,sd2 = solve(0.5*x*x -(1.36*np.sqrt(2))*x + (np.log(1+pi3)),x)
              print(pi3)
            
            #always choose lower standard deviation from solving the square root equation. 
            if sd1 < sd2:
               log_mean = np.log(ef)-0.5*(sd1**2)
               return round(log_mean,2),round(sd1,2)
            else:
               log_mean = np.log(ef)-0.5*(sd2**2)
               return round(log_mean,2),round(sd2,2)