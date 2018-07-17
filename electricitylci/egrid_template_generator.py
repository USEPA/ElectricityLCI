#from electricitylci.elci_database_generator import final_elci_database
from electricitylci.egrid_facilities import egrid_subregions
from egrid_databasefile_setup import fuel_name
import os
import openpyxl
from copy import copy, deepcopy


#This function is necessary to creates new line in the template for writing input and output flows. 
def createblnkrow(br,io):
    
    Column_number = 43 #THis is based on the template number of columns in the template. 
    for i in range(1,Column_number):
      
      v = io.cell(row = br,column = i).value
      io.cell(row = br+1 ,column = i).value = v
      io.cell(row = br+1 ,column = i).font = copy(io.cell(row = br,column = i).font)
      io.cell(row = br+1 ,column = i).border = copy(io.cell(row = br,column = i).border)
      io.cell(row = br+1 ,column = i).fill = copy(io.cell(row = br,column = i).fill)
    
    br = br + 1;
      #print(io.cell(row = 6,column = 4).value);
    return br


data_dir = os.path.dirname(os.path.realpath(__file__))   
os.chdir(data_dir+'\\data\\')

for Reg in egrid_subregions: 

    
 for row in fuel_name.itertuples():
#assuming the starting row to fill in the emissions is the 5th row Blank
   
   #Reading complete fuel name and heat content information       
   fuelname = row[2] 
     
   if (fuelname+'_'+Reg) in final_elci_database: 
    global blank_row;    
    blank_row = 6;
    
    row1 = 5;
    
     
    #Opeing tempalte file for wriitng
    wbook = openpyxl.load_workbook('template.xlsx')   
    
   
    
    
    
    
 
    #Read the template files.                         

    gi = wbook['General information']          
    
    
    #This block is used for filling up the General Infomration Sheet      
    gi['D4'].value = 'Electricity'
    
    gi['D5'].value = 'from '+ fuelname;
    
    gi['D6'].value = 'at generating facility';
    
    gi['D10'].value = 'Electricity from '+ fuelname+' using power plants in the '+Reg+' region'
    
    #This function is used for the name of the fuels.
    #def pm1(fn):
    #    for row in primemover.itertuples():
    #       if row[3]  == fn or row[4] == fn or row[5] == fn or row[6] == fn:
    #          if row[4] != None:                   
    #             return row[4].capitalize()
    
    gi['D11'].value = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+str(fuelname)
    
    gi['D12'].value = 'FALSE'
    
    gi['D14'].value = 'Electricity; voltage?'
    
    gi['D16'].value = final_elci_database[fuelname+'_'+Reg]['processDocumentation']['validFrom']
    
    gi['D17'].value = final_elci_database[fuelname+'_'+Reg]['processDocumentation']['validUntil']
    
        
    gi['D18'].value = final_elci_database[fuelname+'_'+Reg]['processDocumentation']['validFrom']
    
    gi['D20'].value = 'US-eGRID-'+Reg

    gi['D21'].value = 'F'
    v= [];
    v1= [];
    
    
  
    
    gi['D24'].value = 'database subregion definitions:<https://www.epa.gov/energy/emissions-generation-resource-integrated-database-database>\nNERC region: '
                          
    gi['D26'].value = 'This is an aggregation of technology types within this eGRID subregion.  List of prime movers:'
    
    


    

    io = wbook['InputsOutputs']

    
    for index in range(0,len(final_elci_database[fuelname+'_'+Reg]['exchanges'])):
        
        #print(final_elci_database[fuelname+'_'+Reg]['exchanges'][index])
    

        blank_row = createblnkrow(blank_row,io)
        
        
        io[row1][0].value = index+1;
        if final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['input'] == True:
          io[row1][1].value = 5
        else: 
          if  index == 0: 
             io[row1][2].value = 0
          else:
             io[row1][2].value = 4
        
        name = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['flow']['name']
        
        #Making the string flow name within limits accepted by OpenLCA. 
        io[row1][3].value  = name[0:255]
        
        if index == 0:
          io[row1][4].value = final_elci_database[fuelname+'_'+Reg]['category']
        else:
          io[row1][4].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['flow']['category']
        io[row1][6].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['amount']
        io[row1][7].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['unit']['name']
        io[row1][21].value = 'database data with plants over 10% efficiency';
        
        if 'uncertainty' in final_elci_database[fuelname+'_'+Reg]['exchanges'][index]:
           io[row1][26].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty']['distributionType']
           if 'geomMean' in final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty'] and 'geomSd' in final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty']: 
            #uncertianty calculations
            io[row1][27].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty']['geomMean']
            io[row1][28].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty']['geomSd']
           io[row1][29].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty']['maximum']
           io[row1][30].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['uncertainty']['minimum']
        if 'comment' in final_elci_database[fuelname+'_'+Reg]['exchanges'][index]:
            io[row1][33].value = final_elci_database[fuelname+'_'+Reg]['exchanges'][index]['comment']

        
        row1 = blank_row-1
        
        
            


             
        
         
            
    #Writing final file. 
    
    filename = fuelname+"_"+Reg+".xlsx"
    data_dir = os.path.dirname(os.path.realpath(__file__))+"\\output\\"
    wbook.save(data_dir+filename)
    print(filename+' File written Successfully')
            
    '''
        
    
    
    #Fillin up with reference only only
    io['A5'].value = 1
    io['C5'].value =  0;
    io['D5'].value =  str(names.iloc[0,0])+' '+fuelname
    #io['E5'].value = '22: Utilities/2211: Electric Power Generation, Transmission and Distribution/'+str(fuelname)
    io['F5'].value = str(Reg)
    io['G5'].value =  1 
    io['H5'].value =  'MWh'
    row1 = blank_row;
    
    blank_row = createblnkrow(blank_row,io)
    
    #This part is used for writing the input fuel flow informationn
    if database_f1['HeatInput'].mean() != 0 and fuelheat != 0:
    #Fillin up the fuel flow
        io[row1][0].value = index;
        io[row1][1].value = 5;
        io[row1][3].value = fuelname;
        io[row1][4].value = 'Input Fuel'
        if str(fuelheat)!='nan':
          io[row1][6].value = (np.sum(database_f1['HeatInput'])/np.sum(database_f1['NetGen']))/fuelheat;
          io[row1][7].value = 'kg';
        else:
          io[row1][6].value = (np.sum(database_f1['HeatInput'])/np.sum(database_f1['NetGen']));
          io[row1][7].value = 'MJ';  
        
        io[row1][21].value = 'database data with plants over 10% efficiency';
        io[row1][26].value = 'LogNormal';
        temp_data = database_f1[['NetGen','HeatInput']]
        #uncertianty calculations only if database length is more than 3
        l,b = temp_data.shape
        if l > 3:
           u,s = uncertainty(temp_data)
           if str(fuelheat)!='nan':
             io[row1][27].value = str(round(math.exp(u),3)/fuelheat);
           else:
             io[row1][27].value = str(round(math.exp(u),3));  
            
           io[row1][28].value = str(round(math.exp(s),3));                        
        
        
        io[row1][29].value = database_f1['HeatInput'].min();
        io[row1][30].value = database_f1['HeatInput'].max();
        io[row1][33].value = 'eGRID'+str(year);
        row1 = blank_row;
        index = index + 1;
        
    (blank_row,row1,index) = flowwriter_infrastructure(io,row1,index,blank_row)

    #This part is used for filling up the emission information from the different databases. 
    for x in d_list:
        
        if x == 'eGRID': 
            database_f3 = database_f1[['NetGen','Carbon dioxide', 'Nitrous oxide', 'Nitrogen oxides', 'Sulfur dioxide', 'Methane']]
            (blank_row,row1,index) = flowwriter(database_f3,io,row1,index,blank_row,'air',x)
            
        elif x != 'eGRID':  
            database_f3 = tri_func(database_f1,l_limit,u_limit);
            
            #This is for extracing only the database being considered fro the d list names. 
            if x == 'TRI':                     
                database_f3 = database_f3[database_f3['Source']=='TRI']
           
            elif x == 'NEI':
                database_f3 = database_f3[database_f3['Source']=='NEI']
           
            elif x == 'RCRAInfo':
                database_f3 = database_f3[database_f3['Source']=='RCRAInfo']
            
            #CHecking if its not empty and differentiating intp the different Compartments that are possible, air , water soil and wastes. 
            if database_f3.empty != True:
                                      
                #water
                d1 = database_f3[database_f3['Compartment']=='air']
                d1 = d1.drop(columns = ['Compartment','Source'])
                
                if d1.empty != True:
                  
                  (blank_row,row1,index) = flowwriter(d1,io,row1,index,blank_row,'air',x)                            
                
                
                #water
                d1 = database_f3[database_f3['Compartment']=='water']
                d1 = d1.drop(columns = ['Compartment','Source'])
                
                if d1.empty != True:
                  (blank_row,row1,index) = flowwriter(d1,io,row1,index,blank_row,'water',x)
                
                #soil
                d1 = database_f3[database_f3['Compartment']=='soil']
                d1 = d1.drop(columns = ['Compartment','Source'])
                
                if d1.empty != True:
                  (blank_row,row1,index) = flowwriter(d1,io,row1,index,blank_row,'soil',x)
                
                                        
                #waste
                d1 = database_f3[database_f3['Compartment']=='waste']
                d1 = d1.drop(columns = ['Compartment','Source'])
                
                if d1.empty != True:
                  (blank_row,row1,index) = flowwriter(d1,io,row1,index,blank_row,'waste',x)            
def flowwriter_infrastructure(io,row1,index,blank_row):
    global infrastructure;
    global year;
    global fuelname
    
    for i in infrastructure.iteritems():
        if i[0] != 'Description' and i[0] != 'Fuel':
            blank_row = createblnkrow(blank_row,io)
            io[row1][0].value = index;
            io[row1][1].value = 5;
            io[row1][3].value = i[0];
            io[row1][4].value = 'Category Input Flows'
            for j in infrastructure.itertuples():
                
                if j[2] == fuelname:

                 io[row1][6].value =j[infrastructure.columns.get_loc(i[0])+1]

            io[row1][7].value = 'kg';
            io[row1][21].value = 'database data with plants over 10% efficiency';
            io[row1][26].value = 'LogNormal';
            
                            
            io[row1][33].value = str(year);
                 
            
            row1 = row1+1;
            index = index+1;
            
    return (blank_row,row1,index)



def flowwriter(database_f1,io,row1,index,blank_row,comp,y):
    
    global year;
    global odd_year;
    global odd_database;
    
    for i in database_f1.iteritems():
      
      #Only writng the emissions. NOt any other flows or columns in the template files.   
      if str(i[0]) != 'NetGen' and str(i[0]) != 'ReliabilityScore': 
        database_f2 = database_f1[['NetGen',i[0]]] 
            
        if(compilation(database_f2) != 0 and compilation(database_f2)!= None):
            
            blank_row = createblnkrow(blank_row,io)
            io[row1][0].value = index;
            io[row1][2].value = 4;
            io[row1][3].value = i[0];
            io[row1][4].value = 'Elementary Flows/'+str(comp)+'/unspecified'
            
            io[row1][6].value = compilation(database_f2);
            io[row1][7].value = 'kg';
            io[row1][21].value = 'database data with plants over 10% efficiency';
            io[row1][26].value = 'LogNormal';
            
            #uncertianty calculations
            l,b = database_f2.shape
            if l > 3:
               u,s = (uncertainty(database_f2))
               io[row1][27].value = str(round(math.exp(u),9));
               io[row1][28].value = str(round(math.exp(s),3));
            io[row1][29].value = database_f2[i[0]].min();
            io[row1][30].value = database_f2[i[0]].max();
            
            if y == odd_database:
                 
                 io[row1][33].value = y+' '+str(odd_year);
                 
            else: 
                 
                 io[row1][33].value = y+' '+str(year);
                 
            
            row1 = row1+1;
            index = index+1;
            
    return (blank_row,row1,index)
'''
