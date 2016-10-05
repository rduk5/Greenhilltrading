# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 15:00:40 2016
 
@author: Richard
"""
 
import pandas as pd
import quandl as qd
import datetime
import math

#API Key
API_Key='8xwJvL4fmwEPxBcjyqvt'
 
#Needed functions, Loader files and hard coded dictionaries
def stringcon(x):
    return str(x)
     
FutMonthCodes={'F':'January','G':'February','H':'March','J':'April','K':'May','M':'June','N':'July','Q':'August','U':'September','V':'October','X':'November','Z':'December'}
MonthDict={'January':1,'February':2,'March':3,'April':4,'May':5,'June':6,'July':7,'August':8,'September':9,'October':10,'November':11,'December':12}
databaselist='C:\FuturesDatabaseCodes.csv'
databases=pd.read_csv(databaselist)
 
CodesList='C:\Codes.csv'
Codes=pd.read_csv(CodesList)
 
Codes2=Codes
Codes=pd.read_csv(CodesList)
 
Codes2['Name']=Codes2['Name'].apply(lambda x: x.replace(' ','_'))
Codes2['comb'] = Codes2[['Symbol', 'Name']].apply(tuple, axis=1)
del Codes2['Symbol']
del Codes2['Name']
CodesDict=dict(Codes2['comb'].tolist())
CodesDictRev = {v: k for k, v in CodesDict.items()}
 
databases['Com']=databases['Codes'].apply(lambda x: pd.Series(x.split('_')))[1]
databases['Exch']=databases['Codes'].apply(lambda x: pd.Series(x.split('/')))[1].apply(lambda x: pd.Series(x.split('_')))[0]
databases['Year']=databases['Codes'].apply(lambda x: (x[-4:]))
databases['Com']=databases['Com'].apply(lambda x: x[:len(x)-4])
databases['Sym']=databases['Com'].apply(lambda x: x[:len(x)-1])
databases['Con']=databases['Com'].apply(lambda x: x[-1:])
databases['Expiry']=(databases['Con'].apply(lambda x: FutMonthCodes.get(x))+ ' ' + databases['Year'].apply(lambda x:x)).apply(lambda x: datetime.datetime.strptime(x,'%B %Y'))
databases['ExpMonth']=databases['Con'].apply(lambda x: FutMonthCodes.get(x))
databases['GHT Name']=databases['Sym'].apply(lambda x: CodesDict.get(x))+'_' +databases['Con'].apply(lambda x: FutMonthCodes.get(x))+ '_' +databases['Year'].apply(lambda x:x)
del databases['Com']
 
def ReturnData(Commodity,ColFetch,FilterList,ColFilt,Thresh,DayList,MonthList,YearList,databases,API_Key):
    import quandl as qd
    import pandas as pd
    import datetime
    
    if DayList=='All':
        DayList=list(range(1,32))
    
    if MonthList=='All':
        MonthList=list(range(1,13))
    
    if FilterList=='All':
        FilterList=['January','February','March','April','May','June','July','August','September','October','November','December']
    
    #Extracts the Commodity, Sorts based on Expiry and resets the index
    data=databases[databases['Sym']==CodesDictRev.get(Commodity)]
    data=data.reset_index(drop=True)
    data=data.sort_values(['Expiry'],ascending=True)
    data=data.reset_index(drop=True)
    #print(data)
    #Filters by Expiry Month (if the user has chosen it)    
    data=data[data['ExpMonth'].isin(FilterList)]
    data=data.reset_index(drop=True)
    #Frequency Analysis
    outputDF=[]
    for i in range(0,len(data)-1):
        #Downloads table by table
        #print(i)
        temptable=qd.get(data.get_value(i,'Codes'),authtoken=API_Key)
        #Filters by Column if specified        
        temptable=temptable[temptable[ColFilt]>Thresh]
        #print(temptable)
        #Checks the columns we have in the table returned
        availablecol=[x  for x in map(stringcon,list(temptable.columns))]
        #print(temptable)
        #Creates the list of columns we need to drop i.e. not in ColFetch
        dropcolumnslist=list(set(availablecol)-(set(ColFetch)&set(availablecol)))
        #print(dropcolumnslist)
        #Drops these columns from the table
        temptable=temptable.drop(dropcolumnslist,axis=1)
     
        if i==0 and YearList=='All':
            YearList=list(range(temptable.index[0].year,datetime.datetime.now().year+1))
        
        #Filters based on chosen day/month/year
        temptable['ObsDay']=temptable.index.map(lambda x:x.day)
        temptable['ObsMonth']=temptable.index.map(lambda x:x.month)
        temptable['ObsYear']=temptable.index.map(lambda x:x.year)
        #print(temptable)
        temptable=temptable[temptable['ObsYear'].isin(YearList)]
        temptable=temptable[temptable['ObsMonth'].isin(MonthList)]
        temptable=temptable[temptable['ObsDay'].isin(DayList)]
        del temptable['ObsYear']
        del temptable['ObsMonth']
        del temptable['ObsDay']
        #Adds the name of the contract to the column names
        dataColumnsadj=[data['GHT Name'][i]+ ' ' + x  for x in map(stringcon,list(temptable.columns))]
        temptable.columns=dataColumnsadj        
       
        if i==0:
            outputDF=temptable
        else:
            outputDF=pd.concat([outputDF,temptable],axis=1)
         
    return outputDF

     
 
#a=ReturnData('CBOT_Corn',['Settle','Volume'],['March','September'],'Volume',0,[16,15,14,13],[6,7,8,9],[1959,1960,1961,1962,1963,1964,1965],databases,API_Key)
#b=ReturnData('CBOT_Corn',['Settle'],['March','September'],'Volume',0,'All','All','All',databases,API_Key)
#c=ReturnData('CBOT_Corn',['Settle'],['March','September'],'Volume',0,[16],'All','All',databases,API_Key) 
AllCornSettleData=ReturnData('CBOT_Corn',['Settle'],'All','Volume',0,'All','All','All',databases,API_Key)
AllCornVolumeData=ReturnData('CBOT_Corn',['Volume'],'All','Volume',0,'All','All','All',databases,API_Key)

#Rolling 20 day volume (used as a filter)
AllCornVolumeData2=AllCornVolumeData.rolling(window=20,center=False).mean()
#Getting the months (as a number) of the expiries and calculating the Roll dates
availablecol=[x for x in map(stringcon,list(AllCornSettleData.columns))]
temp1=pd.DataFrame(availablecol)
temp1['Month']=temp1[0].apply(lambda x: pd.Series(x.split(' ')))[0]
temp1['Month']=temp1['Month'].apply(lambda x: pd.Series(x.split('_')))[2]+' '+temp1['Month'].apply(lambda x: pd.Series(x.split('_')))[3]
temp1['Month']=temp1['Month'].apply(lambda x: datetime.datetime.strptime(x,'%B %Y'))
temp1=temp1[temp1[0].str.contains("Volume")==False]
temp1.columns=['Contract','Roll_Date']
from pandas.tseries.offsets import *
temp1['Roll_Date']=temp1['Roll_Date'].apply(lambda x: x+DateOffset(months=-1))
temp1['Roll_Date']=temp1['Roll_Date'].apply(lambda x: x.replace(day=16))
rolldict=temp1.set_index('Contract')['Roll_Date'].to_dict()

#Testing only
AllCornSettleDatatest=AllCornSettleData.copy()

#The key line, it works, this is the capped roll date values
AllCornSettleDatatest=AllCornSettleDatatest.apply(lambda x: x[x.index<=rolldict.get((x.name))],axis=0)

#Builds the active contract column based on rolling 20day volume, NEED TO MIX THIS WITH THE ROLL DATE CONSIDERATIONS
AllCornVolumeData2['Active_Contract']=AllCornVolumeData2.idxmax(axis=1)
AC=pd.DataFrame(AllCornVolumeData2['Active_Contract'],AllCornVolumeData2.index)
AC=AC.dropna()
AC['Active_Contract']=AC['Active_Contract'].apply(lambda x: pd.Series(x.split(' ')))[0]+' Settle'
rollcontdict=AC.set_index(AC.index)['Active_Contract'].to_dict()

AllCornSettleDatatest['ContSeries']=AllCornSettleDatatest.apply(lambda : x[str(rollcontdict.get(x.index))],axis=0)


class FuturesCurve:
    def __init_class__(self,Curve):
        self.Curve=Curve
    
    def Prices():
        
    def NumContracts():
        
    def Spreads():
        















df['Days']=(df['index']-df['index'].shift(1)).astype('timedelta64[D]')
















temptable1=qd.get(data.get_value(0,'Codes'),authtoken=API_Key)
#Filters by Column if specified        
temptable1=temptable1[temptable1[ColFilt]>Thresh]
#print(temptable)
#Checks the columns we have in the table returned
availablecol=[x  for x in map(stringcon,list(temptable1.columns))]
#print(temptable)
#Creates the list of columns we need to drop i.e. not in ColFetch
dropcolumnslist=list(set(availablecol)-(set(ColFetch)&set(availablecol)))
#print(dropcolumnslist)
#Drops these columns from the table
temptable1=temptable1.drop(dropcolumnslist,axis=1)

 
YearList=list(range(temptable1.index[0].year,datetime.datetime.now().year+1))
   
#Filters based on chosen day/month/year
temptable1['ObsDay']=temptable1.index.map(lambda x:x.day)
temptable1['ObsMonth']=temptable1.index.map(lambda x:x.month)
temptable1['ObsYear']=temptable1.index.map(lambda x:x.year)
#print(temptable)
temptable1=temptable1[temptable1['ObsYear'].isin(YearList)]
temptable1=temptable1[temptable1['ObsMonth'].isin(MonthList)]
temptable1=temptable1[temptable1['ObsDay'].isin(DayList)]
del temptable1['ObsYear']
del temptable1['ObsMonth']
del temptable1['ObsDay']
#Adds the name of the contract to the column names
dataColumnsadj=[data['GHT Name'][0]+ ' ' + x  for x in map(stringcon,list(temptable1.columns))]
temptable1.columns=dataColumnsadj





temptable2=qd.get(data.get_value(1,'Codes'),authtoken=API_Key)
#Filters by Column if specified        
temptable2=temptable2[temptable2[ColFilt]>Thresh]
#print(temptable)
#Checks the columns we have in the table returned
availablecol=[x  for x in map(stringcon,list(temptable2.columns))]
#print(temptable)
#Creates the list of columns we need to drop i.e. not in ColFetch
dropcolumnslist=list(set(availablecol)-(set(ColFetch)&set(availablecol)))
#print(dropcolumnslist)
#Drops these columns from the table
temptable2=temptable2.drop(dropcolumnslist,axis=1)


YearList=list(range(temptable2.index[1].year,datetime.datetime.now().year+1))

#Filters based on chosen day/month/year
temptable2['ObsDay']=temptable2.index.map(lambda x:x.day)
temptable2['ObsMonth']=temptable2.index.map(lambda x:x.month)
temptable2['ObsYear']=temptable2.index.map(lambda x:x.year)
#print(temptable)
temptable2=temptable2[temptable2['ObsYear'].isin(YearList)]
temptable2=temptable2[temptable2['ObsMonth'].isin(MonthList)]
temptable2=temptable2[temptable2['ObsDay'].isin(DayList)]
del temptable2['ObsYear']
del temptable2['ObsMonth']
del temptable2['ObsDay']
#Adds the name of the contract to the column names
dataColumnsadj=[data['GHT Name'][1]+ ' ' + x  for x in map(stringcon,list(temptable2.columns))]
temptable2.columns=dataColumnsadj




    Corndatabases=databases[databases['Sym']=='C']
    Corndatabases=Corndatabases.reset_index(drop=True)
    Corndatabases=Corndatabases.sort_values(['Expiry'],ascending=True)
    Corndatabases=Corndatabases.reset_index(drop=True)
     
    Corndata1=qd.get(Corndatabases['Codes'][0],authtoken=API_Key)
    Corndata1Columnsadj=[Corndatabases['GHT Name'][0]+ ' ' + x  for x in map(stringcon,list(Corndata1.columns))]
    Corndata1.columns=Corndata1Columnsadj
     
    Corndata2=qd.get(Corndatabases['Codes'][1],authtoken=API_Key)
    Corndata2Columnsadj=[Corndatabases['GHT Name'][1]+ ' ' +x  for x in map(stringcon,list(Corndata2.columns))]
    Corndata2.columns=Corndata2Columnsadj
 
 
 
 
def GetDatabase(name,code,API_Key):
    name=qd.get(code,authtoken=API_Key)
    return name