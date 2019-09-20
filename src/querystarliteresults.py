#!/usr/bin/env python
# coding: utf-8

# In[35]:


import pandas as pd
import os, json
import re


# In[36]:


#Sample Query in string format
userName =input("Please login with your email address: ")


# devuser2@beyondbanking.nl
# select Month,DayofMonth,CRSDepTime,Origin,Dest from flights where year=2008
#  devuser4@beyondbanking.nl

# In[71]:


userQuery =input("please submit your query: ")


# In[72]:


userQuery =userQuery.lower()


# In[73]:


path_to_json = '<Path to Json>'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if   pos_json.endswith('.json')]


# In[74]:


def json_parsing(tableName,userID):
    policyID =[]
    columns =[]
    policyFilter =[]
    for index, js in enumerate(json_files):
        with open(os.path.join(path_to_json, js)) as json_file:
            df_json = pd.read_json(json_file)
        
            #print(df_json)
            indexNamesArr = df_json.index.values
            listOfRowIndexLabels = list(indexNamesArr)
            try:
                allowedDataset =df_json.iloc[listOfRowIndexLabels.index('alloweddataset')].values[0]
                allowedUser =df_json.loc["assigned_to"][0]

                if (str.lower(allowedDataset)==tableName and (userID  in allowedUser)  ):
                    try:
                        
                        allowedColumns =df_json.loc["allowedcolumns"]
                        allowedFilter =df_json.loc["rowfilters"]
                        allowedPolicy =df_json.loc["id"]
                    except:
                        allowedColumns =df_json.loc["allowedcolumns"]
                        allowedFilter =[]
                        allowedPolicy =df_json.loc["id"]
                        
                    
                    policyID = allowedPolicy[0]
                    columns = allowedColumns[0]
                    policyFilter =allowedFilter[0]
                
            except:
                pass
                
        
    return policyID,columns,policyFilter  
                
            


# In[75]:


#Coulmn,Tablename and filtercondition from SQL query
def queryParser(userQuery,userName):
    columnsQuery = re.search(r'select(.*?)from', userQuery).group(1).strip()
    columnsQuery=columnsQuery.split (",")
    print(columnsQuery)
    userFilterQuery =re.search(r'\where\s+(.*)', userQuery).group(1)
    print(userFilterQuery)
    table =re.search(r'from(.*?)where', userQuery).group(1)
    table =table.strip()
    userName=userName
    policyID,columns,policyFilter =json_parsing(table,userName)
    columns=[x.lower() for x in columns]
    if columnsQuery[0] =='*':
        intersectionColumns=columns
    else:
        intersectionColumns = [i for i in columnsQuery if i in columns]
    return policyID,intersectionColumns,policyFilter,userFilterQuery,table


# In[76]:


policyID,intersectionColumns,policyFilter,userFilterQuery,table =queryParser(userQuery,userName)
str1 = ','.join(str(e) for e in intersectionColumns)
if(policyFilter !=[]):
    new_filter=[k+'='+"'"+v+"'" for k,v in policyFilter[0].items()]
   #table = "OPENROWSET (BULK 'https://<name of the storage account>.dfs.core.windows.net/<containername>/<file>', FORMAT = 'CSV', FIELDTERMINATOR =',', ROWTERMINATOR = '0x0a', FIRSTROW = 2) WITH ([year] smallint,[month] smallint,[dayofmonth] smallint,[DayOfWeek] smallint,[DepTime] smallint ,[CRSDepTime] smallint,[ArrTime] smallint,[CRSArrTime] smallint,[UniqueCarrier] VARCHAR (100) COLLATE Latin1_General_BIN2,[FlightNum] smallint,[TailNum] VARCHAR (100) COLLATE Latin1_General_BIN2,[ActualElapsedTime] smallint,[CRSElapsedTime] smallint,[AirTime] smallint,[ArrDelay] smallint,[DepDelay] smallint,[Origin] VARCHAR (100) COLLATE Latin1_General_BIN2,[Dest] VARCHAR (100) COLLATE Latin1_General_BIN2,[Distance] smallint,[TaxiIn] smallint,[TaxiOut] smallint,[Cancelled] smallint,[CancellationCode] smallint,[Diverted] smallint,[CarrierDelay] VARCHAR (100) COLLATE Latin1_General_BIN2, [WeatherDelay] VARCHAR (100) COLLATE Latin1_General_BIN2,[NASDelay] VARCHAR (100) COLLATE Latin1_General_BIN2,[SecurityDelay] VARCHAR (100) COLLATE Latin1_General_BIN2,[LateAircraftDelay] VARCHAR (100) COLLATE Latin1_General_BIN2) AS [r]"
    table = "OPENROWSET (BULK 'https://<name of the storage account>.dfs.core.windows.net/<containername>/<file>', FORMAT = 'CSV', FIELDTERMINATOR =',', ROWTERMINATOR = '0x0a', FIRSTROW = 2) WITH (<schema>) AS [r]"
    newQuery ='select '+str1+ ' from ' + str(table)+' where '+str(userFilterQuery)+' and '+str(new_filter[0])
else:
    newQuery ='select '+str1+ ' from ' + str(table)+' where '+str(userFilterQuery)

    

#print(newQuery)


# In[ ]:


import pyodbc
server = '<Enter Startlite Server Name>'
database = '<Enter Database Name>'
username = '<User Name>'
password = '<Password>'
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
cursor.execute(newQuery)
row = cursor.fetchone()
result="\n\n"+str1+"\n"
while row:
    for column in row:
        result+=str(column)+"    "
    print (result)
    result=""
    row = cursor.fetchone()

