import sys
from sys import argv
import pyodbc
import requests
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import urllib
params = urllib.parse.quote_plus(r'Driver={SQL Server};''SERVER=localhost;''DATABASE=test;'' UID = sa;'' PWD = Admin1@;' 'Trusted_Connection=yes')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = create_engine(conn_str)
dbcon = pyodbc.connect('Driver={SQL Server};''SERVER=localhost;''DATABASE=test;'' UID = sa;'' PWD = Admin1@;' 'Trusted_Connection=yes')
def getApidata():
    import io
    url = r'http://www.tennet.org/english/operational_management/export_data.aspx?exporttype=bidpriceladder&format=csv&datefrom=2020-01-01&dateto=2020-01-02&submit=1'
    r = requests.get(url)
    df = pd.read_csv(io.StringIO(r.text))
    currentdate = dt.datetime.now()
    df['LoadDate'] = currentdate 
    df.to_csv(r'C:\\Users\\rahul\\Desktop\\data.csv', index = False)
    x=df.pivot_table(index='Date', columns='PTE', values='total_plus')
    x.to_csv(r'C:\\Users\\rahul\\Desktop\\pivotdata.csv', index = True)
    df.to_sql(name='Apidata',con=engine, if_exists='append',index=False)
   
def getData(dbcon):
    print("Read data from the table")
    cursor = dbcon.cursor();
    cursor.execute('select * from dbo.Apidata')
    for row in cursor:
        print(f'{row}')
     
def insertData():
    print("Insert sample data into the table")
    data={'Date':['01/01/2020'],'PTE':['10000'],'period_from':['00:00'],'period_until':['00:15'],'Total_min':['-753'],'Minmax':['-422.35'],'Min600':['-109.06'],'Min300':['0.0'],'Min100':['23.49'],'minmin':['30.02'],'posmin':['0.0'],'pos100':['31.06'],'pos300':['140.02'],'pos600':['00.0'],'posmax':['341.88'],'total_plus':['383']} 
    df1=pd.DataFrame(data)
    currentdate = dt.datetime.now()
    df1['LoadDate'] = currentdate
    df1.to_sql(name='Apidata',con=engine, if_exists='append',index=False)
    
def updateData(dbcon):
    print("Update the table")
    cursor = dbcon.cursor();
    cursor.execute(
        'update [dbo].[Apidata] set total_plus = ? where PTE = ?',
        ('1001',1))
    dbcon.commit() 
    
def deleteData(dbcon):
    print("Delete data from the table")
    cursor = dbcon.cursor();
    cursor.execute(
        'delete from [dbo].[Apidata] where PTE = ?',
        (2))
    dbcon.commit() 
    
#Get data from API, append date column and load to database and load the pivot into a sample file
getApidata()
#Retrive data from database
getData(dbcon)
#Insert data into database
insertData()
#Update data into database
updateData(dbcon)
#Delete data from database
deleteData(dbcon)
