import pdblp
import mariadb
from datetime import datetime, timedelta
import time
import pandas as pd
from sqlalchemy import create_engine
import os
import sys

creds_file='C:/Users/' + os.getlogin() + '/Documents/ETA/login.csv'
creds = pd.read_csv(creds_file)
creds.set_index('field',inplace=True)
creds=creds.to_dict(orient='dict')

mysql_user=creds['data']['mysql_user']
mysql_passwd=creds['data']['mysql_passwd']
mysql_IP=creds['data']['mysql_IP']
mysql_port=creds['data']['mysql_port']
mysql_db=creds['data']['mysql_db']

time_start=time.time()

#connect to mysql server
condb = mariadb.connect(
  host=mysql_IP,
  user=mysql_user,
  password=mysql_passwd,
  database=mysql_db
)

cur = condb.cursor()

#query the database for underlyings
def undl_list():
    strsql= "SELECT underlying_id FROM data GROUP BY underlying_id"
    try:
        cur.execute(strsql)
        return pd.DataFrame(cur.fetchall(),columns=['Ticker','underlying_id'])
    except mariadb.Error as e:
        print(f"Error: {e}")

#retrieving the list of und
UL=undl_list()

N=[6,13,442,123,606,58,102,18] 

def getthedata(F,days):
    strsql= "SELECT date"
    for i in range(len(N)):
        strsql = strsql + " ,AVG(CASE WHEN underlying_id = " + str(N[i]) +" THEN " + F + " END)"
    strsql = strsql + " FROM data GROUP BY date ORDER BY date DESC LIMIT " + str(days)
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall())
        return df.dropna(how='any',axis=0).values.tolist()
    except mariadb.Error as e:
        print(f"Error: {e}")



condb.close

