import mariadb
import datetime
from scipy import stats
import numpy as np
import pandas as pd
import time
from sqlalchemy import create_engine
import os
import sys

#import credentials
#csv file has to be stored on local computer in Users/$winloginid$/Documents/ETA/login.csv
#in the form
# field,data
# mysql_user,$user$
# mysql_passwd,$password$
# mysql_IP,$IP$
# mysql_port,$port$
# mysql_db,$name of db$


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
engine = create_engine('mysql+mysqlconnector://' + mysql_user + ':' + mysql_passwd + '@' + mysql_IP +':' + mysql_port + '/' + mysql_db, echo=False,paramstyle="format")

#query the database for underlyings
def Buy_list():
    strsql= """
            SELECT AA.underlying_id,AA.Date, Bloomberg,  TScore, Marc_Rank, Matt_Rank, 5DMove as 5DMoveExSkew FROM
            (
            SELECT BList.underlying_id,BList.Date, BList.Bloomberg, round(RelVal_Rank,2) as Marc_Rank, round(TS,2) as TScore, RelVal_RV as RV, round(Catchup,1) as Ketchup, round(Move,1) as 5DMove, round(ranking,2) as Matt_Rank, MarketCap, LiqClass FROM
            (
            SELECT Date, tt.underlying_id, Bloomberg, RelVal_RV, RelVal_Last, RelVal_Rank, ATM_Last, ATM_Rank, ATM_Move-ATM_OfSkew as Move, AveBT3M-AveIVinBT3M as Catchup, (TScore12M+TScore6M)/2 as TS, MarketCap, LiqClass FROM (SELECT * FROM (WITH latest AS (SELECT c.*, ROW_NUMBER() OVER (PARTITION BY underlying_id ORDER BY Date DESC) AS rnk FROM deathstar.calc as c) SELECT * FROM latest WHERE rnk=1) as t) as tt, deathstar.underlying, deathstar.fundamentals WHERE tt.underlying_id=underlying.underlying_id and tt.underlying_id=fundamentals.underlying_id and (MarketCap>30000 or Sector='Index')  and ATM_Last>10 AND Bloomberg LIKE '% US' AND Sector <>'Index' GROUP BY tt.underlying_id
            ) as BList
            INNER JOIN
            (
            select Bloomberg, AVG(percentile) as ranking from deathstar.rating,deathstar.underlying where rating.underlying_id=underlying.underlying_id and axis1='ATM' and period='12M' and dt_calc BETWEEN DATE_SUB(NOW(), INTERVAL 14 DAY) AND NOW() 
            group by rating.underlying_id
            ) as MattList
            ON BList.Bloomberg=MattList.Bloomberg
            ORDER BY MarketCap DESC
            ) as AA
            WHERE Marc_Rank<0.5 AND TScore<0.5 AND RV<0 AND 5DMove<0 AND LiqClass IN ('US_SS_20_40','US_SS_40_60','US_SS_60_80','US_SS_80_100') ORDER BY TScore ASC
        """
    try:
        return pd.read_sql(strsql, engine)
    except mariadb.Error as e:
        print(f"Error: {e}")

def Sell_list():
    strsql= """
            SELECT AA.Date, AA.underlying_id, Bloomberg, TScore, Marc_Rank, Matt_Rank, 5DMove as 5DMoveExSkew FROM
            (
            SELECT BList.underlying_id, BList.Date, BList.Bloomberg, round(RelVal_Rank,2) as Marc_Rank, round(TS,2) as TScore, RelVal_RV as RV, round(Catchup,1) as Ketchup, round(Move,1) as 5DMove, round(ranking,2) as Matt_Rank, MarketCap, LiqClass FROM
            (
            SELECT Date,tt.underlying_id, Bloomberg, RelVal_RV, RelVal_Last, RelVal_Rank, ATM_Last, ATM_Rank, ATM_Move-ATM_OfSkew as Move, AveBT3M-AveIVinBT3M as Catchup, (TScore12M+TScore6M)/2 as TS, MarketCap, LiqClass FROM (SELECT * FROM (WITH latest AS (SELECT c.*, ROW_NUMBER() OVER (PARTITION BY underlying_id ORDER BY Date DESC) AS rnk FROM deathstar.calc as c) SELECT * FROM latest WHERE rnk=1) as t) as tt, deathstar.underlying, deathstar.fundamentals WHERE tt.underlying_id=underlying.underlying_id and tt.underlying_id=fundamentals.underlying_id and (MarketCap>30000 or Sector='Index')  and ATM_Last>10 AND Bloomberg LIKE '% US' AND Sector <>'Index' GROUP BY tt.underlying_id
            ) as BList
            INNER JOIN
            (
            select Bloomberg, AVG(percentile) as ranking from deathstar.rating,deathstar.underlying where rating.underlying_id=underlying.underlying_id and axis1='ATM' and period='12M' and dt_calc BETWEEN DATE_SUB(NOW(), INTERVAL 14 DAY) AND NOW() 
            group by rating.underlying_id
            ) as MattList
            ON BList.Bloomberg=MattList.Bloomberg
            ORDER BY MarketCap DESC
            ) as AA
            WHERE Marc_Rank>0.5 AND TScore>0.5 AND RV>0 AND 5DMove>0 AND LiqClass IN ('US_SS_20_40','US_SS_40_60','US_SS_60_80','US_SS_80_100') ORDER BY TScore DESC
        """
    try:
        return pd.read_sql(strsql, engine)
    except mariadb.Error as e:
        print(f"Error: {e}")

engine.dispose()

print("Buy:", Buy_list()['Bloomberg'].values.tolist())
print("Sell:", Sell_list()['Bloomberg'].values.tolist())