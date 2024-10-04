import pdblp
import mariadb
from datetime import datetime, timedelta
import time
import pandas as pd
from sqlalchemy import create_engine
import os
import sys

# import credentials
# csv file has to be stored on local computer in Users/$winloginid$/Documents/ETA/login.csv
# in the form
# field,data
# mysql_user,$user$
# mysql_passwd,$password$
# mysql_IP,$IP$
# mysql_port,$port$
# mysql_db,$name of db$


creds_file = 'C:/Users/' + os.getlogin() + '/Documents/ETA/login_ds.csv'
creds = pd.read_csv(creds_file)
creds.set_index('field', inplace=True)
creds = creds.to_dict(orient='dict')

mysql_user = creds['data']['mysql_user']
mysql_passwd = creds['data']['mysql_passwd']
mysql_IP = creds['data']['mysql_IP']
mysql_port = creds['data']['mysql_port']
mysql_db = creds['data']['mysql_db']

time_start = time.time()

# connect to mysql server
condb = mariadb.connect(
  host=mysql_IP,
  user=mysql_user,
  port=int(mysql_port),
  password=mysql_passwd,
  database=mysql_db
)

cur = condb.cursor()

# dict of all the fields with bbg correspondence

fields = {'underlying_id': 'underlying_id',
        'date': 'Date',
        'ticker': 'Ticker',
        '12MTH_IMPVOL_100.0%MNY_DF': 'IV12M100',
        '12MTH_IMPVOL_102.5%MNY_DF': 'IV12M102HALF',
        '12MTH_IMPVOL_110.0%MNY_DF': 'IV12M110',
        '12MTH_IMPVOL_90.0%MNY_DF': 'IV12M90',
        '12MTH_IMPVOL_97.5%MNY_DF': 'IV12M97HALF',      
        '24MTH_IMPVOL_100.0%MNY_DF': 'IV24M100',
        '24MTH_IMPVOL_102.5%MNY_DF': 'IV24M102HALF',
        '24MTH_IMPVOL_110.0%MNY_DF': 'IV24M110',
        '24MTH_IMPVOL_90.0%MNY_DF': 'IV24M90',
        '24MTH_IMPVOL_97.5%MNY_DF': 'IV24M97HALF',
        '30DAY_IMPVOL_100.0%MNY_DF': 'IV1M100',
        '30DAY_IMPVOL_102.5%MNY_DF': 'IV1M102HALF',
        '30DAY_IMPVOL_90.0%MNY_DF': 'IV1M90',
        '3MTH_IMPVOL_100.0%MNY_DF': 'IV3M100',
        '3MTH_IMPVOL_102.5%MNY_DF': 'IV3M102HALF',
        '3MTH_IMPVOL_110.0%MNY_DF': 'IV3M110',
        '3MTH_IMPVOL_90.0%MNY_DF': 'IV3M90',
        '3MTH_IMPVOL_97.5%MNY_DF': 'IV3M97HALF',
        '6MTH_IMPVOL_100.0%MNY_DF': 'IV6M100',
        '6MTH_IMPVOL_102.5%MNY_DF': 'IV6M102HALF',
        '6MTH_IMPVOL_110.0%MNY_DF': 'IV6M110',
        '6MTH_IMPVOL_90.0%MNY_DF': 'IV6M90',
        '6MTH_IMPVOL_97.5%MNY_DF': 'IV6M97HALF',
        'HIGH': 'High',
        'LAST_PRICE': 'Last',
        'LOW': 'Low',
        'OPEN': 'Open',
        'VOLATILITY_180D': 'HV6M',
        'VOLATILITY_20D': 'HV1M',
        'VOLATILITY_260D': 'HV12M',
        'VOLATILITY_360D': 'HV24M',
        'VOLATILITY_60D': 'HV3M',
        '60DAY_IMPVOL_90.0%MNY_DF':'IV2M90',
        '60DAY_IMPVOL_100.0%MNY_DF':'IV2M100'}

# query the database for underlyings


def undl_list():
    strsql = "SELECT Ticker,underlying_id from underlying WHERE (R2D2 LIKE 'DLIB%' OR R2D2 LIKE 'STATS') AND Ticker<>'';"
    try:
        cur.execute(strsql)
        return pd.DataFrame(cur.fetchall(), columns=['Ticker', 'underlying_id'])
    except mariadb.Error as e:
        print(f"Error: {e}")


print("Importing from BBG... ")
# retrieving the list of und
UL = undl_list()

# connect to bbg
conbbg = pdblp.BCon(timeout=10000)
print("Connection to BBG initialized")

# download X days from bbg
# check for weekends
if datetime.today().weekday()==0:
     shift=3
else:
     shift=1
print("Downloading %s day(s)" % str(shift))

begindate = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
enddate = (datetime.today() - timedelta(days=shift)).strftime('%Y%m%d')

bbgfields = list(fields.keys())  #get the bbg fields

conbbg.start()

datadf1 = conbbg.bdh(list(UL.iloc[:, 0]), bbgfields[3:25], enddate, begindate) #have to limit to 25 fields..., so we
datadf2 = conbbg.bdh(list(UL.iloc[:, 0]), bbgfields[25:], enddate, begindate) #split in 2

datadf = pd.concat([datadf1, datadf2], axis=1) #and concatenate
datadf = datadf.stack(level=0, future_stack=True) #Does not work anymore
# datadf = datadf.stack(level=0, dropna=False, sort=False)  #switching tickers in columns


# adding the underlying_id to the dataframe
UL.set_index('Ticker', inplace=True)
datadf = datadf.join(UL, on='ticker')
datadf = datadf.reset_index()
datadf.rename(columns=fields, inplace=True)
datadf = datadf.drop('Ticker', axis='columns') # remove unused columns (maybe not necessary)
# datadf=datadf.dropna(how='any',axis=0) #remove n/a

# formating for mysql insert into
dsfields = list(fields.values())
dsfields = dsfields[:2]+dsfields[3:]
datadf = datadf[dsfields]
# print(datadf)

# for debug purposes
dateslist=[d.strftime("%Y-%m-%d") for d in datadf['Date'].unique().tolist()]
underlyingslist=datadf['underlying_id'].unique().tolist()

print(str(len(dateslist)) + " date" + ("s" if len(dateslist)>1 else ""))
print(str(len(datadf['underlying_id'].unique().tolist())) + " underlyings")

# removing existing data
print("Import to DS")
for d in dateslist:
        cur.execute("DELETE FROM deathstar.data WHERE date='" + d + "'")
#cur.execute("TRUNCATE deathstar.data")

condb.commit()
condb.close()

# inserting
engine = create_engine('mysql+mysqlconnector://' + mysql_user + ':' + mysql_passwd + '@' + mysql_IP +':' + mysql_port + '/' + mysql_db, echo=False, paramstyle="format")
datadf.to_sql('data', engine, if_exists='append', chunksize=1000, index=False)
print("Data inserted. total time: %s" % time.strftime("%H:%M:%S", time.gmtime(time.time()-time_start)))
engine.dispose()
conbbg.stop()
#test