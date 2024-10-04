import mariadb
import datetime
import time
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import math
import pandas as pd
from pathlib import Path
import os

#import credentials
#csv file has to be stored on local computer in Users/$winloginid$/Documents/ETA/login.csv
#in the form
# field,data
# mysql_user,$user$
# mysql_passwd,$password$
# mysql_IP,$IP$
# mysql_port,$port$
# mysql_db,$name of db$


creds_file='C:/Users/' + os.getlogin() + '/Documents/ETA/login_ds.csv'
creds = pd.read_csv(creds_file)
creds.set_index('field',inplace=True)
creds=creds.to_dict(orient='dict')

mysql_user=creds['data']['mysql_user']
mysql_passwd=creds['data']['mysql_passwd']
mysql_IP=creds['data']['mysql_IP']
mysql_port=creds['data']['mysql_port']
mysql_db=creds['data']['mysql_db']


time_start=time.time()

conn = mariadb.connect(
  host=mysql_IP,
  user=mysql_user,
  port=int(mysql_port),
  password=mysql_passwd,
  database=mysql_db
)

cur = conn.cursor()

def getbasket(index_ticker, nbr_und):
    strsql= "SELECT index_id, underlying_id, weight FROM deathstar.new_baskets3 WHERE index_id=(SELECT underlying_id FROM deathstar.underlying WHERE Bloomberg='" + index_ticker + "') ORDER BY baskets_id DESC,weight DESC LIMIT " + str(nbr_und)
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall(),columns=['index_id','underlying_id','weight'])
        return df
    except mariadb.Error as e:
        print(f"Error: {e}")


def getbookvega(books,index_ticker,nbr_und):
    strsql= "SELECT (SELECT underlying_id FROM underlying WHERE Bloomberg='" + index_ticker + "') as index_id, underlying_id, sum(Vega) as vega FROM deathstar.vega, deathstar.underlying WHERE vega.ShortBloom=underlying.Bloomberg AND Portfolio IN (" + books +") AND ShortBloom<>'" + index_ticker + "' GROUP BY ShortBloom ORDER BY sum(Vega) DESC LIMIT " + str(nbr_und)
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall(),columns=['index_id','underlying_id','weight'])
        return df
    except mariadb.Error as e:
        print(f"Error: {e}")

def getbuylist(index_ticker,nbr_und,frombaskets=True):
    strsqlinit= """
        SELECT (SELECT underlying_id FROM deathstar.underlying WHERE Bloomberg='""" + index_ticker +  """') as index_id, S.underlying_id, 1/Tscore FROM
        (
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
        WHERE Marc_Rank<0.5 AND TScore>0.1 AND TScore<0.5 AND RV<0 AND 5DMove<0 AND LiqClass IN ('US_SS_20_40','US_SS_40_60','US_SS_60_80','US_SS_80_100') ORDER BY TScore ASC
        ) as S
        """
    strsqljoin="""
        INNER JOIN 
        (SELECT underlying_id,weight FROM deathstar.baskets WHERE index_id=6)
        AS Bs
        WHERE S.underlying_id=Bs.underlying_id
        """
    strsqlend=" ORDER BY TScore ASC LIMIT " + str(nbr_und)
    if frombaskets:
        strsql=strsqlinit + strsqljoin + strsqlend
    else:
        strsql=strsqlinit + strsqlend
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall(),columns=['index_id','underlying_id','weight'])
        return df
    except mariadb.Error as e:
        print(f"Error: {e}")


##Query the database for a given F field (pivoted in a table date/field yeah!)
def getthedata(underlyings,field,days):
    strsql= "SELECT date"
    for i in range(len(underlyings)):
        strsql = strsql + " ,AVG(CASE WHEN underlying_id = " + str(underlyings[i]) +" THEN " + field + " END) AS '" + str(underlyings[i]) + "'"
    strsql = strsql + " FROM data WHERE date<CURDATE() GROUP BY date ORDER BY date DESC LIMIT " + str(days)
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall())
        return df.dropna(how='any',axis=0).values.tolist()
    except mariadb.Error as e:
        print(f"Error: {e}")


#log return correlation calc with pandas
def calcC(data,d,r):
    df=pd.DataFrame(data)
    C=(np.log(df.iloc[d:d+r:,2:]/df.iloc[d:d+r:,2:].shift())).corr('pearson').values.tolist()
    return C



#calc the basket implied vol (using historical correlation) and weighted vol 
def calcV(underlyings,volatilities,skews,spots,realized,d,C0,Cav0,spotindex0,spotbasket0,constcorr=True):
    res=[volatilities[d][0]]
    Vindex=volatilities[d][1]
    Rindex=realized[d][1]
    Vskewindex=skews[d][1]
    SpotIndex=spots[d][1]
    V=list(volatilities[d][2:])
    R=list(realized[d][2:])
    S=list(spots[d][2:])
    Vskew=list(skews[d][2:])
    C=C0
    Cav=Cav0
    Onesdiag0=np.ones((len(underlyings)-1,len(underlyings)-1),int)
    np.fill_diagonal(Onesdiag0,0)
    if not constcorr:
        C=calcC(spots,d,corr_maturity)
        Cav=np.average([C[i][:i]+C[i][i+1:] for i in range(len(C))]) #average correlation
    SpotBasket=sum(np.einsum('i,i->i',W[1:],S))
    Vw=np.einsum('i,i->i',W[1:],V) #weights * V as a vector
    Rw=np.einsum('i,i->i',W[1:],R) #weights * R as a vector
    Vwskew=np.einsum('i,i->i',W[1:],Vskew) #weights * Vskew as a vector
    Vb=math.sqrt(np.einsum('i,ij,j->',Vw,C,Vw)) #sqrt of sum of sum einstein way...
    Vbskew=math.sqrt(np.einsum('i,ij,j->',Vwskew,C,Vwskew)) #same for skew
    CorrelImp=100*((Vindex)**2-np.einsum('i,i->',np.square(W[1:]),np.square(V)))/np.einsum('i,ij,j->',Vw,Onesdiag0,Vw)
    CorrelImpSkew=100*((Vskewindex)**2-np.einsum('i,i->',np.square(W[1:]),np.square(Vskew)))/np.einsum('i,ij,j->',Vwskew,Onesdiag0,Vwskew)
    Vw=sum(Vw)  #sum of vol weighted
    Vwskew=sum(Vwskew)  #sum of vol weighted skew
    res.append(Vindex)
    res.append(Vw) #vol weighted
    res.append(Vb)   #vol basket using correls
    spreadVw=Vw-Vindex
    res.append(spreadVw) #spread vol weighted vs index
    res.append(Vb-Vindex) #spread vol basket vs index
    res.append(Vw/Vindex) #ratio vol weighted vs index
    res.append(Cav*100)  # average correl
    res.append(CorrelImp) #implied correl
    res.append(SpotIndex)
    skewW=Vwskew-Vw
    res.append(skewW) #skewweighted
    res.append(Vbskew-Vb) #skewbasket
    res.append(CorrelImpSkew-CorrelImp) #correlskew
    spreadVwskew=Vwskew-Vskewindex
    res.append(spreadVwskew-spreadVw) #shadowdelta
    skewindex=Vskewindex-Vindex
    res.append(skewindex) #index skew
    res.append(Rindex)
    Rw=sum(Rw)  #sum of realized weighted
    res.append(Rw) #realized weighted
    res.append(Rw-Rindex) #spread realized
    res.append(SpotBasket)
    res.append(SpotIndex/spotindex0-1)
    res.append(SpotBasket/spotbasket0-1)
    res.append(spreadVwskew)  #spread vol weighted skew vs index skew

    return res


#underlyings

Bskt=getbasket('SPY US', 50)
#Bskt=getbookvega("'XEQMEX OTC','XEQMEX EMTN','EQ_US_EXO_MONO','EQ_US_EXO_MULTI'",'SPX',25)
#Bskt=getbuylist('SPX',50)
NbrDays=1250 #Nbr of days considered
corr_maturity=20   #correl maturity


#UL=[6,2272,259,309,532,442,166,40,117,63,148]
UL=[Bskt['index_id'][0]] + Bskt['underlying_id'].values.tolist()
W=[1] + [Bskt.loc[i,'weight']/Bskt['weight'].sum() for i in range(len(Bskt))]  #weights
#W=[1]+[1/(len(UL)-1) for t in range(len(UL)-1)] #generate weights that sum 1
#W=[1,.23,.22,.16,.15,.10,.07,.07]

#Retrieving the data and calc C0
A=getthedata(UL,'IV12M100',NbrDays)
Askew=getthedata(UL,'IV12M90',NbrDays)
B=getthedata(UL,'Last',NbrDays+corr_maturity)
R=getthedata(UL,'HV1M',NbrDays)


C0=calcC(B,0,corr_maturity) #calc the correl when d=0
Cav0=np.average([C0[i][:i]+C0[i][i+1:] for i in range(len(C0))]) #average correlation
SpotIndex0=B[0][1]
SpotBasket0=sum(np.einsum('i,i->i',W[1:],list(B[0][2:])))

VB=[calcV(UL,A,Askew,B,R,d,C0,Cav0,SpotIndex0,SpotBasket0,False) for d in range(len(A))]
#VB=[calcV(A,B,d,False) for d in range(10)]
dfr=pd.DataFrame(VB,columns=['Date','VolIndex','VW','VB','SpreadVW','SpreadVB','RatioVW','RC','IC','SpotIndex','SkewW','SkewB','SkewIC','ShadowD','SkewIndex','RIndex','RW','SpreadRW','SpotBasket','PerfIndex','PerfBasket','spreadVwskew'])

dfr.to_csv(r'c:\Users\MB15433\Documents\BBVA\Python\test.csv',index=False)
#print(dfr)

#print(C0)

prctile_spread=stats.percentileofscore(dfr['SpreadVW'],dfr['SpreadVW'][0],nan_policy='omit')
prctile_ratio=stats.percentileofscore(dfr['RatioVW'],dfr['RatioVW'][0],nan_policy='omit')
prctile_IC=stats.percentileofscore(dfr['IC'],dfr['IC'][0],nan_policy='omit')

#print("percentile spread, ratio and IC: {:.0f}, {:.0f}, {:.0f}".format(prctile_spread, prctile_ratio,prctile_IC))

#print("calc time: %s" % time.strftime("%H:%M:%S", time.gmtime(time.time()-time_start)))

#m=pd.concat([A,B],axis=1) #concatenate
#plt.plot(dfr['VolIndex'],dfr['RC'])
#dfr.plot.scatter('VolIndex','IC')
#plt.show()
conn.close

#test
