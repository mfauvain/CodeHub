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

#connect to mysql server
engine = create_engine('mysql+mysqlconnector://' + mysql_user + ':' + mysql_passwd + '@' + mysql_IP +':' + mysql_port + '/' + mysql_db, echo=False,paramstyle="format")

#query the database for underlyings
def get_undl_list():
    strsql= "SELECT underlying_id from underlying WHERE R2D2 LIKE 'DLIB%' OR R2D2 LIKE 'STATS'"
    try:
        return pd.read_sql(strsql, engine).iloc[:,0].values.tolist()
    except mariadb.Error as e:
        print(f"Error: {e}")

#retrieving data as a pivot table
def get_data(und_list,F):
    strsql= "SELECT date"
    for i in range(len(und_list)):
        strsql = strsql + " ,AVG(CASE WHEN underlying_id = " + str(und_list[i]) +" THEN " + F + " END) as '" + str(und_list[i]) + "'"
    strsql = strsql + " FROM data GROUP BY date ORDER BY date DESC"
    try:
        df=pd.read_sql(strsql, engine)
        return df  #.dropna(how='any',axis=0).values.tolist()
    except mariadb.Error as e:
        print(f"Error: {e}")

#simple linear function
def funcL(L):
    def func(x):
        return L[0]*x+L[1]
    return func

#distance to line
def distance(x,linreg, y):
    return y-funcL(linreg)(x)

#distance to line when on same side as current dot
def sameside_distance(x,linreg,y,current):
    res=y-funcL(linreg)(x)
    if res*current>0:
        return abs(res)
    else:
        return float('nan')

#calc the score for each field
def calc_scores(list_und,flds,nd,w,proxy,offset):
    list_und.remove(proxy)
    list_und=[proxy]+list_und
    R=pd.DataFrame(columns=['Date','underlying_id'] + list(flds.values()))
    for f in list(flds.keys()):
        A=get_data(list_und,f)
        for o in range(offset):
            start_date=A.iloc[o,0]
            for und in list_und[1:]:
                if R.loc[(R['Date']==start_date) & (R['underlying_id']==und)].empty:
                    R=pd.concat([R,pd.DataFrame([{'Date':start_date,'underlying_id':und}])],ignore_index=True)
                und_data_full=A[[str(proxy),str(und)]].dropna(how='any',axis=0)
                TRank=0
                #calc reference linreg
                proxy_data=und_data_full.iloc[o:nd[0]+o,0].values.tolist()
                und_data=und_data_full.iloc[o:nd[0]+o,1].values.tolist()
                if len(und_data)==nd[0]:
                        LRref=stats.linregress(proxy_data,und_data)
                        #print(str(nd[0]) +'d linreg used')     
                for i in nd[1:]:
                    proxy_data=und_data_full.iloc[o:i+o,0].values.tolist()
                    und_data=und_data_full.iloc[o:i+o,1].values.tolist()
                    if len(und_data)==i:
                        LR=stats.linregress(proxy_data,und_data)
                        if LR[2]>LRref[2]:
                            LRref=LR
                        #print(str(i) +'d linreg used')

                for i in nd:
                    proxy_data=und_data_full.iloc[o:i+o,0].values.tolist()
                    und_data=und_data_full.iloc[o:i+o,1].values.tolist()
                    if len(und_data)==i:
                        current_distance=distance(proxy_data[0], LRref, und_data[0])
                        D=list(map(lambda x,y: sameside_distance(x,LRref,y,current_distance),proxy_data[1:],und_data[1:]))
                        if current_distance>=0:
                            P=50+(0.5*stats.percentileofscore(D,abs(current_distance),nan_policy='omit')*LRref[2]**2)
                        else:
                            P=50-(0.5*stats.percentileofscore(D,abs(current_distance),nan_policy='omit')*LRref[2]**2)
                        TRank=TRank+P*w[nd.index(i)]
                    else:
                        TRank=np.nan
                R.loc[(R['Date']==start_date) & (R['underlying_id']==und),str(flds.get(f))]=TRank
    return R


def get_undl_list():
    strsql = "SELECT Bloomberg FROM (SELECT * FROM underlying WHERE R2D2 LIKE 'DLIB%' OR R2D2 LIKE 'STATS') as A LEFT JOIN (SELECT underlying_id, MarketCap FROM fundamentals) as B ON A.underlying_id=B.underlying_id ORDER BY MarketCap DESC"
    return pd.read_sql(strsql, engine)['Bloomberg'].tolist()
    
    

def calc_TScore(und,und_field,DW,offset,ref=None,ref_field=None):
    d=list(DW.keys())
    w=list(DW.values())
    
    if ref==None:
        ref=und
    if ref_field==None:
        ref_field='Last'

    if sum(w)==1:
        strsql= """SELECT U.Date, U.F, R.F, U.underlying_id FROM 
                (SELECT """ + und_field + """ as F, Date, underlying_id FROM data WHERE underlying_id=(SELECT underlying_id FROM underlying WHERE Bloomberg='""" + und + """')) as U
                JOIN
                (SELECT """ + ref_field + """ as F, Date FROM data WHERE underlying_id=(SELECT underlying_id FROM underlying WHERE Bloomberg='""" + ref + """')) as R
                ON U.Date=R.Date
                WHERE U.F<>0 AND R.F<>0 AND U.Date<CURDATE()
                ORDER BY U.Date DESC
                """
        data=pd.read_sql(strsql, engine).dropna(how='any',axis=0)
        res=[]
        if len(data)>0:
            underlying_id=data.iloc[:,3][0]
            for o in range(min(offset,len(data))):
                TScore=0
                date=data.iloc[o,0]
                und_data=data.iloc[o:d[0]+o,1].values.tolist()
                ref_data=data.iloc[o:d[0]+o,2].values.tolist()
                if len(und_data)==d[0]:
                    LRref=stats.linregress(ref_data,und_data)
                    #usedreg=d[0]
                if len(d)>1:
                    for i in d[1:3]: ##only the first 3 considered in the best regression selection
                        und_data=data.iloc[o:i+o,1].values.tolist()
                        ref_data=data.iloc[o:i+o,2].values.tolist()
                        if len(und_data)==i:
                            LR=stats.linregress(ref_data,und_data)
                            if LR[2]>LRref[2]:
                                LRref=LR
                                #usedreg=i
                        
                for i in d:
                    und_data=data.iloc[o:i+o,1].values.tolist()
                    ref_data=data.iloc[o:i+o,2].values.tolist()
                    if len(und_data)==i:
                        current_distance=distance(ref_data[0], LRref, und_data[0])
                        D=list(map(lambda x,y: sameside_distance(x,LRref,y,current_distance),ref_data[1:],und_data[1:]))
                        pctile=stats.percentileofscore(D,abs(current_distance),nan_policy='omit')*(0.5+0.5*LRref[2]**2)
                        if current_distance>=0:
                            P=50+(0.5*pctile)
                        else:
                            P=50-(0.5*pctile)
                    else:
                        P=np.nan
                    #print(str(usedreg) + ":" + str(i) + ":" + str(P))
                    TScore=TScore+P*w[d.index(i)]

                res=res+[[date,underlying_id,round(TScore,1)]]
        else:
            res=[]
            
    return pd.DataFrame(res,columns=['date','underlying_id',und_field])

    
#str(flds.get(f))

#UL=get_undl_list()
#UL=[6,127]
#Fields={'IV1M100':'TRank1M','IV3M100':'TRank3M','IV6M100':'TRank6M','IV12M100':'TRank12M','IV24M100':'TRank24M'}
#Fields={'IV12M100':'TRank12M'}
#NbDays=[250, 500, 750, 1000, 1250]

#Proxy=6
#Offset=1

#{250:0.3,500:0.3,750:0.2,1000:0.1,1250:0.1}

time_start = time.time()

res=[]
NbrDays=1
Phi=(1+5**0.5)/2
W=[Phi**-3, Phi**-2, Phi**-1, Phi**0]
Sum_W=sum(W)
Norm_W=[w/Sum_W for w in W]
IndicWeights=[Norm_W[2],Norm_W[3],Norm_W[1],Norm_W[0]]

#IndicWeights=[0.3,0.4,0.2,0.1]
#DictDW={250:0.2,500:0.4,750:0.2,1000:0.1,1250:0.1}
DictDW={250:0.4,500:0.4,750:0.2}
#DictDW={250:1}
#undl_list=['ARM US']
undl_list=get_undl_list()
print("Calculating TRanks for %s underlyings and %s dates" % (str(len(undl_list)),str(NbrDays)))
for undl in range(len(undl_list)):
    df=pd.DataFrame({'date':{},'underlying_id':{}})
    for field in ['IV3M100','IV6M100','IV12M100','IV24M100']:
        c_df=calc_TScore(undl_list[undl],field,DictDW,NbrDays,None,None)
        if len(c_df)>0:
            df=pd.merge(df,c_df,how='outer', on=['date','underlying_id'])
            df.rename(columns={field: 'TR0'}, inplace=True)
            df=pd.merge(df,calc_TScore(undl_list[undl],field,DictDW,NbrDays,'SPX',field),how='outer', on=['date','underlying_id'])
            df.rename(columns={field: 'TR1'}, inplace=True)
            df=pd.merge(df,calc_TScore(undl_list[undl],field,DictDW,NbrDays,None,'HV1M'),how='outer', on=['date','underlying_id'])
            df.rename(columns={field: 'TR2'}, inplace=True)
            df=pd.merge(df,calc_TScore(undl_list[undl],field,DictDW,NbrDays,None,'IV12M100-IV3M100'),how='outer', on=['date','underlying_id'])
            df.rename(columns={field: 'TR3'}, inplace=True)
            df['TRank'+field]=round(sum([df['TR'+str(i)]*IndicWeights[i] for i in range(len(IndicWeights))]),1)
            df.drop(['TR'+str(i) for i in range(len(IndicWeights))],axis=1,inplace=True)
            df.dropna(how='any',axis=0,inplace=True)

    df.to_sql('trank', engine, if_exists='append', chunksize=10000, index=False)
    print("Data inserted. %s elapsed, %s done" % (time.strftime("%H:%M:%S", time.gmtime(time.time()-time_start)),format(undl/len(undl_list),".1%")))

print("Data inserted. total time: %s" % time.strftime("%H:%M:%S", time.gmtime(time.time()-time_start)))
    #print(df)
    #df.to_csv(r'c:\Users\MB15433\Documents\BBVA\Python\testTR.csv',index=False)
    #TRanks=calc_TScore(undl_list[undl],'IV3M100',{250:0.2,500:0.4,750:0.2,1000:0.1,1250:0.1},0,None,None)
    #TRanks=[calc_TScore(undl_list[undl],field,{250:0.2,500:0.4,750:0.2,1000:0.1,1250:0.1},0,None,None) for field in ['IV3M100','IV12M100','IV24M100']]
    #TRanksRV=[calc_TScore(undl_list[undl],field,{250:0.2,500:0.4,750:0.2,1000:0.1,1250:0.1},10,'SPX',field) for field in ['IV3M100','IV12M100','IV24M100']]
    #TRanksHV=[calc_TScore(undl_list[undl],field,{250:0.2,500:0.4,750:0.2,1000:0.1,1250:0.1},10,None,'HV1M') for field in ['IV3M100','IV12M100','IV24M100']]
    #TRanksCal=[calc_TScore(undl_list[undl],field,{250:0.2,500:0.4,750:0.2,1000:0.1,1250:0.1},10,None,'IV12M100-IV3M100') for field in ['IV3M100','IV12M100','IV24M100']]
    #res.append([undl_list[undl]]+[IndicWeights[0]*TRanks[i] + IndicWeights[1]*TRanksRV[i] + IndicWeights[2]*TRanksHV[i] + IndicWeights[3]*TRanksCal[i] for i in range(3)])   
    #res.append([undl_list[undl],TRanks,TRanksRV,TRanksHV,TRanksCal])
         

#print(res)
#dfr=pd.DataFrame(res,columns=['Name','TRank1M','TRank3M','TRank6M','TRank12M','TRank24M'])
#print(dfr)
#print(calc_scores(UL,Fields,NbDays,Weights,Proxy,Offset))
#print("stats calculated. total time: %s" % time.strftime("%H:%M:%S", time.gmtime(time.time()-time_start)))

engine.dispose()