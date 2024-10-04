import mariadb
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams['toolbar'] = 'None'

time_start=time.time()

conn = mariadb.connect(
  host="22.121.64.146",
  user="Marc",
  password="12345678",
  database="deathstar"
)

cur = conn.cursor()

#get underlyings list
def undl_list():
    strsql= "SELECT underlying_id from underlying WHERE R2D2 LIKE 'DLIB%' OR R2D2 LIKE 'STATS'"
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall(),columns=['underlying_id'])
        return list(df.iloc[:,0])
    except mariadb.Error as e:
        print(f"Error: {e}")

N=undl_list()

#N=[1,2,3,4,5,8,9,10,11,12,13,16,17,18,20,21,22,23,24,25,26,27,28,30,31,32,33,36,37,39,40,45,46,47,48,49,50,52,53,54,55,56,57,58,59,60,61,62,63,64,65,67,68,70,72,73,74,75,77,78,79,80,81,83,84,87,88,89,90,91,92,93,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,120,121,122,123,124,125,126,127,128,130,132,133,134,135,136,137,138,139,140,143,146,147,148,149,150,151,152,153,154,156,157,158,159,161,162,163,164,166,167,171,172,173,174,177,179,180,181,182,183,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,205,207,209,210,211,212,213,214,215,217,218,219,220,221,222,224,226,227,229,230,231,232,233,234,235,236,239,240,241,243,244,247,248,249,250,252,253,254,255,258,259,260,261,262,263,264,266,267,268,269,270,271,272,273,274,275,276,277,278,280,281,282,283,284,285,286,287,290,291,292,293,294,296,297,299,300,302,304,305,307,309,311,312,314,316,320,321,322,324,326,327,328,329,330,333,334,335,341,342,345,346,347,350,351,353,357,358,359,363,365,367,369,372,374,375,376,377,379,403,406,408,411,413,414,415,416,418,420,421,422,424,426,427,429,432,433,434,435,437,438,439,440,441,442,446,449,450,451,454,456,457,458,460,463,464,469,471,473,474,475,476,480,481,482,485,486,490,493,494,498,499,500,503,506,513,514,516,517,518,524,526,528,532,533,534,535,538,539,540,541,544,546,547,552,554,555,557,558,560,561,562,563,564,565,566,569,570,574,575,576,579,580,581,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,631,633,634,635,636,638,643,647,648,650,651,654,657,658,661,670,672,673,674,676,678,681,682,684,689,695,698,700,701,702,703,706,711,733,740,745,746,747,756,757,758,759,760,764,765,767,771,772,775,776,777,781,782,783,784,785,786,788,799,800,801,804,811,814,824,827,833,837,838,843,845,862,868,869,873,874,881,883,888,889,893,903,904,916,921,925,927,928]
#N=[7,13,475]


#correl maturity
corr_maturity=90

##Query the database for a given F field (pivoted in a table date/field yeah!) and a nbr of days
def getthedata(F,days):
    strsql= "SELECT date"
    for i in range(len(N)):
        strsql = strsql + " ,AVG(CASE WHEN underlying_id = " + str(N[i]) +" THEN " + F + " END)"
    strsql = strsql + " FROM data GROUP BY date ORDER BY date DESC LIMIT " + str(days)
    try:
        cur.execute(strsql)
        df=pd.DataFrame(cur.fetchall())
        return df.dropna(how='all',axis=1).dropna(how='any',axis=0).values.tolist()
    except mariadb.Error as e:
        print(f"Error: {e}")

#Retrieving the data

A=getthedata('IV6M100',)
B=getthedata('Last',90)



#log return correlation calc with pandas
def calcC(mat,d,r):
    df=pd.DataFrame(mat)
    C=(np.log(df.iloc[d:r:,1:]/df.iloc[d:r:,1:].shift())).corr('pearson').values.tolist()
    return C

C0=calcC(B,0,corr_maturity) #calc the correl when d=0
print(len(C0))
Cav0=np.average([C0[i][:i]+C0[i][i+1:] for i in range(len(C0))]) #average correlation
print(Cav0)

print("calc time: %s" % time.strftime("%H:%M:%S", time.gmtime(time.time()-time_start)))



conn.close

