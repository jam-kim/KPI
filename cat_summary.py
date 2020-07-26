import pyhs2
import pandas as pd
import time 
import os

class bms_code:
    typedata = "/home/ndap/output/table_types.xlsx"
    bmspartition = "/home/ndap/output/bms_partition.xlsx"
    rackpartition = "/home/ndap/output/rack_partition.xlsx"
    cellvolpartition = "/home/ndap/output/cellvol_partition.xlsx"
        
    def __init__(self):
        self.typedata = pd.read_excel(self.typedata)
        self.bmspartition = pd.read_excel(self.bmspartition)
        self.rackpartition = pd.read_excel(self.rackpartition)
        self.cellvolpartition = pd.read_excel(self.cellvolpartition)
    
    def ndap_cat(self, tablename, date, time):
        data = self.typedata
        catlist = data.loc[data["types"]=="categorical","col"].reset_index(drop=True)
        collist = ""
        for i in range(len(catlist)):
            attach = catlist[i]
            if i != len(catlist):
                collist += "'"+attach+"',"
            elif i == len(catlist):
                collist += "'"+attach
        self.ndapcode = "SELECT {columns}, count(1) AS count FROM {tablename} WHERE colec_date='{date}' AND colec_time='{time}''".format(columns=collist,
                                                                                                                 tablename=tablename,
                                                                                                                 date=date,
                                                                                                                 time=time)   
    def ndap_con(data):
        data = self.typedata
        cat_list = data.loc[datatypes["types"]=="continuous","col"].reset_index(drop=True)
        col_list = ""
        for i in range(len(cat_list)):
            attach = cat_list[i]
            if i != len(cat_list):
                col_list += "'"+attach+"',"
            elif i == len(cat_list):
                col_list += "'"+attach
        return col_list
    col_list = columns_list("table_types.xlsx", "categorical")

    def samplecode(self,tablename, date, time):
        self.ndapcode = "SELECT bms_no FROM {tablename} WHERE colec_date='{date}' AND colec_time='{time}'".format(tablename=tablename,
                                                                                                                 date=date,
                                                                                                                 time=time)   
        
    def excute_query(self, types, tablename, date, time):
        if types is "continuous":
            self.ndap_con(tablename, date, time)
        elif types is "categorical":
            self.ndap_cat(tablename, date, time)      
        elif types is "samplecode":
            self.samplecode(tablename,date,time)            
        print self.ndapcode
        conn1 = pyhs2.connect(host='mgmt05.ess.com',
                              port=10000,
                              authMechanism="PLAIN",
                              user='bmsanl',
                              password='ndap1234~',
                              database='default')
        cursor = conn1.cursor()
        # cursor.getDatabases().
        cursor.execute(self.ndapcode)
        columnNames = [a['columnName'] for a in  cursor.getSchema()]
        value = cursor.fetchall()
        df=pd.DataFrame(data=value,columns=columnNames)
        
        return df



test = bms_code()

# test.ndap_cat("table_types.xlsx","bms_snsr","2019-07-17","14")
# test.excute_query("table_types.xlsx","bms_snsr","2019-07-17","14")

# test.hiveql_cat("bms_snsr","2019-07-17","14")
test.typedata
# test.ndapcode
data = test.excute_query("samplecode","mgt.bms_snsr","2019-07-17","14")

df = test.typedata
df2 = df.loc[df["types"]=="categorical",:]
df2.head()

"min({}) AS min_{}, percentile(CAST({} AS BIGINT), 0.1) AS per10_{}, percentile(CAST({} AS BIGINT), 0.2) AS per20_{}, percentile(CAST({} AS BIGINT), 0.3) AS per30_{}, percentile(CAST({} AS BIGINT), 0.4) AS per40_{}, percentile(CAST({} AS BIGINT), 0.5) AS median_{}, percentile(CAST({} AS BIGINT), 0.6) AS per60_{}, percentile(CAST({} AS BIGINT), 0.7) AS per70_{}, percentile(CAST({} AS BIGINT), 0.8) AS per80_{}, percentile(CAST({} AS BIGINT), 0.9) AS per90_{}, max({}) AS max_{}, avg({}) AS mean_{}, stddev_samp({}) AS Sd_samp_{}, sum(IF({} IS null, 1, 0)) AS count_null_{}, count(1) AS count_tot_{},".
