import pyhs2
import pandas as pd
import time 
import os

class code:
    typedata = "/home/ndap/ref/table_types.xlsx"
    bmspartition = "/home/ndap/ref/(partition)bms3.xlsx"
    rackpartition = "/home/ndap/ref/(partition)rack3.xlsx"
    cellvoltpartition = "/home/ndap/ref/(partition)cellvolt3.xlsx"
    
    def __init__(self):
        self.typedata = pd.read_excel(self.typedata)
        self.bmspartition = pd.read_excel(self.bmspartition)
        self.rackpartition = pd.read_excel(self.rackpartition)
        self.cellvoltpartition = pd.read_excel(self.cellvoltpartition)
    
    def save_folder(self, location):
        self.savefolder = "/data1/output_py/{}".format(location)
        if os.path.exists(self.savefolder) is not True:
            os.mkdir(self.savefolder)
            
    def load_folder(self, location):
        self.loadfolder = "/data1/output_py/{}".format(location)
        assert os.path.exists(self.loadfolder) is True, "Check folder name......"
    '''
    범주형 변수 
    각 범주 개수 파악    
    '''
    def ndap_cat(self, tablename, date):
        assert tablename in ["mgt.bms_snsr","mgt.rack_genl_snsr","mgt.cell_volt_snsr"], "Check tablename......"
        
        data = self.typedata
        catlist = data.loc[(data["table"]==tablename[4:])&(data["types"]=="categorical"),"col"].reset_index(drop=True)
        collist = ""
        for i in range(len(catlist)):
            attach = catlist[i]
            if i != (len(catlist)-1):
                collist += attach+", "
            elif i == (len(catlist)-1):
                collist += attach
        self.ndapcode = "SELECT {columns}, count(1) AS count FROM {tablename} WHERE colec_date='{date}' GROUP BY {columns}".format(columns=collist, tablename=tablename, date=date)
#         self.ndapcode = "SELECT {columns}, count(1) AS count FROM {tablename} WHERE colec_date='{date}' GROUP BY {columns}".format(columns=collist, tablename=tablename, date=date)
    '''
    연속형 변수
    0~100 백분위수, 평균, 표준편차
    '''
    def ndap_con(self, tablename):
        assert tablename in ["mgt.bms_snsr","mgt.rack_genl_snsr","mgt.cell_volt_snsr"], "Check tablename......"
        
        data = self.typedata
        catlist = data.loc[(data["table"]==tablename[4:])&(data["types"]=="continuous"),"col"].reset_index(drop=True)
        tempcode = tempcode = "min({0}) AS min_{0}, percentile(CAST({0} AS BIGINT), 0.1) AS per10_{0}, percentile(CAST({0} AS BIGINT), 0.2) AS per20_{0}, percentile(CAST({0} AS BIGINT), 0.3) AS per30_{0}, percentile(CAST({0} AS BIGINT), 0.4) AS per40_{0}, percentile(CAST({0} AS BIGINT), 0.5) AS median_{0}, percentile(CAST({0} AS BIGINT), 0.6) AS per60_{0}, percentile(CAST({0} AS BIGINT), 0.7) AS per70_{0}, percentile(CAST({0} AS BIGINT), 0.8) AS per80_{0}, percentile(CAST({0} AS BIGINT), 0.9) AS per90_{0}, max({0}) AS max_{0}, avg({0}) AS mean_{0}, stddev_samp({0}) AS Sd_samp_{0}, sum(IF({0} IS null, 1, 0)) AS count_null_{0}, sum(IF({0} IS NOT null, 1, 0)) AS count_tot_{0}"
        for i in range(len(catlist)):
            collist = ""
            for i in range(len(catlist)):
                attach = tempcode.format(catlist[i])
                if i != (len(catlist)-1):
                    collist += attach+", "
                elif i == (len(catlist)-1):
                    collist += attach
        self.ndapcode = "SELECT {columns} FROM {tablename}".format(columns=collist, tablename=tablename)
    '''
    reserved 변수 
    각 범주 개수 파악    
    '''
    def ndap_res(self, tablename, date):
        assert tablename in ["mgt.bms_snsr","mgt.rack_genl_snsr","mgt.cell_volt_snsr"], "Check tablename......"
        
        data = self.typedata
        catlist = data.loc[(data["table"]==tablename[4:])&(data["types"]=="reserved","col")].reset_index(drop=True)
        collist = ""
        for i in range(len(catlist)):
            attach = catlist[i]
            if i != (len(catlist)-1):
                collist += attach+", "
            elif i == (len(catlist)-1):
                collist += attach
        self.ndapcode = "SELECT {columns}, count(1) AS count FROM {tablename} WHERE colec_date='{date}' GROUP BY {columns}".format(columns=collist, tablename=tablename, date=date)
    '''
    hive query 실행 함수    
    '''
    def sendingcode(self):
        conn = pyhs2.connect(host='',
                                  port='',
                                  authMechanism='',
                                  user='',
                                  password='',
                                  database='')
        cursor = conn.cursor()  
        # cursor.getDatabases().
        cursor.execute(self.ndapcode)
        columnNames = [a['columnName'] for a in  cursor.getSchema()]
        value = cursor.fetchall()
        df=pd.DataFrame(data=value,columns=columnNames)
        return df

    
    def excute_query(self, types, tablename, range_s, range_e, bandwidth, location):
        print(pd.Timestamp.now())
        start = time.time()
        
        if tablename == "mgt.bms_snsr":
            self.partition = self.bmspartition   
        elif tablename == "mgt.rack_genl_snsr":
            self.partition = self.rackpartition
        elif tablename == "mgt.cell_volt_snsr":
            self.partition = self.cellvoltpartition
        
        # save folder setting
        self.save_folder(location)
        
        if types is "continuous":
            self.ndap_con(tablename)
            self.sendingcode().to_csv(os.path.join(self.savefolder,"{}_{}.csv".format(tablename, types)), encoding="utf-8", sep=',', index=False)
            end = time.time()-start2
            m, s = divmod(end, 60)
            h, m = divmod(m, 60)
            print("times to reading: {:02d}:{:02d}:{:.2f}".format(int(h),int(m),s)) 
        elif types is "categorical":
            # 범위 설정
            if range_e == 9999:
                range_e = len(self.partition)
            # 묶음 수 지정
            bandnow = 1
            df = pd.DataFrame()
            # 파티션 반복 시행
            for i in range(range_s, range_e):      
                start2 = time.time()
                date = self.partition["partition"][i]
                self.ndap_cat(tablename, date)                    
                df_temp = self.sendingcode()
                df_temp["file"] = "{}".format(date)
                df = pd.concat([df, df_temp])
                end = time.time()-start2
                m, s = divmod(end, 60)
                h, m = divmod(m, 60)
                print("row:{}({})\n=>times to reading : {:02d}:{:02d}:{:.2f}".format(i, date, int(h),int(m),s))
                if bandnow == bandwidth:
                    df.to_csv(os.path.join(self.savefolder,"{}_{}_{}.csv".format(tablename,bandwidth,i)), encoding="utf-8", sep=',', index=False)
                    df = pd.DataFrame()
                    bandnow = 0
                    print("saving is done")
                bandnow +=1        
            '''
            ndap 코드 확인
            print self.ndapcode
            '''            
        end = time.time()-start
        m, s = divmod(end, 60)
        h, m = divmod(m, 60)
        print("times to excute: {:02d}:{:02d}:{:.2f}".format(int(h),int(m),s))
    
    def summary_cat(self, save_name, loading, saving):
        
        # load folder setting
        self.load_folder(loading)
        # save folder setting
        self.save_folder(saving)
        
        datalocation = os.path.join(self.loadfolder)
        datalist = os.listdir(datalocation)
        if '.ipynb_checkpoints' in datalist:
            datalist.remove('.ipynb_checkpoints')
        datalist.sort()
        df = pd.DataFrame()
        # 무슨 체크 포인트 파일이 존재함
        for i in range(0,len(datalist)):
            temp = pd.read_csv(os.path.join(datalocation,datalist[i]))
            for name in ['Unnamed: 0','Unnamed: 85','bms_no','rack_no','string_no','module_no','maxcellvol1moduleid', 'maxcellvol1cellid', 'maxcellvol2moduleid', 'maxcellvol2cellid', 'mincellvol2moduleid', 'mincellvol2cellid', 'mincellvol1moduleid', 'mincellvol1cellid', 'maxcelltemp1moduleid', 'maxcelltemp1cellid', 'maxcelltemp2moduleid', 'maxcelltemp2cellid', 'mincelltemp2moduleid', 'mincelltemp2cellid', 'mincelltemp1moduleid', 'mincelltemp1cellid']:
                if name in temp.columns.values:
                    del temp[name]
            df = pd.concat([df, temp])
            df_col = df.columns.values.tolist()
            df_col.remove("count")
            print(datalist[i], temp["count"].sum())
            df = df.groupby(df_col).sum()
            df = df.reset_index()
#             print(df.sum()["count"])
        # summary data 저장
        df.to_csv('{}/{}.csv'.format(self.savefolder, save_name), index=False)        


test = code()

print(pd.Timestamp.now())
start = time.time()

# test.excute_query(types="categorical", 
#                   tablename="mgt.cell_volt_snsr",
#                   bandwidth = 1,
#                   range_s=0, 
#                   range_e=9999,
#                   location = 'cell_volt_snsr')

test.summary_cat(save_name="rack_genl_snsr",
                loading="rack_genl_snsr/rawdata",
                saving="rack_genl_snsr/summarydata")
end = time.time()-start
m, s = divmod(end, 60)
h, m = divmod(m, 60)
print("times to reading : {:02d}:{:02d}:{:.2f}".format(int(h),int(m),s))
