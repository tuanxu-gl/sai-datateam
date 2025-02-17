import pandas as pd
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import snowflake.connector
import datetime
from dateutil.relativedelta import relativedelta
import configs
import utils.ultils as ultils

def f_get_preprocessed_data_on_Snowflake(conn,table_name):
    sql = "select start_date,end_date,dashboard_id,query_id from applaydu_not_certified.%s where 1=1"%table_name
    print ("Execute select_sql ")
    result = conn.cursor().execute(sql)
    print(result.rowcount, "record(s) downloaded")
    #Fletch result
    df_sql = pd.DataFrame.from_records(result.fetchall(), columns=[x[0] for x in result.description])
    if(len(df_sql) > 0):
        df_sql.to_csv("data/%s.csv"%table_name,index=True)
        return df_sql
    return pd.DataFrame()

def f_delete_old_data(conn,table_name,dashboard_id,query_id,str_start_date, str_end_date):
    #delete old data
    delete_sql = "delete from applaydu_not_certified.%s where 1=1 and dashboard_id = %d and query_id= %d and start_date = '%s' and end_date = '%s'"%(table_name,dashboard_id,query_id,str_start_date, str_end_date)
    print ("Execute delete_sql ")
    result = conn.cursor().execute(delete_sql)
    print(result.rowcount, "record(s) affected")



def f_preprocess_report_data(conn,table_name,str_sql,dashboard_id,query_id,str_start_date, str_end_date):
    str_sql = str_sql.replace("istart_date",str_start_date)
    str_sql = str_sql.replace("iend_date",str_end_date)
    str_sql = str_sql.replace("idashboard_id", str(dashboard_id))
    str_sql = str_sql.replace("iquery_id", str(query_id))
    #print(str_sql)
    with open(configs.SQLs_Path+"output.sql", "w") as file:
        file.write(str_sql)
        

    result = conn.cursor().execute(str_sql)
    #print(result.rowcount, "record(s) affected")

    #time.sleep(1)

def f_write_log(log_txt):
    print(log_txt)
    with open("log.txt", "a") as f:
        f.write(log_txt + "\n")

def start(apd_version):

    conn = snowflake.connector.connect(
        user=configs.user,
        password=configs.password,
        account=configs.account,
        warehouse=configs.importWarehouse,
        database=configs.db,
        schema=configs.schema,
        #role=configs.role,
    )

    start_date = configs.apd_start_date[apd_version]
    #end_date = configs.consumer_preprocess_end_date[apd_version]
    str_start_date = ("%d-%.2d-%.2d" % (start_date.year, start_date.month, start_date.day))
    str_end_date = configs.str_date_4 #("%d-%.2d-%.2d" % (end_date.year, end_date.month, end_date.day))
    
    str_sql = {}
    df_snowflake= {}
    for dashboard_id in configs.dashboards_supported:
        df_snowflake["apd_report_%d"%(dashboard_id)] = f_get_preprocessed_data_on_Snowflake(conn, "apd_report_%d" % dashboard_id)

    for dashboard_query_id in configs.dashboard_query_ids:
        time_start_preprocess = datetime.datetime.now()
      
            

        table_name = "apd_report_%d"%(dashboard_query_id['dashboard'])

        if(configs.overide_old_data == True):

            f_delete_old_data(conn, table_name, dashboard_query_id['dashboard'], dashboard_query_id['query'], str_start_date, str_end_date)
        else:
            #check if the data is already preprocessed
            df_check = df_snowflake[table_name]

            if(len(df_check) > 0):
                if(len(df_check[(df_check['DASHBOARD_ID'] == dashboard_query_id['dashboard']) & (df_check['QUERY_ID'] == dashboard_query_id['query']) & (df_check['START_DATE'] == str_start_date) & (df_check['END_DATE'] == str_end_date)]) > 0):
                    time_end_preprocess = datetime.datetime.now()
                    log_txt = "Log datetime=%s Data is already preprocessed %s start_date=%s, end_date=%s, dashboard_id = %d,query_id=%d total = %d seconds"%(
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),table_name, str_start_date, str_end_date, dashboard_query_id['dashboard'], dashboard_query_id['query'], (time_end_preprocess - time_start_preprocess).seconds)
                    
                    f_write_log(log_txt)

                    continue
        f_sql = open(configs.SQLs_Path+'apd_report_%d_%d.sql'%(dashboard_query_id['dashboard'], dashboard_query_id['query']))
        str_sql = f_sql.read()



        f_preprocess_report_data(conn,table_name,str_sql,dashboard_query_id['dashboard'], dashboard_query_id['query'],str_start_date, str_end_date)

        time_end_preprocess = datetime.datetime.now()

        log_txt =  "Log datetime=%s Preprocessed %s start_date=%s, end_date=%s, dashboard_id = %d,query_id=%d total = %d seconds"%(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),table_name, str_start_date, str_end_date, dashboard_query_id['dashboard'], dashboard_query_id['query'], (time_end_preprocess - time_start_preprocess).seconds)
        f_write_log(log_txt)
        

    conn.close()
