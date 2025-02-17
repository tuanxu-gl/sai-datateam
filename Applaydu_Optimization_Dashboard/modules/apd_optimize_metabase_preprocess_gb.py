import pandas as pd
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import datetime
from dateutil.relativedelta import relativedelta
import configs
import utils.ultils as ultils
from google.cloud import bigquery
import os
from datetime import datetime

def f_get_preprocessed_data(conn,table_name):
    sql = "select start_date,end_date,dashboard_id,query_id from `gcp-gfb-sai-tracking-gold.applaydu.%s` where 1=1"%table_name
    print ("Execute select_sql ")

    query_job = conn.query(sql)

    result = query_job.result()

    print(result.total_rows, " record(s) downloaded")
    #Fletch result
    df_sql = result.to_dataframe()
    if(len(df_sql) > 0):
        df_sql.to_csv("data/%s.csv"%table_name,index=True)
        return df_sql
    return None

def f_delete_old_data(conn,table_name,dashboard_id,query_id,str_start_date, str_end_date):
    #delete old data
    delete_sql = "delete from `gcp-gfb-sai-tracking-gold.applaydu.%s` where 1=1 and dashboard_id = %d and query_id= %d and start_date = '%s' and end_date = '%s'"%(table_name,dashboard_id,query_id,str_start_date, str_end_date)
    print ("Execute delete_sql ")
    
    query_job = conn.query(delete_sql)

    result = query_job.result()

    print(result.total_rows, "record(s) affected")
    if result is None or result.total_rows == 0:
        print("No records found to delete.")
        return



def f_preprocess_report_data(conn,table_name,str_sql,dashboard_id,query_id,str_start_date, str_end_date):
    str_sql = str_sql.replace("istart_date",str_start_date)
    str_sql = str_sql.replace("iend_date",str_end_date)
    str_sql = str_sql.replace("idashboard_id", str(dashboard_id))
    str_sql = str_sql.replace("iquery_id", str(query_id))
    #print(str_sql)
    with open(configs.SQLs_Path+"output.sql", "w") as file:
        file.write(str_sql)
        

    query_job = conn.query(str_sql)
    try:
        result = query_job.result()
        print(result.num_dml_affected_rows , "record(s) added")
        if result is None or result.num_dml_affected_rows  == 0:
            print("No records added=> Wrong code")
            #print(str_sql)
            return 'Failed'
        return 'Successful'
    except Exception as e:
        print("An error occurred: ", e)
        #print("Failed SQL: ", str_sql)
        return 'Failed'
   

def f_write_log(log_txt):
    print(log_txt)
    with open("log.txt", "a") as f:
        f.write(log_txt + "\n")

def start(apd_version):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cre/gcp-gfb-sai-tracking-gold.json'

    conn = bigquery.Client()
    for date_range in configs.date_ranges:
        start_date = date_range['start_date']
        #end_date = configs.consumer_preprocess_end_date[apd_version]
        str_start_date = date_range['start_date'] #2025-01-01
        str_end_date = date_range['end_date'] #2025-01-31
       
        str_sql = {}
        df_snowflake= {}
        for dashboard_id in configs.dashboards_supported:
            df_snowflake["apd_report_%d"%(dashboard_id)] = f_get_preprocessed_data(conn, "apd_report_%d" % dashboard_id)

        for dashboard_query_id in configs.dashboard_query_ids:
            time_start_preprocess = datetime.now()
        
                

            table_name = "apd_report_%d"%(dashboard_query_id['dashboard'])

            if(configs.overide_old_data == True):

                f_delete_old_data(conn, table_name, dashboard_query_id['dashboard'], dashboard_query_id['query'], str_start_date, str_end_date)
            else:
                #check if the data is already preprocessed
                df_check = df_snowflake[table_name]

                if df_check is not None and not df_check.empty:
                    if(len(df_check[(df_check['dashboard_id'] == dashboard_query_id['dashboard']) & (df_check['query_id'] == dashboard_query_id['query']) & (df_check['start_date'] == str_start_date) & (df_check['end_date'] == str_end_date)]) > 0):
                        time_end_preprocess = datetime.now()
                        log_txt = "Log datetime=%s Data is already preprocessed %s start_date=%s, end_date=%s, dashboard_id = %d,query_id=%d total = %d seconds"%(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),table_name, str_start_date, str_end_date, dashboard_query_id['dashboard'], dashboard_query_id['query'], (time_end_preprocess - time_start_preprocess).seconds)
                        
                        f_write_log(log_txt)

                        continue
            f_sql = open(configs.SQLs_Path+'apd_report_%d_%d.sql'%(dashboard_query_id['dashboard'], dashboard_query_id['query']))
            str_sql = f_sql.read()



            result = f_preprocess_report_data(conn,table_name,str_sql,dashboard_query_id['dashboard'], dashboard_query_id['query'],str_start_date, str_end_date)

            time_end_preprocess = datetime.now()

            log_txt =  "Log datetime=%s Preprocessed %s %s start_date=%s, end_date=%s, dashboard_id = %d,query_id=%d total = %d seconds"%(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),table_name, result, str_start_date, str_end_date, dashboard_query_id['dashboard'], dashboard_query_id['query'], (time_end_preprocess - time_start_preprocess).seconds)
            f_write_log(log_txt)
        

    conn.close()
