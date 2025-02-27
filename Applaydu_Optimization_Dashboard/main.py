from modules import apd_1_create_preprocess_optimize_file_gb as create_optimizer
from modules import apd_2_create_new_query_with_preprocess as create_query
from modules import apd_3_optimizer_gb as process_optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils

#preprocess.start()

from utils import ultils as ultils
import configs
import json
from google.cloud import bigquery
import os

def getConn():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cre/gcp-gfb-sai-tracking-gold.json'
    conn = bigquery.Client()
    return conn

def closeConn(conn):
    conn.close()

def main():

    create_preprocess_file = False
    run_preprocess = True

    dashboard_query_ids = []

    if(create_preprocess_file == True):
        for dashboard in configs.dashboards_supported:
            with open(configs.SQLs_Path + 'dashboard_%d_%s_gb.json'%(dashboard,configs.env), 'r') as json_file:
                dashboard_query_ids = json.load(json_file)
                for dashboard_query_id in dashboard_query_ids:
                    #optimizer the queries
                    create_optimizer.start(dashboard_query_id)

                    #create query with preprocess data
                    create_query.start(dashboard_query_id)

               
    if(run_preprocess == False):
        return
    
    conn = getConn()
    df_preprocessed= {}
    for dashboard_id in configs.dashboards_supported:
        df_preprocessed["apd_report_%d"%(dashboard_id)] = process_optimizer.f_get_preprocessed_data(conn, "apd_report_%d" % dashboard_id)
   
    for dashboard in configs.dashboards_supported:
        with open(configs.SQLs_Path + 'dashboard_%d_%s_gb.json'%(dashboard,configs.env), 'r') as json_file:
            dashboard_query_ids = json.load(json_file)
            for dashboard_query_id in dashboard_query_ids:            
                   
                #preprocess the data
                process_optimizer.start(conn,df_preprocessed,dashboard_query_id)
    
    closeConn(conn)
main()