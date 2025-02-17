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

import webbrowser

def f_open_GP_download_links():
    for file in configs.gp_data_files:
        print (file['link'])
        webbrowser.open(file['link'], new=0, autoraise=True)
        time.sleep(3)



def f_preprocess_data_on_Snowflake():
    data_path = "h:/Projects/Applaydu/Data/TBL_CONSUMER_t95_PREPROCESS_v6/"
    for filename in os.listdir (data_path):
        if filename.endswith(".csv"):
            
            print('Loading data of '  + filename)
            df = pd.read_csv(data_path  + filename)

            print('Processing data of '  + filename)
            df = df.drop(['LOAD_TIME','Unnamed: 0'], axis=1)
            for game_id in configs.apd_game_id:
                print('Processing data of game_id %d'%game_id)
                df_game_id = df[df['GAME_ID'] == game_id]

                new_filename = data_path + '%d/'%game_id + filename[:-4] + '_%d.csv'%game_id
                print('Saving data of ' +  new_filename)
                df_game_id.to_csv(new_filename,index=True)


def f_get_data_on_Snowflake(conn,sql,file_name):
    print ("Execute select_sql ")
    result = conn.cursor().execute(sql)
    print(result.rowcount, " record(s) downloaded - saving ",file_name)
    #Fletch result
    df_sql = pd.DataFrame.from_records(result.fetchall(), columns=[x[0] for x in result.description])
    print(result.rowcount, " record(s) downloaded - saving 1 ",file_name)
    if(len(df_sql) > 0):
        df_sql.to_csv("data/%s.csv"%file_name,index=True)
        print(result.rowcount, " record(s) downloaded - saved ",file_name)
        return df_sql
    return pd.DataFrame()

def build_list_date_range_new_data():
    start_date = configs.apd_start_date
    end_date = configs.consumer_preprocess_end_date

    #add list date
    period_list = list(range(1,7))
    substep_list = list(range(0,6))
    days_diff = (end_date - start_date).days + 1
    years_diff = int((end_date - start_date).days/365)
    months_diff = int((end_date - start_date).days/30)
    print('day_diff: %d'%days_diff)
    
    #add list 10  weeks 
    for week in range(1,configs.period_weeks + 1):
        value_to_add = week * 7
        if(value_to_add not in period_list):
            period_list.append(value_to_add)
    
    #add list of 10 months
    #store_date = datetime.datetime.strptime(row['Date'], "%b %d, %Y")
    istart_date = datetime.datetime(start_date.year,start_date.month,1)
    while(istart_date < end_date):
        #do something...
        for i in range(1,configs.period_months+1):
            ichecking = istart_date + relativedelta(months=i)
            idiff = (ichecking - istart_date).days 
            if(idiff not in period_list):
                period_list.append(idiff)
            #building isubstep
            diff_from_begin = (ichecking - configs.apd_start_date).days 
            isubstep = diff_from_begin % idiff
            if(isubstep not in substep_list):
                substep_list.append(isubstep)

        istart_date = istart_date + relativedelta(months=1)
    
    #add list of 4 years
    # istart_date = datetime.datetime(start_date.year,start_date.month,1)
    # while(istart_date < end_date):
    #     #do something...
    #     for i in range(1,configs.period_years):
    #         ichecking = istart_date + relativedelta(years=i)
    #         idiff = (ichecking - istart_date).days 
    #         if(idiff not in period_list):
    #             period_list.append(idiff)

    #         #building isubstep
    #         diff_from_begin = (ichecking - configs.apd_start_date).days 
    #         isubstep = diff_from_begin % idiff
    #         if(isubstep not in substep_list):
    #             substep_list.append(isubstep)

    #     istart_date = istart_date + relativedelta(years=1)
    

    period_list = sorted(period_list,key=int)
    print('list dates: ')
    for period in period_list:
        print (period)

    substep_list = sorted(substep_list,key=int)
    print('list substep: ')
    for substep in substep_list:
        print (substep)

def build_list_date_range():
    start_date = configs.apd_start_date
    end_date = configs.consumer_preprocess_end_date

    #add list date
    period_list = list(range(1,32))
    days_diff = (end_date - start_date).days + 1
    years_diff = int((end_date - start_date).days/365)
    months_diff = int((end_date - start_date).days/30)
    print('day_diff: %d'%days_diff)
    
    #add list 10  weeks 
    for week in range(5,configs.period_weeks):
        value_to_add = week * 7
        if(value_to_add not in period_list):
            period_list.append(value_to_add)
    
    #add list of 10 months
    #store_date = datetime.datetime.strptime(row['Date'], "%b %d, %Y")
    istart_date = datetime.datetime(start_date.year,start_date.month,1)
    while(istart_date < end_date):
        #do something...
        for i in range(1,configs.period_months):
            ichecking = istart_date + relativedelta(months=i)
            idiff = (ichecking - istart_date).days 
            if(idiff not in period_list):
                period_list.append(idiff)

        istart_date = istart_date + relativedelta(months=1)
    
    #add list of 4 years
    istart_date = datetime.datetime(start_date.year,start_date.month,1)
    while(istart_date < end_date):
        #do something...
        for i in range(1,configs.period_years):
            ichecking = istart_date + relativedelta(years=i)
            idiff = (ichecking - istart_date).days 
            if(idiff not in period_list):
                period_list.append(idiff)
                
        istart_date = istart_date + relativedelta(years=1)
    

    period_list = sorted(period_list,key=int)
    print('list dates: ')
    for period in period_list:
        print (period)

