import datetime
import json


#GENERAL CONFIGS
now = datetime.datetime.now()
date_today = ("%d-%.2d-%.2d %.2d-%.2d" % (now.year, now.month, now.day, now.hour, now.minute))
now_3 = now- datetime.timedelta(days=3)
now_4 = now- datetime.timedelta(days=4)
date_today_3 = datetime.date.today()- datetime.timedelta(days=3)
str_date_3 = ("%d%.2d%.2d" % (now_3.year, now_3.month, now_3.day))
str_date_4 = ("%d-%.2d-%.2d" % (now_4.year, now_4.month, now_4.day))

str_today = ("%d-%.2d-%.2d" % (now.year, now.month, now.day))

date_ranges = [
  #  {'start_date':'2020-08-10', 'end_date':str_date_4},
    {'start_date':'2025-01-01', 'end_date':'2025-01-31'},
]



variables_keep_original = ['Shop','avg_time_spent','Week','sum_sessions_count','sum_total_time_spent','time_result','total_time_spent'
                           ,'sum_toy_unlocked_count','sum_scan_mode_finished_count','total_scans','Total_Users','total_users','users','minigame','environment','percentage',
                           'grass_month','starting_persona','persona','user_cnt','year','month','country_name','feature','session','ratio']

dashboards_supported_sf = [14,292,319,294]
dashboards_supported_gb = [319]
dashboards_supported = dashboards_supported_gb

overide_old_data = False

env = 'beta'




SQLs_Path = 'scripts/Metabase_Optimize_SQLs_gb/'





user        = "SAI_METABASE"
password    = "BWQ3n8wgMXgYr4s"
account     = "gameloft.eu-west-1"
db          = "ELEPHANT_DB"
importWarehouse = "GAME_TEAMS"
schema      = "APPLAYDU"
store_table      = "STORE_STATS"

