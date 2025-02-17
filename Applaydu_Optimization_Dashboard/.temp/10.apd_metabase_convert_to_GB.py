from modules import apd_optimize_metabase_preprocess as optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils
import configs

dashboard_id = 319
query_id = 4244

# Define the input and output file paths
input_file_path = configs.SQLs_Path+'input.sql'
output_file_path =  configs.SQLs_Path+ 'gb_%d_%d.sql'%(dashboard_id, query_id)

# Load the text file
with open(input_file_path, 'r') as file:
    data = file.read()

    # Remove the specific text from the data

   

    text_to_removes =[
        #schema names
        '"elephant_db".',
          'elephant_db.',
        '"elephant_db"',
          'elephant_db',
          '::int',
          '::float',
          '::string',
          '::time',
          ]
    
    text_to_upper =[
        #sql keywords
        'SELECT',
        'FROM',
        'WHERE',
        'AND',
       
        'ORDER BY',
        'GROUP BY',
        'LEFT JOIN',
        'INNER JOIN',
        'RIGHT JOIN',
        'JOIN',
       
        'CASE',
        'WHEN',
        'THEN',
        'ELSE',
        'END',
       
        'COUNT',
        'SUM',
        'MAX',
        'MIN',
        'AVG',
        'DISTINCT',
        'IS NULL',
        'IS NOT NULL',
        'LIKE',
        'BETWEEN',
        'NOT IN',
        'NOT LIKE',
        'NOT BETWEEN',
        'NULL',
        'EXISTS',
        'ALL',
        'ANY',
        'UNION',
        'UNION ALL',
        'EXCEPT',
        'INTERSECT',
      

        #column names
        'SERVER_DATE',
    ]
    text_to_lowers = [
        #schema names
        'APPLAYDU_NOT_CERTIFIED',
        'APPLAYDU',
        'ELEPHANT_DB',
        
        #table names
        'tbl_STORY_MODE_FINISHED',
        'REAL_STORY_MODE_FINISHED',
        'MINIGAME_FINISHED',
        'STORY_MODE_FINISHED',
        'TBL_VARIABLES',

        #variables
        'GAME_ID',
        'CLIENT_TIME',
        'SERVER_TIME',
        'SERVER_DATE',
        'VERSION',
        'COUNTRY',
        'SCENE_NAME',
        'CLICK_FROM',
        'INSTALL_SOURCE',
        'COUNTRY_NAME',
        'VERSION',
        'USER_ID',
        'LOAD_TIME',
        'FROM_SCENE',
        

    ]
    text_to_replaces = [
        ['  ', ' '],
        ['  ', ' '],
        ['  ', ' '],
        ['  ', ' '],
        ['  ', ' '],
        [' >', '>'],
        ['> ', '>'],
        [' <', '<'],
        ['< ', '<'],
        [' =', '='],
        ['= ', '='],
        [' ,', ','],
        [', ', ','],
        ['] )','])'],
        #schema names
        ['"applaydu"', 'applaydu'],
        ['"applaydu_not_certified"', 'applaydu_not_certified'],
        ['"elephant_db"', 'elephant_db'],

        #table names
        ['"minigame_finished"', 'minigame_finished'],

        #variables
        ['"game_id"', 'game_id'],   
        ['"client_time"', 'client_time'],
        ['"server_time"', 'server_time'],
        ['"version"', 'version'],
        ['"country"', 'country'],
        ['"scene_name"', 'scene_name'],
        ['"click_from"', 'click_from'],
        ['"date"', 'date'],
        ['"install_source"', 'install_source'],
        ['"country_name"', 'country_name'],
        ['"user_id"', 'user_id'],
        ['"load_time"', 'load_time'],
        ['"from_scene"', 'from_scene'],

        ['server_time', 'client_time'],

        ['dateadd(day, -3, CURRENT_DATE())', 'DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)'],
        ['dateadd(day,-3,CURRENT_DATE())', 'DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)'],

        ['[[AND {{iinstallsource}}]]', 'install_source in (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])'],
        ['applaydu.', 'gcp-bi-elephant-db-gold.applaydu.'],
        ['applaydu_not_certified.', 'gcp-gfb-sai-tracking-gold.applaydu.'],
       
        ['tbl_date_filter', '`applaydu.tbl_date_filter`'],
        ['tbl_country_filter', '`applaydu.tbl_country_filter`'],
        ['tbl_shop_filter', '`applaydu.tbl_shop_filter`'],
        ['tbl_version_filter', '`applaydu.tbl_version_filter`'],
        
        ['tbl_country_vr', '`applaydu.tbl_country_vr`'],
        ['tbl_filter_toy_tracking', '`applaydu.tbl_filter_toy_tracking`'],

        ['client_time>=(select ivalue','date(client_time)>=(select date(ivalue)'],

        ['client_time<','date(client_time)<'],
        ['client_time>','date(client_time)>'],

        #["(SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] )","(SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])"],
        #['client_time<dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] ))', 'DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)'],
        #['COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 1=1  [[AND {{icountry}}]])', 'COUNTRY_NAME in (select COUNTRY_NAME from `applaydu.tbl_country_filter` where 1=1  [[AND {{icountry}}]])'],
        ['(select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]])', """(
            SELECT DISTINCT user_id 
            FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
            WHERE 1=1 and install_source in (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
            AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
            )"""
         ],

        #bug fix for date add function
        ['dateadd(day,1,(SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]))'
            ,'DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)'],
       
        ['to_varchar(client_time) as tfs_session','CAST(client_time AS STRING) AS tfs_session'],
        ['to_varchar(activity_10_value) as tfs_session','CAST(activity_10_value AS STRING) AS tfs_session'],

         #limit date by version
         ["version>='4.0.0'","(version >='4.0.0' AND DATE(client_time) >= '2023-08-22')"],
         ["version>='4.3.0'","(version >='4.3.0' AND DATE(client_time) >= '2023-11-24')"],
         ["version>='4.5.0'","(version >='4.5.0' AND DATE(client_time) >= '2024-02-05')"],
         ["version>='4.6.1'","(version >='4.6.1' AND DATE(client_time) >= '2024-03-11')"],
         ["version>='5.0.0'","(version >='5.0.0' AND DATE(client_time) >= '2024-08-28')"],
         ["version>='5.2.0'","(version >='5.2.0' AND DATE(client_time) >= '2024-10-19')"],
         ["version>='5.4.0'","(version >='5.4.0' AND DATE(client_time) >= '2024-12-04')"],
         
         ['time_spent::int', 'time_spent'],
         ['time_between_sessions::int', 'time_between_sessions'],
         #remove dup replace
         ['gcp-gfb-sai-tracking-gold.gcp-bi-elephant-db-gold', 'gcp-bi-elephant-db-gold'],
         ['"','`'],

         ['gcp-bi-elephant-db-gold.applaydu.minigame_finished.',''],

         ['client_time,client_time','client_time'],
         ["""client_time AS client_time,\nclient_time AS client_time""", 'client_time AS client_time,server_time AS server_time'],
         
        #correct variable name
        ['(min)','in min'],
        ['(min - sec)','in min sec'],
       
    ] 
    # Lower the case of the data
    for text in text_to_lowers:
        data = data.replace(text, text.lower())

    # Lower the case of the data
    for text in text_to_upper:
        data = data.replace(text, text.upper())

    # Remove the specific text from the data
    
    for text_to_remove in text_to_removes:
        data = data.replace(text_to_remove, "")

    for text_to_replace in text_to_replaces:
        data = data.replace(text_to_replace[0], text_to_replace[1])


# Remove empty lines
data = "\n".join([line for line in data.split("\n") if line.strip() != ""])




# Save the content to the output file
with open(output_file_path, 'w') as file:
    file.write(data)
    print("The file has been saved to %s" % output_file_path)