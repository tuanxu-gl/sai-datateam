from modules import apd_optimize_metabase_preprocess as optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils
import configs
import re

def start(dashboard_query_id):
    
    dashboard_id = dashboard_query_id['dashboard']
    query_id = dashboard_query_id['query']
    # Define the input and output file paths
    input_file_path = configs.SQLs_Path+'i%d/i_%d_%d.sql'%(dashboard_id,dashboard_id,query_id)
    output_file_path =  configs.SQLs_Path+ 'r%d'%dashboard_id + '/apd_report_%d_%d.sql'%(dashboard_id,query_id)

    # Load the text file
    with open(input_file_path, 'r') as file:
        data = file.read()

        # Remove the specific text from the data

        #1. Standalisize the text
        replacements = [
            ['"', '`'],
            ['2=2', '1=1'],
            ['server_time', 'client_time'],
            ['CLIENT_TIME', 'client_time'],
            ['SERVER_DATE', 'server_date'],
            ['WHERE', 'where'],
            ['AND', 'and'],
            [' IN ', ' in '],
            ['SELECT', 'select'],
            ['COUNTRY_NAME', 'country_name'],
            ['COUNTRY', 'country'],
            ['  ', ' '],
            ['] )', '])'],
            ['client_time,client_time', 'client_time,server_time'],
            ['CURRENT_DATE(', 'current_date('],
            ['DATE_ADD(', 'date_add('],
            ['DATE_SUB(', 'date_sub('],
            ['DATE(', 'date('],
            ['MIN(', 'min('],
            ['MAX(', 'max('],
            ['FROM', 'from'],
            ['JOIN', 'join'],
            [' >', '>'],
            [' <', '<'],
            [' =', '='],
            ['> ', '>'],
            ['< ', '<'],
            ['= ', '='],
            #['2=2', '1=1'],
            # ['APPLAYDU_NOT_CERTIFIED', '`gcp-gfb-sai-tracking-gold.applaydu`'],

            # ['tbl_ua_filter` where 1=1', 'tbl_ua_filter` where 2=2'],
            # ['tbl_date_filter` where 1=1', 'tbl_date_filter` where 2=2'],
            # ['tbl_date_filter where 2=2', 'tbl_date_filter` where 2=2'],
            # ['tbl_country_filter` where 1=1', 'tbl_country_filter` where 2=2'],
            # ['tbl_version_filter` where 1=1', 'tbl_version_filter` where 2=2'],
            # ['tbl_shop_filter` where 1=1', 'tbl_shop_filter` where 2=2'],
            # ['tbl_persona_filter` where 1=1', 'tbl_persona_filter` where 2=2'],

         

            ['USER_ACTIVITY', 'user_activity'],
           # ['gcp-bi-elephant-db-gold.applaydu.user_activity' , '`gcp-bi-elephant-db-gold.applaydu.user_activity`'],
            ['``', '`'],
        ]

        for old, new in replacements:
            data = data.replace(old, new)

        #2. Remove the commond not used text 
        text_to_removes = [
            "and (select available from tbl_check_preprocess_report)='N/A'",

            "and t.country in (select country from `applaydu.tbl_country_filter` where 1=1 [[and {{icountry}}]])",
            "and t.country_name in (select country_name from `applaydu.tbl_country_filter` where 1=1 [[and {{icountry}}]])",
            "and t.country_name in (select country_name from tbl_country_filter where 2=2  [[AND {{icountry}}]])",
            "and t.country_name in (select country_name from tbl_country_filter where 1=1 [[and {{icountry}}]])",
            "and t.country_name in (select country_name from `applaydu.tbl_country_filter` where 2=2 )",
            "and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[and {{icountry}}]] [[and {{iregion}}]])",
            "and t.country in (select country from `applaydu.tbl_country_filter` where 1=1 [[and {{icountry}}]] [[and {{iregion}}]])",
            # date all filter
            "and client_time >= '2020-08-10' and client_time < dateadd(day, -3, current_date())",
        
            #date min filter
            "and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]])",
            "and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]])",
            "and date(server_date)>=(select max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]])",
            "and date(client_time)>='2020-08-10'",
            "and client_time>='2020-08-10'",
            "and client_time>=(select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]])",
            "and client_time<dateadd(day, 1,(select max(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]))",
            "and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )",
            "and client_time>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]])",
        #     #date max filter
            "and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]]), INTERVAL 1 DAY)",
            "and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]]), INTERVAL 1 DAY)",
            "and date(client_time)<date_sub(current_date(), INTERVAL 3 DAY)",
            "and date(client_time)<date_sub(current_date(), INTERVAL 3 DAY)",
            "and client_time<dateadd(day, -3, current_date())",
            "and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)",
            "and client_time<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]]), INTERVAL 1 DAY)",
        #version filter
            "and version in (select version from tbl_version_filter where 1=1  [[and {{iversion}}]])",
            "and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[and {{iversion}}]])",
            "and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[and {{from_version}}]])",
            "and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[and {{to_version}}]])",
            #shop filter
            "join tbl_shop_filter on tbl_shop_filter.game_id=t.game_id and tbl_shop_filter.country_name=t.country_name",
            """JOIN 
    `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name""",
    "join \n  `applaydu.tbl_shop_filter` sf ON sf.game_id=t.game_id and sf.country_name=t.country_name",


        ]
        

        for text_to_remove in text_to_removes:
            data = data.replace(text_to_remove, "")
        with open('output.sql', 'w') as file:
            file.write(data)

    # Replace the regex string as user_activity` ... + any + where 1=1 to user_activity` where 10=10
        pattern = re.compile(r"user_activity`\s*.*?\s*where 1=1", re.DOTALL)
        data = pattern.sub("user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_sub('iend_date', INTERVAL 3 DAY)", data)

        #remove the text specific
        text_to_removes = [
            "[[and {{ishopfilter}}]]",
            "[[and {{ishop_filter}}]]",
            "[[and {{iinstallsource}}]]",
            "[[and {{icountry}}]]",
            "[[and {{iregion}}]]",
            "[[and {{iversion}}]]",
            "[[and {{idate}}]]",
            "[[and {{from_version}}]]",
            "[[and {{to_version}}]]",
            "[[and game_id={{ggi}}]]",
            "[[and game_id={{GGI}}]]",
            "[[and {{inotcountry}}]]",
            "[[and {{inotregion}}]]",
            "[[and {{ipersona}}]]",
            "[[and {{iinstall_source}}]]",
            "[[and {{ishopfilter}}]]",
        ]
        for text_to_remove in text_to_removes:
            data = data.replace(text_to_remove, "")

        #replace the text specific
        replacements = [
            ('where 1=1', "where 1=1 and date(%s) >= 'istart_date' and date(%s) < date_add('iend_date', INTERVAL 1 DAY)" % (dashboard_query_id['date_filter'], dashboard_query_id['date_filter'])),
            ('min(client_time),min(client_time)', 'min(client_time),min(server_time)'),
            ('server_time AS client_time', 'server_time AS server_time'),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'", "'2024-08-28'"),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_kinderini_start_date'", "'2024-10-19'"),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'", "'2024-08-28'"),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'", "'2024-08-28'"),
        ]

        for old, new in replacements:
            data = data.replace(old, new)

    # Remove empty lines
    data = "\n".join([line for line in data.split("\n") if line.strip() != ""])


    params = ""
    selectors = ""
    for i, col in enumerate(dashboard_query_id['columns']):
        new_col = "value%d" % (i+1)
        new_selector = col[0]
        if col[1] == 'string':
            new_col = "value%d_str" % (i+1)

        params = params + new_col + ","
        selector = col[0]
        if(col[0] not in configs.variables_keep_original):
            selector = '`' + col[0] +'`'
        if col[1] != 'string':
            selectors = selectors + selector + " as " + new_col + ","
        else:
            selectors = selectors + "CAST(" + selector + " as STRING) as " + new_col + ","

    params = params[:-1]
    selectors = selectors[:-1]

    main_sql_to_insert = """\n\nselect %d as dashboard_id
            ,%d as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'%s' as kpi_name
            ,%s
        from
        (
        """%(dashboard_id,query_id,dashboard_query_id['kpi_name'],selectors)


        # Find the last occurrence of ')'
    last_paren_index = data.rfind('main query')

        # Insert "abc" after the last ')'
    if last_paren_index != -1:
        data = data[:last_paren_index +10] + main_sql_to_insert + data[last_paren_index + 10:] + "\n)"

    data = """insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_%d` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,%s)\n"""%(dashboard_id,params) + data


    # Save the content to the output file
    with open(output_file_path, 'w') as file:
        file.write(data)
        print("The file has been saved to %s" % output_file_path)