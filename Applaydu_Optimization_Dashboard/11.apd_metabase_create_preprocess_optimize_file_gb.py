from modules import apd_optimize_metabase_preprocess as optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils
import configs
import re

dashboard_id = configs.dashboard_query_ids[0]['dashboard']
query_id = configs.dashboard_query_ids[0]['query']

# Define the input and output file paths
input_file_path = configs.SQLs_Path+'input.sql'
output_file_path =  configs.SQLs_Path+ 'apd_report_%d_%d.sql'%(dashboard_id, query_id)

# Load the text file
with open(input_file_path, 'r') as file:
    data = file.read()

    # Remove the specific text from the data

    replacements = [
        ['server_time', 'client_time'],
        ['CLIENT_TIME', 'client_time'],
        ['SERVER_DATE', 'server_date'],
        ['WHERE', 'where'],
        ['AND', 'and'],
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
        ['tbl_ua_filter` where 1=1', 'tbl_ua_filter` where 2=2'],
        ['tbl_date_filter` where 1=1', 'tbl_date_filter` where 2=2'],
        ['tbl_country_filter` where 1=1', 'tbl_country_filter` where 2=2'],
        ['tbl_version_filter` where 1=1', 'tbl_version_filter` where 2=2'],
        ['tbl_shop_filter` where 1=1', 'tbl_shop_filter` where 2=2'],
        ['tbl_persona_filter` where 1=1', 'tbl_persona_filter` where 2=2'],
        ['USER_ACTIVITY', 'user_activity'],
        ['gcp-bi-elephant-db-gold.applaydu.user_activity' , '`gcp-bi-elephant-db-gold.applaydu.user_activity`'],
        ['``', '`'],
    ]

    for old, new in replacements:
        data = data.replace(old, new)

# Replace the regex string as user_activity` ... + any + where 1=1 to user_activity` where 10=10
    pattern = re.compile(r"user_activity`\s*.*?\s*where 1=1", re.DOTALL)
    data = pattern.sub("user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_sub('iend_date', INTERVAL 3 DAY)", data)


    text_to_removes = [
         "and t.country_name IN (select country_name from `applaydu.tbl_country_filter` where 1=1 [[and {{icountry}}]])",
        # date all filter
         "and client_time >= '2020-08-10' and client_time < dateadd(day, -3, current_date())",
    
        #date min filter
        "and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]])",
        "and date(server_date)>=(select max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]])",
        "and date(client_time)>='2020-08-10'",
    
    #     #date max filter
         "and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]]), INTERVAL 1 DAY)",
        "and date(client_time)<date_sub(current_date(), INTERVAL 3 DAY)",

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
    ]
    
    
    for text_to_remove in text_to_removes:
        data = data.replace(text_to_remove, "")

    replacements = [
        ('where 1=1', "where 1=1 and date(%s) >= 'istart_date' and date(%s) < date_sub('iend_date', INTERVAL 3 DAY)" % (configs.dashboard_query_ids[0]['date_filter'], configs.dashboard_query_ids[0]['date_filter'])),
        ('min(client_time),min(client_time)', 'min(client_time),min(server_time)'),
        ('server_time AS client_time', 'server_time AS server_time'),
       ]

    for old, new in replacements:
        data = data.replace(old, new)

# Remove empty lines
data = "\n".join([line for line in data.split("\n") if line.strip() != ""])


params = ""
selectors = ""
for i, col in enumerate(configs.dashboard_query_ids[0]['columns']):
    new_col = "value%d" % (i+1)
    new_selector = col[0]
    if col[1] == 'string':
        new_col = "value%d_str" % (i+1)

    params = params + new_col + ","
    selector = col[0]
    if(col[0] not in configs.variables_keep_original):
        selector = '`' + col[0] +'`'
    selectors = selectors + selector + " as " + new_col + ","

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
	"""%(dashboard_id,query_id,configs.dashboard_query_ids[0]['kpi_name'],selectors)


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