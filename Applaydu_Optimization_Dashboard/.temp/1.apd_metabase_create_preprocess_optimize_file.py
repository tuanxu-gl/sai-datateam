from modules import apd_optimize_metabase_preprocess as optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils
import configs

dashboard_id = configs.dashboard_query_ids[0]['dashboard']
query_id = configs.dashboard_query_ids[0]['query']

# Define the input and output file paths
input_file_path = configs.SQLs_Path+'input.sql'
output_file_path =  configs.SQLs_Path+ 'apd_report_%d_%d.sql'%(dashboard_id, query_id)

# Load the text file
with open(input_file_path, 'r') as file:
    data = file.read()

    # Remove the specific text from the data

    data = data.replace('server_time', 'client_time')
    data = data.replace('CLIENT_TIME', 'client_time')
    data = data.replace('SERVER_DATE', 'server_date')
    data = data.replace('WHERE', 'where')
    data = data.replace('AND', 'and')
    data = data.replace('SELECT', 'select')
    data = data.replace('COUNTRY_NAME', 'country_name')
    data = data.replace('COUNTRY', 'country')
    data = data.replace('  ', ' ')
    data = data.replace('] )', '])')
    data = data.replace('client_time,client_time', 'client_time,server_time')



    text_to_removes = [
        #country filter
        "and country in (select country from tbl_country_filter where 1=1 [[and {{icountry}}]])",
        "and country in (select country from tbl_country_filter where 1=1 [[and {{icountry}}]] [[and {{iregion}}]])",
        "and country_name in (select country_name from tbl_country_filter where 1=1 [[and {{icountry}}]])",
        "and country_name in (select country_name from tbl_shop_filter where 1=1 [[and {{ishopfilter}}]])",
        "and t.country in (select country from tbl_country_filter where 1=1 [[and {{icountry}}]] [[and {{iregion}}]])",
        "and t.country in (select country from tbl_country_filter where 1=1 [[and {{icountry}}]])",
        "and t.country_name in (select country_name from tbl_country_filter where 1=1 [[and {{icountry}}]])",

        #version filter
        "and version <= (select max(version) from tbl_version_filter where 1=1  [[and {{to_version}}]])",
        "and version <= (select max(version) from tbl_version_filter where 1=1 [[and {{to_version}}]])",
        "and version >= (select min(version) from tbl_version_filter where 1=1 [[and {{from_version}}]])",
        "and version in (select version from tbl_version_filter where 1=1 [[and {{iversion}}]])",
        

        #date all filter
        "and client_time >= '2020-08-10' and client_time < dateadd(day, -3, current_date())",
        "and (server_date >= '2020-08-10' and server_date < dateadd(day, -3, current_date()))",
        "and (server_time >= '2020-08-10' and server_time < dateadd(day, -3, current_date()))",

        
        #date min filter
        "and client_time >= (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]])",
        "and server_date >= (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]])",

        

        #date max filter
        "and client_time < dateadd(day, 1, (select max(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]))",
        "and client_time < dateadd(day, 1,(select max(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]] ))",
        
        "and client_time < current_date()",

        "and server_date < dateadd(day, 1,(select max(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]))",
        "and server_date < dateadd(day, -3, current_date())",
        "and server_date < dateadd(day, 1, (select max(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]))",
        "and client_time < dateadd(day, 1,(select max(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]))",
        
        #date old optimization filter
        """and date(client_time) >= (
        case
            when (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]) > '2020-08-10'
            then (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]])
            else cast((select ivalue from tbl_variables where ikey = 'db_start_date') as date)
        end
    )""",
    """date(client_time) >= (    case       when (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]) > '2020-08-10'       then (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]])       else cast((select ivalue from tbl_variables where ikey = 'db_start_date') as date)     end  )""",
    """and date(client_time) >= (case when (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]) > '2020-08-10' then (select min(server_date) from tbl_date_filter where 1=1 [[and {{idate}}]]) else cast((select ivalue from tbl_variables where ikey = 'db_start_date') as date) end)""",
    #shop filter
    "join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country_name = t.country_name",
    "and game_id in (select game_id from tbl_shop_filter where 1=1  [[and {{ishopfilter}}]])",
    
    "join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country = t.country",
    "join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country_name = t.country_name",
    "join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[and {{iINSTALL_SOURCE}}]]) using (user_id)",
    "and GAME_ID in (select GAME_ID from tbl_shop_filter where 1=1 [[and {{ishopfilter}}]])",
    "[[and {{ishopfilter}}]]",
    "[[and {{ishop_filter}}]]",
    ]
    
    
    for text_to_remove in text_to_removes:
        data = data.replace(text_to_remove, "")

    data = data.replace('where 1=1', "where 1=1 and %s >= 'istart_date' and %s < dateadd(day, 1, 'iend_date')"%(configs.dashboard_query_ids[0]['date_filter'],configs.dashboard_query_ids[0]['date_filter']))
    data = data.replace('WHERE  1=1', "where 1=1 and %s >= 'istart_date' and %s < dateadd(day, 1, 'iend_date')"%(configs.dashboard_query_ids[0]['date_filter'],configs.dashboard_query_ids[0]['date_filter']))

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
        selector = '"' + col[0] +'"'
    selectors = selectors + selector + " as " + new_col + ","

params = params[:-1]
selectors = selectors[:-1]

main_sql_to_insert = """\n\nselect %d as dashboard_id
		,%d as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
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

data = """insert into APPLAYDU_NOT_CERTIFIED.apd_report_%d 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,%s)\n"""%(dashboard_id,params) + data


# Save the content to the output file
with open(output_file_path, 'w') as file:
    file.write(data)
    print("The file has been saved to %s" % output_file_path)