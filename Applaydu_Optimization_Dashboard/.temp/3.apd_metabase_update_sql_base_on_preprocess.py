from modules import apd_optimize_metabase_preprocess as optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils
import configs

dashboard_id = configs.dashboard_query_ids[0]['dashboard']
query_id = configs.dashboard_query_ids[0]['query']

# Define the input and output file paths
input_file_path = configs.SQLs_Path+ 'input.sql'
output_file_path = configs.SQLs_Path+'q_%d_%d.sql'%(dashboard_id, query_id)

# Load the text file
with open(input_file_path, 'r') as file:
    data = file.read()

    # Remove the specific text from the data

    



    

    text_to_removes = [
        #Update date/country filter from 1=1 to 2=2
        ["from tbl_date_filter where 1=1", "from tbl_date_filter where 2=2"],
        ["from tbl_country_filter where 1=1", "from tbl_country_filter where 2=2"],
        ["where 1=1", "where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'"],
        ["WHERE 1=1", "where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'"],
       
    ]
    
    
    for text_to_remove in text_to_removes:
        data = data.replace(text_to_remove[0], text_to_remove[1])

# Remove empty lines
data = "\n".join([line for line in data.split("\n") if line.strip() != ""])


selectors = ""
for i, col in enumerate(configs.dashboard_query_ids[0]['columns']):
    new_col = "value%d" % (i+1)
    new_selector = col[0]
    if col[1] == 'string':
        new_col = "value%d_str" % (i+1)

    
    selector = col[0]
    if(col[0] not in configs.variables_keep_original):
        selector = '"' + col[0] +'"'
    selectors = selectors + new_col + " as " + selector + ","


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



top_query ="""with r%d as(
SELECT %s
FROM APPLAYDU_NOT_CERTIFIED.apd_report_%d
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=%d and query_id = %d 
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM APPLAYDU_NOT_CERTIFIED.apd_report_%d
        WHERE 1=1
        AND start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
		 and dashboard_id=%d and query_id = %d
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
,""" %(query_id,selectors,dashboard_id,dashboard_id,query_id,dashboard_id,dashboard_id,query_id)

data = data.replace('server_time', 'client_time')
data = data.replace('with', top_query)

main_query= """
select * from r%d
union
select * from
(
"""%query_id
data = data.replace('--main query', main_query)

first_col = configs.dashboard_query_ids[0]['columns'][0][0]
if(first_col not in configs.variables_keep_original):
        first_col = '"' + first_col +'"'
bot_query = """
)
where %s > 0
"""%first_col


data = data + bot_query
# Save the content to the output file
with open(output_file_path, 'w') as file:
    file.write(data)
    print('File saved to %s' % output_file_path)