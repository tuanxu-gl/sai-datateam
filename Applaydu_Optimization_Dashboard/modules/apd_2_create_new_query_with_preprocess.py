from modules import apd_optimize_metabase_preprocess as optimizer
#from modules import apd_preprocess_consumer_data as preprocess
from utils import ultils as ultils
import configs

def start(dashboard_query_id):
  dashboard_id = dashboard_query_id['dashboard']
  query_id = dashboard_query_id['query']

  # Define the input and output file paths
  input_file_path = configs.SQLs_Path+'i%d/i_%d_%d.sql'%(dashboard_id,dashboard_id,query_id)
  output_file_path = configs.SQLs_Path+'q%d'%dashboard_id + '/q_%d_%d.sql'%(dashboard_id, query_id)

  # Load the text file
  with open(input_file_path, 'r') as file:
      data = file.read()

      # Remove the specific text from the data


      text_to_replaces = [
          ['"', '`'],
          ["select", "SELECT"],
          ["server_time", "client_time"],
          ["from tbl_date_filter where 1=1", "from tbl_date_filter where 2=2"],
          ["from tbl_country_filter where 1=1", "from tbl_country_filter where 2=2"],
          #["where 1=1", "where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'"],
          #["WHERE 1=1", "where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'"],
        ["min(client_time),min(client_time)", "min(client_time),min(server_time)"],
        ["client_time AS client_time,client_time AS client_time", "client_time AS client_time,server_time AS server_time"],
            ['APPLAYDU_NOT_CERTIFIED', '`gcp-gfb-sai-tracking-gold.applaydu`'],

      ]
      
      
      for text_to_replace in text_to_replaces:
          data = data.replace(text_to_replace[0], text_to_replace[1])

  # Remove empty lines
  data = "\n".join([line for line in data.split("\n") if line.strip() != ""])

  params = ""
  selectors = ""
  for i, col in enumerate(dashboard_query_id['columns']):
      
      type_column = col[1]
      new_selector = col[0]

      if(new_selector not in configs.variables_keep_original):
          new_selector = '`' + new_selector +'`'

      new_type = 'FLOAT64'
      value_column = "value%d" % (i+1)

      if type_column == 'string':
          new_type = 'STRING'
          value_column = "value%d_str" % (i+1)
      elif type_column == 'int':
          new_type = 'INT64'

      params = params + new_selector + " " + new_type + ","
      selectors = selectors + " CAST(" + value_column + " as " + new_type + ") as " + new_selector + ","

  params = params[:-1]
  selectors = selectors[:-1]

  part1 = """DECLARE r%d_q%d ARRAY<STRUCT<%s>>;
  DECLARE row_count FLOAT64;
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]);

  
  SET row_count = (
    SELECT COUNT(0) 
    FROM `applaydu.apd_report_%d`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = %d 
      AND query_id = %d
  );
  """%(dashboard_id, query_id, params, dashboard_id, dashboard_id, query_id)

  data = data.replace('--main query\nSELECT', '--main query\nSELECT AS STRUCT')

  filter_to_replaces = [
      #start date
      ['(SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]])', 'istart_date'],
      ['(SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])','istart_date'],

      #end date
      ['DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)', 'iend_date'],
      ['DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)', 'iend_date'],
      ['DATE_ADD((SELECT max(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)', 'iend_date'],
      ['DATE_ADD((SELECT max(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)', 'iend_date'],
      
      #country
      ["(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])",'UNNEST(icountry_region)'],
      ["(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])",'UNNEST(icountry)'],
    #in versions list
    ["(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])",'UNNEST(iversions)'],
      #min version
      ["SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]",'ifrom_version'],

      #max version
      ["SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]]",'ito_version'],
      #date variables
      ["SELECT CAST(ivalue AS DATE) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v4_start_date'","'2023-08-22'"],
  ]
  for filter_to_replace in filter_to_replaces:
      data = data.replace(filter_to_replace[0], filter_to_replace[1])

  part2 ="""
  IF row_count = 0 THEN
    SET r%d_q%d = (
      SELECT ARRAY(
        %s
      )
    );
    """%(dashboard_id, query_id,data)

  part3 = """
  ELSE
    SET r%d_q%d = (
      SELECT ARRAY_AGG(
        STRUCT(
          %s
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_%d`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = %d 
        AND query_id = %d 
    );
  END IF;

  SELECT * FROM UNNEST(r%d_q%d);
  """%(dashboard_id, query_id, selectors, dashboard_id, dashboard_id, query_id, dashboard_id, query_id)


  data = part1 + part2 + part3

  # Save the content to the output file
  with open(output_file_path, 'w') as file:
      file.write(data)
      print('File saved to %s' % output_file_path)