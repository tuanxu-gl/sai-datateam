# Load the input SQL file
with open('input.sql', 'r') as file:
    data = file.read()

         #replace the text specific
    replacements = [
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'", "'2024-08-28'"),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_kinderini_start_date'", "'2024-10-19'"),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'", "'2024-08-28'"),
            ("select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'", "'2024-08-28'"),
            ("(SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v4_start_date')", "'2023-08-22'"),
            ("(SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_be_parent_registration_start_date')","'2024-10-02'"),
            ("(SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')","'2024-08-28'"),
        ] 

    for old, new in replacements:
        data = data.replace(old, new)

    filter_to_replaces = [
      
      ['  ', ' '],
      ['server_time', 'client_time'],

      ["(SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])",'(SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])'],
      ["DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)",
        'DATE_ADD((SELECT max(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)'],
      ['with', 'WITH'],
      ['WITH', 'WITH db319_q4243 as (select 0),'],
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
      ["(SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]) ",'UNNEST(icountry_region)'],
      ["(SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])",'UNNEST(icountry_region)'],
   
      ["(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])",'UNNEST(icountry)'],
      ["(SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]) ",'UNNEST(icountry)'],
    #in versions list
      ["(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])",'UNNEST(iversions)'],
    #min version
      ["SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]",'ifrom_version'],
    #max version
      ["SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]]",'ito_version'],

    #in ua_filter list
      ["(SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1 [[AND {{iinstallsource}}]])",'UNNEST(iua_filter)'],
    
    #in shop_filter list
      ["(SELECT shop_filter FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])	",'UNNEST(ishop_filter)'],
      ["(SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])",'UNNEST(ishop_filter)'],
      #date variables
      ["SELECT CAST(ivalue AS DATE) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v4_start_date'","'2023-08-22'"],
  ]
    for filter_to_replace in filter_to_replaces:
      data = data.replace(filter_to_replace[0], filter_to_replace[1])
    text_to_replaces = [
        
        ['DECLARE row_count INT64;','DECLARE row_count FLOAT64;'],
         ["DECLARE row_count FLOAT64;", """DECLARE row_count FLOAT64;
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;
  DECLARE iua_filter ARRAY<STRING>;
  DECLARE ishop_filter ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]);
  SET iua_filter = ARRAY(SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]]);
  SET ishop_filter = ARRAY(SELECT shop_filter FROM `applaydu.tbl_shop_filter` WHERE 1=1  [[AND {{ishopfilter}}]]);
          """],
          
      ]
      
    for text_to_replace in text_to_replaces:
        data = data.replace(text_to_replace[0], text_to_replace[1])
# Save the modified SQL query to the output file


with open('output.sql', 'w') as file:
    file.write(data)