# Load the input SQL file
with open('input.sql', 'r') as file:
    data = file.read()

    text_to_replaces = [
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
            ['client_time,client_time', 'client_time'],
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
            ['tbl_version_filter', 'applaydu_not_certified.tbl_version_filter'],
            ['tbl_country_filter', 'applaydu_not_certified.tbl_country_filter'],
            ['tbl_date_filter', 'applaydu_not_certified.tbl_date_filter'],
            ['tbl_variables', 'applaydu_not_certified.tbl_variables'],
            ['applaydu_not_certified.applaydu_not_certified', 'applaydu_not_certified'],
           
            ["and (select available from tbl_check_preprocess_report)='N/A'", ''],
            ["and tbl_check_preprocess_report.available='N/A'", ''],
      ]
      
      
    for text_to_replace in text_to_replaces:
          data = data.replace(text_to_replace[0], text_to_replace[1])

    part1 = """
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM applaydu_not_certified.tbl_version_filter WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM applaydu_not_certified.tbl_version_filter WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM applaydu_not_certified.tbl_version_filter WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM applaydu_not_certified.tbl_country_filter WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM applaydu_not_certified.tbl_country_filter WHERE 1=1 [[AND {{icountry}}]]);

  
  """

    #data = data.replace('--main query\nSELECT', '--main query\nSELECT AS STRUCT')

   
    filter_to_replaces = [
      # start date
      ['(select min(server_date) from applaydu_not_certified.tbl_date_filter where 2=2 [[and {{idate}}]])', 'istart_date'],
      ['(select min(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 [[and {{idate}}]])', 'istart_date'],

      # end date
      ['date_add((select max(server_date) from applaydu_not_certified.tbl_date_filter where 2=2 [[and {{idate}}]]), interval 1 day)', 'iend_date'],
      ['date_add((select max(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 [[and {{idate}}]]), interval 1 day)', 'iend_date'],
      ['date_add((select max(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 [[and {{idate}}]]), interval 1 day)', 'iend_date'],
      ['date_add((select max(server_date) from applaydu_not_certified.tbl_date_filter where 2=2 [[and {{idate}}]]), interval 1 day)', 'iend_date'],
      ['dateadd(day, 1,(select max(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 [[and {{idate}}]]))', 'iend_date'],
      # country
      ['(select country from applaydu_not_certified.tbl_country_filter where 1=1 [[and {{icountry}}]] [[and {{iregion}}]])', 'unnest(icountry_region)'],
      ['(select country from applaydu_not_certified.tbl_country_filter where 1=1 [[and {{icountry}}]])', 'unnest(icountry)'],
      
      # in versions list
      ['(select version from applaydu_not_certified.tbl_version_filter where 1=1 [[and {{iversion}}]])', 'unnest(iversions)'],
      
      # min version
      ['select min(version) from applaydu_not_certified.tbl_version_filter where 1=1 [[and {{from_version}}]]', 'ifrom_version'],
      ['(select min(version) from tbl_version_filter where 1=1 [[and {{from_version}}]])', 'ifrom_version'],
      
      # max version
      ['select max(version) from applaydu_not_certified.tbl_version_filter where 1=1 [[and {{to_version}}]]', 'ito_version'],
      ['(select max(version) from tbl_version_filter where 1=1 [[and {{to_version}}]])', 'ito_version'],
      
      # date variables
      ['select cast(ivalue as date) from applaydu_not_certified.tbl_variables where ikey = \'apd_v4_start_date\'', '\'2023-08-22\''],
  ]
    #for filter_to_replace in filter_to_replaces:
    #    data = data.replace(filter_to_replace[0], filter_to_replace[1])
      
    #data = part1 + data
# Save the modified SQL query to the output file
with open('output.sql', 'w') as file:
    file.write(data)