DECLARE r14_q252 ARRAY<STRUCT<`Month` INT64,`Year` INT64,`Time` STRING,`Monthly Active Users` INT64>>;
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
    FROM `applaydu.apd_report_14`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 14 
      AND query_id = 252
  );
  
  IF row_count = 0 THEN
    SET r14_q252 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    EXTRACT(MONTH FROM client_time) AS `Month`,
    EXTRACT(YEAR FROM client_time) AS `Year`,
    CONCAT(CAST(EXTRACT(YEAR FROM client_time) AS STRING), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
    COUNT(DISTINCT user_id) AS `Monthly Active Users`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume`
WHERE 1=1
    AND NOT (game_id = 82471 AND client_time < '2020-12-14')
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND country IN UNNEST(icountry)    
    AND version IN UNNEST(iversions)
GROUP BY all
      )
    );
    
  ELSE
    SET r14_q252 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1 as INT64) as `Month`, CAST(value2 as INT64) as `Year`, CAST(value3_str as STRING) as `Time`, CAST(value4 as INT64) as `Monthly Active Users`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 252 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q252);
  