DECLARE r14_q256 ARRAY<STRUCT<`Total time spent` INT64,`Total Users` INT64,time_result FLOAT64,`Average Time per Users` STRING>>;
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
      AND query_id = 256
  );
  
  IF row_count = 0 THEN
    SET r14_q256 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    SUM(CAST(time_spent AS INT64)) AS `Total time spent`,
    COUNT(DISTINCT user_id) AS `Total Users`,
    SUM(CAST(time_spent AS INT64)) / COUNT(DISTINCT user_id) AS time_result,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Average Time per Users`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
JOIN 
    `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country = t.country
WHERE 1=1
    AND CAST(time_spent AS INT64) >= 0
    AND CAST(time_spent AS INT64) < 86400
    AND version >= (ifrom_version) 
    AND version <= (ito_version)
    AND version IN UNNEST(iversions)
    AND client_time >= istart_date 
    AND client_time < iend_date
    AND t.country IN UNNEST(icountry_region)
    [[AND {{ishopfilter}}]]
      )
    );
    
  ELSE
    SET r14_q256 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1 as INT64) as `Total time spent`, CAST(value2 as INT64) as `Total Users`, CAST(value3 as FLOAT64) as time_result, CAST(value4_str as STRING) as `Average Time per Users`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 256 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q256);
  