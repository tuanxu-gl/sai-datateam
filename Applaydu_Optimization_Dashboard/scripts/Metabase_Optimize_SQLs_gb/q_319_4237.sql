DECLARE r319_q4237 ARRAY<STRUCT<`Total time spent` INT64,`Total Session` INT64,time_result INT64,`Average Time per Users` STRING>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4237
);

IF row_count = 0 THEN
  SET r319_q4237 = (
    SELECT ARRAY(
      with gb4237 as (select 0)
--main query
SELECT AS STRUCT 
    SUM(CAST(time_spent AS INT64)) AS `Total time spent`,
    SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT64) >= 30) THEN 1 ELSE 0 END) AS `Total Session`,
    SUM(CAST(time_spent AS INT64)) / SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT64) >= 30) THEN 1 ELSE 0 END) AS time_result,
    FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(CAST(time_spent AS INT64)) / SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT64) >= 30) THEN 1 ELSE 0 END) AS INT64))) AS `Average Time per Users`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
JOIN 
    (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 
            AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
            AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    ) USING (user_id)
WHERE 
    CAST(time_spent AS INT64) >= 0
    AND CAST(time_spent AS INT64) < 86400
    AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{from_version}}]]) 
    AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{to_version}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) 
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND (DATE(client_time) >= '2020-08-10' AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
    )
  );
  
ELSE
  SET r319_q4237 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as `Total time spent`, CAST(value2 as INT64) as `Total Session`, CAST(value3 as INT64) as time_result, CAST(value4_str as STRING) as `Average Time per Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4237 
  );
END IF;

SELECT * FROM UNNEST(r319_q4237);
