DECLARE r319_q4261 ARRAY<STRUCT<`Start of Week` STRING,`Active Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4261
);

IF row_count = 0 THEN
  SET r319_q4261 = (
    SELECT ARRAY(
      with gb4261 as (SELECT 0)
--main query
SELECT AS STRUCT cast(DATE_TRUNC(DATE(client_time), WEEK) as string) AS `Start of Week`,
       COUNT(DISTINCT user_id) AS `Active Users` 
FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` a
JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 
    AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1 [[AND {{iinstallsource}}]])
) USING (user_id)
WHERE 1=1
    AND NOT (game_id = 82471 AND client_time < '2020-12-14')
    AND DATE(client_time) >= '2020-08-10' 
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
GROUP BY `Start of Week`
ORDER BY `Start of Week`
    )
  );
  
ELSE
  SET r319_q4261 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Start of Week`, CAST(value2 as INT64) as `Active Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4261 
  );
END IF;

SELECT * FROM UNNEST(r319_q4261);
