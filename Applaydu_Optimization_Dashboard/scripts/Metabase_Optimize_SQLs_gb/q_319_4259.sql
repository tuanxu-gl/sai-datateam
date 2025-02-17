DECLARE r319_q4259 ARRAY<STRUCT<`Number of Sessions` INT64,Total_Users INT64,`Average Session per User` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4259
);

IF row_count = 0 THEN
  SET r319_q4259 = (
    SELECT ARRAY(
      with gb4259 as (select 0)
--main query
SELECT AS STRUCT 
    SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT) >= 30) THEN 1 ELSE 0 END) AS `Number of Sessions`,
    COUNT(DISTINCT user_id) AS Total_Users,
    SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT) >= 30) THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) AS `Average Session per User`
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
    CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND (DATE(client_time) >= '2020-08-10' AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
    AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{from_version}}]]) 
    AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{to_version}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) 
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
    [[AND {{ishopfilter}}]]
    )
  );
  
ELSE
  SET r319_q4259 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as `Number of Sessions`, CAST(value2 as INT64) as Total_Users, CAST(value3 as FLOAT64) as `Average Session per User`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4259 
  );
END IF;

SELECT * FROM UNNEST(r319_q4259);
