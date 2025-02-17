DECLARE r319_q4232 ARRAY<STRUCT<Shop STRING,Total_Users INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4232
);

IF row_count = 0 THEN
  SET r319_q4232 = (
    SELECT ARRAY(
      with gb4232 as (SELECT 0)
--main query
SELECT AS STRUCT 
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cast(game_id as string), 
        '81335', 'App Store')
        ,'81337', 'Google Play')
        , '82471','AppInChina')
        , '84155','Google Play')
        , '84515','Samsung')
        , '84137','AppInChina') AS Shop,
    COUNT(DISTINCT user_id) AS Total_Users
FROM 
    `gcp-bi-elephant-db-gold.applaydu.user_activity`
WHERE 
    1=1 
    AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
    AND NOT (game_id = 82471 AND active_date < '2020-12-14')
    AND (DATE(active_date) >= '2020-08-10' AND DATE(active_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
    AND CAST(time_spent AS FLOAT64) >= 0 
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])
    AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{from_version}}]]) 
    AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{to_version}}]])
GROUP BY 
    Shop
ORDER BY 
    2 DESC
    )
  );
  
ELSE
  SET r319_q4232 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as Shop, CAST(value2 as INT64) as Total_Users
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4232 
  );
END IF;

SELECT * FROM UNNEST(r319_q4232);
