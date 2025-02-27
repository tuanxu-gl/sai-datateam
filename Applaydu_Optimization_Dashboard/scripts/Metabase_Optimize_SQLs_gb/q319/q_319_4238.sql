DECLARE r319_q4238 ARRAY<STRUCT<total_users FLOAT64,sum_toy_unlocked_count FLOAT64,sum_scan_mode_finished_count FLOAT64,total_scans FLOAT64,`Average Toys Scanned per User` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4238
);

IF row_count = 0 THEN
  SET r319_q4238 = (
    SELECT ARRAY(
      with bq4238 as (select 0)
--main query
SELECT AS STRUCT 
    COUNT(DISTINCT user_id) AS total_users,
    SUM(toy_unlocked_by_scan_count) AS sum_toy_unlocked_count,
    SUM(scan_mode_finished_count) AS sum_scan_mode_finished_count,
    SUM(toy_unlocked_by_scan_count) + SUM(scan_mode_finished_count) AS total_scans,
    (SUM(toy_unlocked_by_scan_count) + SUM(scan_mode_finished_count)) / COUNT(DISTINCT user_id) AS `Average Toys Scanned per User`
FROM 
    `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
JOIN 
    (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 and install_source in (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
            AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    ) USING (user_id)
WHERE 
    DATE(server_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND DATE(server_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(server_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])   
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])   
    AND shop_filter IN (SELECT shop_filter FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])	
    AND (toy_unlocked_by_scan_count > 0 OR scan_mode_finished_count > 0)
    )
  );
  
ELSE
  SET r319_q4238 = (
    SELECT ARRAY_AGG(
      STRUCT(
        value1 as total_users,value2 as sum_toy_unlocked_count,value3 as sum_scan_mode_finished_count,value4 as total_scans,value5 as `Average Toys Scanned per User`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4238 
  );
END IF;

SELECT * FROM UNNEST(r319_q4238);
