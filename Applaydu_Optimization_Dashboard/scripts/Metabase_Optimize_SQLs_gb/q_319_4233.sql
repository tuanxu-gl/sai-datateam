DECLARE r319_q4233 ARRAY<STRUCT<`Time spent` STRING>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4233
);

IF row_count = 0 THEN
  SET r319_q4233 = (
    SELECT ARRAY(
      WITH gb4233 as (SELECT 0)
,t_users AS (
    SELECT 
        user_id,
        SUM(total_time_spent) AS total_time_spent,
        SUM(toy_unlocked_by_scan_count) AS toy_unlocked_by_scan_count,
        SUM(scan_mode_finished_count) AS scan_mode_finished_count
    FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
    JOIN 
        (
            SELECT DISTINCT user_id 
            FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
            WHERE 1=1 
                AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
                AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
                AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        ) USING (user_id)
    JOIN 
        `applaydu.tbl_shop_filter` using (game_id  ,country_name)
    WHERE 
        DATE(server_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND DATE(server_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])   
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])    	
        [[AND {{ishopfilter}}]]
    GROUP BY 
        user_id
)
--main query
SELECT AS STRUCT `Time spent`
FROM (
    SELECT 
        COUNT(DISTINCT user_id) AS users,
        SUM(total_time_spent) AS sum_total_time_spent,
        SUM(total_time_spent) / COUNT(DISTINCT user_id) AS time_result,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent`
    FROM 
        t_users
    WHERE 
        toy_unlocked_by_scan_count > 0 
        OR scan_mode_finished_count > 0 
)
    )
  );
  
ELSE
  SET r319_q4233 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Time spent`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4233 
  );
END IF;

SELECT * FROM UNNEST(r319_q4233);
