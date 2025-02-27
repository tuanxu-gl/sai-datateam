DECLARE r14_q245 ARRAY<STRUCT<`Time spent` STRING>>;
  DECLARE istart_date date;
  DECLARE iend_date date;
  DECLARE row_count FLOAT64;
  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT max(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);

  
  SET row_count = (
    SELECT COUNT(0) 
    FROM `applaydu.apd_report_14`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 14 
      AND query_id = 245
  );
  
  IF row_count = 0 THEN
    SET r14_q245 = (
      SELECT ARRAY(
        --main query
WITH t_users AS (
    SELECT 
        user_id,
        SUM(total_time_spent) AS total_time_spent,
        SUM(toy_unlocked_by_scan_count) AS toy_unlocked_by_scan_count,
        SUM(scan_mode_finished_count) AS scan_mode_finished_count
    FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
    JOIN 
        `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name
    WHERE 1=1
        AND DATE(server_date) >= istart_date
        AND DATE(server_date) < DATE_ADD(iend_date, INTERVAL 1 DAY)
        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])   
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])    	
        [[AND {{ishopfilter}}]]
    GROUP BY 
        user_id
)
SELECT `Time spent`
FROM (
    SELECT 
        COUNT(DISTINCT user_id) AS users,
        SUM(total_time_spent) AS sum_total_time_spent,
        SUM(total_time_spent) / COUNT(DISTINCT user_id) AS time_result,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent`
    FROM 
        t_users
    WHERE 
        toy_unlocked_by_scan_count > 0 OR scan_mode_finished_count > 0 
)
WHERE `Time spent` IS NOT NULL
      )
    );
    
  ELSE
    SET r14_q245 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as `Time spent`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 245 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q245);
  