DECLARE r319_q4252 ARRAY<STRUCT<month STRING,`D0` INT64,`D1` FLOAT64,`D3` FLOAT64,`D7` FLOAT64,`D28` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4252
);

IF row_count = 0 THEN
  SET r319_q4252 = (
    SELECT ARRAY(
      WITH gb4276 as (SELECT 0),
scan_profile AS (
    SELECT DISTINCT user_id 
    FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_users`
    WHERE (toy_unlocked_by_scan_count > 0 OR scan_mode_finished_count > 0)
),
t1 AS (
    SELECT
        user_id, 
        DATE_TRUNC(DATE(client_time), DAY) AS login_day,
        MIN(DATE_TRUNC(DATE(client_time), DAY)) OVER (PARTITION BY user_id) AS first_day,
        MIN(version) OVER (PARTITION BY user_id) AS first_version,
        MIN(session_id) OVER (PARTITION BY user_id) AS first_session,
        FORMAT_DATE('%A', MIN(DATE_TRUNC(DATE(client_time), DAY)) OVER (PARTITION BY user_id)) AS first_weekday,
        DATE_DIFF(DATE(client_time), MIN(DATE_TRUNC(DATE(client_time), DAY)) OVER (PARTITION BY user_id), DAY) AS subsequent_day
    FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
    JOIN scan_profile USING (user_id)
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 
        AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1 [[AND {{iinstallsource}}]])
    ) USING (user_id)
    JOIN `applaydu.tbl_shop_filter` ON `applaydu.tbl_shop_filter`.game_id = t.game_id AND `applaydu.tbl_shop_filter`.country = t.country 
    WHERE 1=1
        AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        [[AND {{ishopfilter}}]]
),
t2 AS (
    SELECT 
        DATE_TRUNC(first_day, MONTH) AS first_month,
        subsequent_day,
        COUNT(DISTINCT user_id) AS users
    FROM t1
    WHERE 1=1
        AND first_day >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND first_day < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND first_version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND first_day >= '2023-12-01' 
        AND first_day < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND first_weekday IN ('Friday', 'Saturday')
    GROUP BY first_month, subsequent_day
)
--main query
SELECT AS STRUCT 
    cast(first_month as string) AS month,
    SUM(CASE WHEN subsequent_day = 0 THEN users ELSE 0 END) AS D0,
    SUM(CASE WHEN subsequent_day = 1 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day = 0 THEN users ELSE 0 END) AS D1,
    SUM(CASE WHEN subsequent_day = 3 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day = 0 THEN users ELSE 0 END) AS D3,
    SUM(CASE WHEN subsequent_day = 7 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day = 0 THEN users ELSE 0 END) AS D7,
    SUM(CASE WHEN subsequent_day = 28 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day = 0 THEN users ELSE 0 END) AS D28
FROM t2
GROUP BY month
ORDER BY month
    )
  );
  
ELSE
  SET r319_q4252 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as month, CAST(value2 as INT64) as `D0`, CAST(value3 as FLOAT64) as `D1`, CAST(value4 as FLOAT64) as `D3`, CAST(value5 as FLOAT64) as `D7`, CAST(value6 as FLOAT64) as `D28`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4252 
  );
END IF;

SELECT * FROM UNNEST(r319_q4252);
