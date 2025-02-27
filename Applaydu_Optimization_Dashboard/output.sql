DECLARE r319_q4250 ARRAY<STRUCT<`kpi` STRING,`value` STRING>>;

DECLARE row_count FLOAT64;
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;
  DECLARE iua_filter ARRAY<STRING>;
  DECLARE ishop_filter ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]);
  SET iua_filter = ARRAY(SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]]);
  SET ishop_filter = ARRAY(SELECT shop_filter FROM `applaydu.tbl_shop_filter` WHERE 1=1  [[AND {{ishopfilter}}]]);
          
SET row_count = (
 SELECT COUNT(0) 
 FROM `applaydu.apd_report_319`
 WHERE 1=1 
  AND DATE(start_date) = GREATEST(istart_date, '2020-08-10')
  AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
  AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
  AND dashboard_id = 319 
  AND query_id = 4250
);

IF row_count = 0 THEN
 SET r319_q4250 = (
  SELECT ARRAY(
   WITH db319_q4243 as (select 0), gb4250 as (SELECT 0)
,tbl_install as (
  SELECT user_id, install_date
  FROM `gcp-bi-elephant-db-gold.applaydu.user_activity`
  WHERE 1=1 
    AND install_country IN UNNEST(icountry_region)
    AND install_source IN (SELECT install_source FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` WHERE 1=1 [[AND {{iinstallsource}}]])
    AND game_id IN UNNEST(ishop_filter)
    AND DATE(ACTIVE_DATE) >= istart_date
    AND DATE(ACTIVE_DATE) < iend_date
  GROUP BY 1 
),
t_users as (
  SELECT user_id,
      SUM(sessions_count) as sessions_count,
      SUM(total_time_spent) as total_time_spent,
      SUM(toy_unlocked_by_scan_count) + SUM(scan_mode_finished_count) as scans
  FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_users`
  JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 
      AND install_source IN UNNEST(iua_filter)
      AND DATE(ACTIVE_DATE) >= istart_date
      AND DATE(ACTIVE_DATE) < iend_date
  ) USING (user_id)
  WHERE DATE(server_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND DATE(server_date) >= istart_date
    AND DATE(server_date) < iend_date
    AND country_name IN UNNEST(icountry_region)
    AND version >= (ifrom_version)
    AND version <= (ito_version)
    AND version IN UNNEST(iversions)
    AND game_id IN UNNEST(ishop_filter)
  GROUP BY user_id
),
tbl_launch_resume_src as (
  SELECT 'All' as period,
      SUM(time_spent) as `Total time spent`,
      COUNT(DISTINCT user_id) AS `Total Users`,
      SUM(time_spent) / COUNT(DISTINCT user_id) as time_result,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result)) AS `Average Time per Users`,
      SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT) >= 30) THEN 1 ELSE 0 END) AS `Total Sessions`,
      SUM(time_spent) / SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT) >= 30) THEN 1 ELSE 0 END) as time_result_sessions,
      --FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(time_result_sessions)) AS `Average Time per Session`,
      SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT) >= 30) THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) as `Average Session per User`,
      SUM(CASE WHEN install_date >= istart_date 
          AND install_date < iend_date 
          THEN CAST(time_spent AS INT) ELSE 0 END) as `Total Time Spent New users`,
      COUNT(DISTINCT CASE WHEN install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) AS `Total New Users`,
      SUM(CASE WHEN install_date >= istart_date 
          AND install_date < iend_date 
          THEN CAST(time_spent AS INT) ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) as time_result_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_new_users)) AS `Average Time Spent Per New Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT) >= 30) 
             AND install_date >= istart_date 
             AND install_date < iend_date) 
          THEN 1 ELSE 0 END) AS `Total Sessions New Users`,
      SUM(CASE WHEN install_date >= istart_date 
          AND install_date < iend_date 
          THEN CAST(time_spent AS INT) ELSE 0 END) / SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT) >= 30) 
             AND install_date >= istart_date 
             AND install_date < iend_date) 
          THEN 1 ELSE 0 END) as time_result_sessions_new_users,
      --FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(time_result_sessions_new_users)) AS `Average Time per Session New Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT) >= 30) 
             AND install_date >= istart_date 
             AND install_date < iend_date) 
          THEN 1 ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) as `Average Session per New User`,
      SUM(CASE WHEN install_date < istart_date 
          THEN CAST(time_spent AS INT) ELSE 0 END) as `Total Time Spent Old users`,
      COUNT(DISTINCT CASE WHEN install_date < istart_date 
                THEN user_id ELSE 0 END) AS `Total Old Users`,
      SUM(CASE WHEN install_date < istart_date 
          THEN CAST(time_spent AS INT) ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date < istart_date 
                THEN user_id ELSE 0 END) as time_result_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_old_users)) AS `Average Time Per Old Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT) >= 30) 
             AND install_date < istart_date) 
          THEN 1 ELSE 0 END) AS `Total Sessions Old Users`,
      SUM(CASE WHEN install_date < istart_date 
          THEN CAST(time_spent AS INT) ELSE 0 END) / SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT) >= 30) 
             AND install_date < istart_date) 
          THEN 1 ELSE 0 END) as time_result_sessions_old_users,
      --FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(time_result_sessions_old_users)) AS `Average Time per Session Old Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT) >= 30) 
             AND install_date < istart_date) 
          THEN 1 ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date < istart_date 
                THEN user_id ELSE 0 END) as `Average Session per Old User`
  FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume`
  LEFT JOIN tbl_install USING (user_id)
  WHERE CAST(time_spent AS INT) >= 0
    AND CAST(time_spent AS INT) < 86400
    AND version >= (ifrom_version)
    AND version <= (ito_version)
    AND version IN UNNEST(iversions)
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND `gcp-bi-elephant-db-gold.applaydu.launch_resume`.country IN UNNEST(icountry_region)
    AND game_id IN UNNEST(ishop_filter)
    AND (DATE(client_time) >= '2020-08-10' AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
),
tbl_users_src as (
  SELECT 'All' as period,
      COUNT(DISTINCT CASE WHEN scans > 0 THEN user_id ELSE 0 END) as scan_users,
      SUM(CASE WHEN scans > 0 THEN total_time_spent ELSE 0 END) as sum_total_time_spent_scan_users,
      SUM(CASE WHEN scans > 0 THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans > 0 THEN user_id ELSE 0 END) as time_result_scan_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_scan_users)) AS `Time Spent Per Scan User`,
      SUM(CASE WHEN scans > 0 THEN sessions_count ELSE 0 END) as scan_sessions_count,
      SUM(CASE WHEN scans > 0 THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans > 0 THEN sessions_count ELSE 0 END) as time_result_session_scan_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_scan_users)) AS `Time Spent Per Session of Scan Users`,
      COUNT(DISTINCT CASE WHEN scans > 0 AND install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) as scan_new_users,
      SUM(CASE WHEN scans > 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_scan_new_users,
      SUM(CASE WHEN scans > 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans > 0 AND install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) as time_result_scan_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_scan_new_users)) AS `Time Spent Per Scan New User`,
      SUM(CASE WHEN scans > 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN sessions_count ELSE 0 END) as scan_sessions_new_users_count,
      SUM(CASE WHEN scans > 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans > 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN sessions_count ELSE 0 END) as time_result_session_scan_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_scan_new_users)) AS `Time Spent Per Session of Scan New Users`,
      COUNT(DISTINCT CASE WHEN scans > 0 AND install_date < istart_date 
                THEN user_id ELSE 0 END) as scan_old_users,
      SUM(CASE WHEN scans > 0 AND install_date < istart_date 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_scan_old_users,
      SUM(CASE WHEN scans > 0 AND install_date < istart_date 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans > 0 AND install_date < istart_date 
                THEN user_id ELSE 0 END) as time_result_scan_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_scan_old_users)) AS `Time Spent Per Scan Old User`,
      SUM(CASE WHEN scans > 0 AND install_date < istart_date 
          THEN sessions_count ELSE 0 END) as scan_sessions_old_users_count,
      CASE WHEN SUM(CASE WHEN scans > 0 AND install_date < istart_date 
          THEN sessions_count ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN scans > 0 AND install_date < istart_date 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans > 0 AND install_date < istart_date 
          THEN sessions_count ELSE 0 END) END as time_result_session_scan_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_scan_old_users)) AS `Time Spent Per Session of Scan Old Users`,
      COUNT(DISTINCT CASE WHEN scans = 0 THEN user_id ELSE 0 END) as free_users,
      SUM(CASE WHEN scans = 0 THEN total_time_spent ELSE 0 END) as sum_total_time_spent_free_users,
      SUM(CASE WHEN scans = 0 THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans = 0 THEN user_id ELSE 0 END) as time_result_free_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_free_users)) AS `Time Spent Per Free User`,
      SUM(CASE WHEN scans = 0 THEN sessions_count ELSE 0 END) as scan_sessions_free_users_count,
      CASE WHEN SUM(CASE WHEN scans = 0 THEN sessions_count ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN scans = 0 THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans = 0 THEN sessions_count ELSE 0 END) END as time_result_session_free_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_free_users)) AS `Time Spent Per Session of Free Users`,
      COUNT(DISTINCT CASE WHEN scans = 0 AND install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) as free_new_users,
      SUM(CASE WHEN scans = 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_free_new_users,
      SUM(CASE WHEN scans = 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans = 0 AND install_date >= istart_date 
                AND install_date < iend_date 
                THEN user_id ELSE 0 END) as time_result_free_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_free_new_users)) AS `Time Spent Per Free New User`,
      SUM(CASE WHEN scans = 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN sessions_count ELSE 0 END) as scan_sessions_new_free_users_count,
      CASE WHEN SUM(CASE WHEN scans = 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN sessions_count ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN scans = 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans = 0 AND install_date >= istart_date 
          AND install_date < iend_date 
          THEN sessions_count ELSE 0 END) END as time_result_session_free_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_free_new_users)) AS `Time Spent Per Session of Free New Users`,
      COUNT(DISTINCT CASE WHEN scans = 0 AND install_date < istart_date 
                THEN user_id ELSE 0 END) as free_old_users,
      SUM(CASE WHEN scans = 0 AND install_date < istart_date 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_free_old_users,
      SUM(CASE WHEN scans = 0 AND install_date < istart_date 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans = 0 AND install_date < istart_date 
                THEN user_id ELSE 0 END) as time_result_free_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_free_old_users)) AS `Time Spent Per Free Old User`,
      SUM(CASE WHEN scans = 0 AND install_date < istart_date 
          THEN sessions_count ELSE 0 END) as scan_sessions_free_old_users_count,
      CASE WHEN SUM(CASE WHEN scans = 0 AND install_date < istart_date 
          THEN sessions_count ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN scans = 0 AND install_date < istart_date 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans = 0 AND install_date < istart_date 
          THEN sessions_count ELSE 0 END) END as time_result_session_free_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_free_old_users)) AS `Time Spent Per Session of Free Old Users`
  FROM t_users
  LEFT JOIN tbl_install USING (user_id)
)
,result as (
SELECT '01.Average Time per Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result AS INT64))) as value from tbl_launch_resume_src
union all SELECT '02.Average Time Spent Per New Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_new_users AS INT64))) as value from tbl_launch_resume_src
union all SELECT '03.Average Time Per Old Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_old_users AS INT64))) as value from tbl_launch_resume_src
union all SELECT '04.Time Spent Per Scan User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_scan_users AS INT64))) as value from tbl_users_src
union all SELECT '05.Time Spent Per Scan New User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_scan_new_users AS INT64))) as value from tbl_users_src
union all SELECT '06.Time Spent Per Scan Old User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_scan_old_users AS INT64))) as value from tbl_users_src
union all SELECT '07.Time Spent Per Free User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_free_users AS INT64))) as value from tbl_users_src
union all SELECT '08.Time Spent Per Free New User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_free_new_users AS INT64))) as value from tbl_users_src
union all SELECT '09.Time Spent Per Free Old User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_free_old_users AS INT64))) as value from tbl_users_src
union all SELECT '10.Average Time per Session' as kpi, FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_sessions AS INT64))) as value from tbl_launch_resume_src
union all SELECT '11.Average Time per Session New Users' as kpi, FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_sessions_new_users AS INT64))) as value from tbl_launch_resume_src
union all SELECT '12.Average Time per Session Old Users' as kpi, FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_sessions_old_users AS INT64))) as value from tbl_launch_resume_src
union all SELECT '13.Time Spent Per Session of Scan Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_scan_users AS INT64))) as value from tbl_users_src
union all SELECT '14.Time Spent Per Session of Scan New Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_scan_new_users AS INT64))) as value from tbl_users_src
union all SELECT '15.Time Spent Per Session of Scan Old Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_scan_old_users AS INT64))) as value from tbl_users_src
union all SELECT '16.Time Spent Per Session of Free Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_free_users AS INT64))) as value from tbl_users_src
union all SELECT '17.Time Spent Per Session of Free New Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_free_new_users AS INT64))) as value from tbl_users_src
union all SELECT '18.Time Spent Per Session of Free Old Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_free_old_users AS INT64))) as value from tbl_users_src
union all SELECT '19.Average Session per User' as kpi, CAST(`Average Session per User` as STRING) as value from tbl_launch_resume_src
union all SELECT '20.Average Session per New User' as kpi, CAST(`Average Session per New User` as STRING) as value from tbl_launch_resume_src
union all SELECT '21.Average Session per Old User' as kpi, CAST(`Average Session per Old User` as STRING) as value from tbl_launch_resume_src
order by kpi asc
)
--main query
SELECT AS STRUCT * from result
  )
 );
 
ELSE
 SET r319_q4250 = (
  SELECT ARRAY_AGG(
   STRUCT(
     CAST(value1_str as STRING) as `kpi`, CAST(value2_str as STRING) as `value`
   )
  )
  FROM 
   `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
  WHERE 
   DATE(start_date) = GREATEST(istart_date, '2020-08-10')
   AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
   AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
   AND dashboard_id = 319 
   AND query_id = 4250 
 );
END IF;

SELECT * FROM UNNEST(r319_q4250);
