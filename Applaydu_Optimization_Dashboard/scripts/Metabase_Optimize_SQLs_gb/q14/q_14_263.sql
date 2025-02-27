DECLARE r14_q263 ARRAY<STRUCT<users INT64,sessions INT64,avg_sessions_per_user FLOAT64,sum_total_time_spent INT64,time_spent_per_user INT64,`Time spent per user` STRING,scan_users INT64,sum_scans INT64,avg_scans FLOAT64,scan_sessions INT64,scan_avg_sessions_per_user FLOAT64,scan_total_time_spent INT64,scan_avg_time_spent_per_user FLOAT64,`Time spent per scan user` STRING,scan_time_spent_per_session FLOAT64,`Time spent per session of scan user` STRING,`time_spent_per_session` FLOAT64,`Time spent per session` STRING,`Time spent per no scan user` STRING,`Time spent per session of no scan user` STRING>>;
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
      AND query_id = 263
  );
  
  IF row_count = 0 THEN
    SET r14_q263 = (
      SELECT ARRAY(
        WITH t_users AS (
    SELECT 
        user_id,
        SUM(sessions_count) AS sessions_count,
        SUM(total_time_spent) AS total_time_spent,
        SUM(toy_unlocked_by_scan_count)+SUM(scan_mode_finished_count) AS total_scans
    FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
    JOIN 
        `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name
    WHERE 1=1
            and date(server_date) >= istart_date
            and date(server_date) < iend_date
        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])   
        AND version IN UNNEST(iversions)    	
        [[AND {{ishopfilter}}]]
    GROUP BY 
        user_id
)
--main query
SELECT AS STRUCT 
    --generic users
        COUNT(DISTINCT user_id) AS users,
        SUM(sessions_count) AS sessions,
        SUM(sessions_count)/COUNT(DISTINCT user_id) AS avg_sessions_per_user,
        SUM(total_time_spent) AS sum_total_time_spent,
        cast(SUM(total_time_spent) / COUNT(DISTINCT user_id) as INT64) AS time_spent_per_user,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent per user`,
    --scan users    
        COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS scan_users,
        SUM(total_scans) AS sum_scans,
        SUM(total_scans)/COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS avg_scans,
        SUM(case when total_scans > 0 then sessions_count else 0 end ) AS scan_sessions,
        SUM(case when total_scans > 0 then sessions_count else 0 end )/COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS scan_avg_sessions_per_user,
        SUM(case when total_scans > 0 then total_time_spent else 0 end ) AS scan_total_time_spent,
        SUM(case when total_scans > 0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS scan_avg_time_spent_per_user,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans > 0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS INT64))) AS `Time spent per scan user`,
        SUM(case when total_scans > 0 then total_time_spent else 0 end ) / SUM(case when total_scans > 0 then sessions_count else 0 end ) AS scan_time_spent_per_session,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans > 0 then total_time_spent else 0 end ) / SUM(case when total_scans > 0 then sessions_count else 0 end ) AS INT64))) AS `Time spent per session of scan user`,
        cast(SUM(total_time_spent) / SUM(sessions_count) as INT64) AS time_spent_per_session,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(cast(SUM(total_time_spent) / SUM(sessions_count) as INT64))) AS `Time spent per session`,
	--no scan users
		FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans = 0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans = 0 then user_id else 0 end) AS INT64))) AS `Time spent per no scan user`,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans = 0 then total_time_spent else 0 end ) / SUM(case when total_scans = 0 then sessions_count else 0 end ) AS INT64))) AS `Time spent per session of no scan user`
    FROM 
        t_users
      )
    );
    
  ELSE
    SET r14_q263 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1 as INT64) as users, CAST(value2 as INT64) as sessions, CAST(value3 as FLOAT64) as avg_sessions_per_user, CAST(value4 as INT64) as sum_total_time_spent, CAST(value5 as INT64) as time_spent_per_user, CAST(value6_str as STRING) as `Time spent per user`, CAST(value7 as INT64) as scan_users, CAST(value8 as INT64) as sum_scans, CAST(value9 as FLOAT64) as avg_scans, CAST(value10 as INT64) as scan_sessions, CAST(value11 as FLOAT64) as scan_avg_sessions_per_user, CAST(value12 as INT64) as scan_total_time_spent, CAST(value13 as FLOAT64) as scan_avg_time_spent_per_user, CAST(value14_str as STRING) as `Time spent per scan user`, CAST(value15 as FLOAT64) as scan_time_spent_per_session, CAST(value16_str as STRING) as `Time spent per session of scan user`, CAST(value17 as FLOAT64) as `time_spent_per_session`, CAST(value18_str as STRING) as `Time spent per session`, CAST(value19_str as STRING) as `Time spent per no scan user`, CAST(value20_str as STRING) as `Time spent per session of no scan user`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 263 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q263);
  