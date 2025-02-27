DECLARE r319_q4268 ARRAY<STRUCT<`year` INT64,`month` INT64,`Time` STRING,`Users` INT64,`Scanned Users` INT64,`Scan users ratio` FLOAT64,`Average Time per Sessions` STRING,`Average Time per scanned user` STRING,`Average Time per NOT scanned user` STRING>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4268
);

IF row_count = 0 THEN
  SET r319_q4268 = (
    SELECT ARRAY(
      with gb4268  as (SELECT 0)
,tbl_mau as
(
    SELECT EXTRACT(YEAR FROM client_time) as year
        ,EXTRACT(MONTH FROM client_time) as month
        ,concat(EXTRACT(YEAR FROM client_time),' ',FORMAT_TIMESTAMP('%B', client_time)) as year_month
        ,count(distinct user_id) as users
        ,sum(cast(time_spent as int)) as total_time_spent
        ,sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as total_sessions
        ,sum(cast(time_spent as int)) / sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as time_result
        ,concat(EXTRACT(MINUTE FROM TIMESTAMP_SECONDS(cast(sum(cast(time_spent as int)) / sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as int))), ' min ', EXTRACT(SECOND FROM TIMESTAMP_SECONDS(cast(sum(cast(time_spent as int)) / sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as int))), ' sec') as `Average Time per Sessions`
    from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
    join (
        SELECT distinct user_id 
        from `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        where 1=1 
        and install_source in (SELECT ua_filter from `applaydu.tbl_ua_filter` where 1=1  [[and {{iinstallsource}}]] )
        and date(active_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(active_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
    ) using (user_id)
    where 1=1
        and not(t.game_id = 82471 and client_time <'2020-12-14')
        and date(client_time) >= date_sub(date_trunc(current_date(), month), interval 2 year)
        and date(client_time) < date_trunc(current_date(), month)
        and cast(time_spent as FLOAT64) >= 0
        and cast(time_spent as FLOAT64) < 86400
        and date(client_time) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(client_time) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{icountry}}]] [[and {{iregion}}]])    
        and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1  [[and {{iversion}}]] )
    group by year, month, year_month
)
,t_users as
(
    SELECT user_id
        ,EXTRACT(YEAR FROM server_date) as year
        ,EXTRACT(MONTH FROM server_date) as month
        ,concat(EXTRACT(YEAR FROM server_date),' ',FORMAT_TIMESTAMP('%B', server_date)) as year_month
        ,sum(total_time_spent) as total_time_spent
        ,sum(toy_unlocked_by_scan_count) as toy_unlocked_by_scan_count
        ,sum(scan_mode_finished_count) as scan_mode_finished_count
    from `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
    join (
        SELECT distinct user_id 
        from `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        where 1=1 
        and install_source in (SELECT ua_filter from `applaydu.tbl_ua_filter` where 1=1  [[and {{iinstallsource}}]] )
        and date(active_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(active_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
    ) using (user_id)
    where date(server_date) >= date_sub(date_trunc(current_date(), month), interval 2 year)
        and date(server_date) < date_trunc(current_date(), month)
        and date(server_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(server_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
        and t.country_name in (SELECT country_name from `applaydu.tbl_country_filter` where 1=1  [[and {{icountry}}]] [[and {{iregion}}]])   
        and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1  [[and {{iversion}}]] )   
    group by user_id, year, month, year_month
)
,t_scan_users as (
    SELECT year
        ,month
        ,year_month
        ,count(distinct user_id) as users
        ,sum(total_time_spent) as sum_total_time_spent
        ,sum(total_time_spent) / count(distinct user_id) as time_result
        ,concat(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' hour ', EXTRACT(MINUTE FROM TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' min ', EXTRACT(SECOND FROM TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' sec') as time_spent
    from t_users
    where toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 
    group by year, month, year_month
)
,t_not_scan_users as (
   SELECT year
        ,month
        ,year_month
        ,count(distinct user_id) as users
        ,sum(total_time_spent) as sum_total_time_spent
        ,sum(total_time_spent) / count(distinct user_id) as time_result
        ,concat(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' hour ', EXTRACT(MINUTE FROM TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' min ', EXTRACT(SECOND FROM TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' sec') as time_spent
    from t_users
    where toy_unlocked_by_scan_count = 0 and  scan_mode_finished_count = 0 
    group by year, month, year_month
)
--main query
SELECT AS STRUCT year, month, year_month as `Time`
    ,tbl_mau.users as `Users`
    ,t_scan_users.users as `Scanned Users`
    ,t_scan_users.users / tbl_mau.users as `Scan users ratio`
    ,`Average Time per Sessions`
    ,t_scan_users.time_spent as `Average Time per scanned user`
    ,t_not_scan_users.time_spent as `Average Time per NOT scanned user`
from tbl_mau
    join t_scan_users using (year, month, year_month)
    join t_not_scan_users using (year, month, year_month)
order by year asc, month asc
    )
  );
  
ELSE
  SET r319_q4268 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as `year`, CAST(value2 as INT64) as `month`, CAST(value3_str as STRING) as `Time`, CAST(value4 as INT64) as `Users`, CAST(value5 as INT64) as `Scanned Users`, CAST(value6 as FLOAT64) as `Scan users ratio`, CAST(value7_str as STRING) as `Average Time per Sessions`, CAST(value8_str as STRING) as `Average Time per scanned user`, CAST(value9_str as STRING) as `Average Time per NOT scanned user`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4268 
  );
END IF;

SELECT * FROM UNNEST(r319_q4268);
