insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6)
WITH gb4276 as (select 0),
scan_profile AS (
  select DISTINCT user_id 
  from `gcp-gfb-sai-tracking-gold.applaydu.tbl_users`
  where (toy_unlocked_by_scan_count>0 OR scan_mode_finished_count>0)
),
t1 AS (
  select
    user_id, 
    DATE_TRUNC(date(client_time), DAY) AS login_day,
    min(DATE_TRUNC(date(client_time), DAY)) OVER (PARTITION BY user_id) AS first_day,
    min(version) OVER (PARTITION BY user_id) AS first_version,
    min(session_id) OVER (PARTITION BY user_id) AS first_session,
    FORMAT_date('%A', min(DATE_TRUNC(date(client_time), DAY)) OVER (PARTITION BY user_id)) AS first_weekday,
    DATE_DIFF(date(client_time), min(DATE_TRUNC(date(client_time), DAY)) OVER (PARTITION BY user_id), DAY) AS subsequent_day
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
  join scan_profile USING (user_id)
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
  ) USING (user_id)
  join `applaydu.tbl_shop_filter` ON `applaydu.tbl_shop_filter`.game_id=t.game_id and `applaydu.tbl_shop_filter`.country=t.country 
  where t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
),
t2 AS (
  select 
    DATE_TRUNC(first_day, MONTH) AS first_month,
    subsequent_day,
    COUNT(DISTINCT user_id) AS users
  from t1
  where 1=1 and date(first_day) >= 'istart_date' and date(first_day) < date_add('iend_date', INTERVAL 1 DAY)
    and first_day>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and first_day<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and first_version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and first_day>='2023-12-01' 
    and first_day<date_sub(current_date(), INTERVAL 3 DAY)
    and first_weekday IN ('Friday', 'Saturday')
  GROUP BY first_month, subsequent_day
)
--main query

select 319 as dashboard_id
		,4276 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'[Scan Users] Monthly [D1, D7, D28] Retention' as kpi_name
		,month as value1_str,`D0` as value2,`D1` as value3,`D3` as value4,`D7` as value5,`D28` as value6
	from
	(
	
select 
  cast(first_month as string) AS month,
  SUM(CASE WHEN subsequent_day=0 THEN users ELSE 0 END) AS D0,
  SUM(CASE WHEN subsequent_day=1 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day=0 THEN users ELSE 0 END) AS D1,
  SUM(CASE WHEN subsequent_day=3 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day=0 THEN users ELSE 0 END) AS D3,
  SUM(CASE WHEN subsequent_day=7 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day=0 THEN users ELSE 0 END) AS D7,
  SUM(CASE WHEN subsequent_day=28 THEN users ELSE 0 END) / SUM(CASE WHEN subsequent_day=0 THEN users ELSE 0 END) AS D28
from t2
GROUP BY month
ORDER BY month
)