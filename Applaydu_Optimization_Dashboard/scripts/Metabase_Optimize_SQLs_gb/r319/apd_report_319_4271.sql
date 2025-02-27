insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
WITH gb4271 as (select 0)
,total_apd AS (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
  join `applaydu.tbl_shop_filter` ON `applaydu.tbl_shop_filter`.game_id=t.game_id and `applaydu.tbl_shop_filter`.country=t.country
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and session_id=1
  and NOT (t.game_id=82471 and date(client_time)<'2020-12-14')
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>='2024-01-01'
),
ftue_list AS (
  select DISTINCT user_id
  from `gcp-bi-elephant-db-gold.applaydu.ftue_event`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  and user_id IN (select * from total_apd)
  and ftue_stage IN ('Start')
  and ftue_steps IN ('Email Registration')
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and date(client_time)>='2024-01-01'
  and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
),
regis_regis AS (
  select COUNT(DISTINCT regis.user_id) AS `Successfully Registered Email`
  from `gcp-bi-elephant-db-gold.applaydu.account_operation` AS regis
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
  and date(client_time)>='2024-01-01'
  and account_operation='Email registration' 
  and result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
),
regis_veri AS (
  select COUNT(DISTINCT user_id) AS `Verified email after registration`
  from `gcp-bi-elephant-db-gold.applaydu.account_operation`
  where account_operation='Email registration confirmation'
  and result='Success'
  and 1=1
  and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v4_start_date')
  and date(client_time)<(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_be_parent_registration_start_date')
  and date(client_time)<current_date()
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
  and date(client_time)>='2024-01-01'
),
regis_veri_be AS (
  select COUNT(DISTINCT anon_id) AS `Verified email after registration`
  from `gcp-gfb-sai-tracking-gold.applaydu.store_stats_subscriptions`
  where date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_be_parent_registration_start_date')
  and date(client_time)<current_date()
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2  )
  and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
)
,result as
(
  select 'New Users Launch' AS Users, COUNT(DISTINCT user_id) AS `Number of Users`
  from total_apd
  UNION ALL
  select 'Email Registration Screen' AS Users, COUNT(DISTINCT user_id) AS `Number of Users`
  from ftue_list
  UNION ALL
  select 'Finish Email Registration' AS Users, `Successfully Registered Email` AS `Number of Users`
  from regis_regis
  UNION ALL
  select 'Email Verification' AS Users, (select `Verified email after registration` from regis_veri) + (select `Verified email after registration` from regis_veri_be) AS `Number of Users`
)
--main query

select 319 as dashboard_id
		,4271 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'[PARENTAL] Email registration funnel' as kpi_name
		,`Users` as value1_str,`Number of Users` as value2
	from
	(
	
select * from result
)