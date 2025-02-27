insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
WITH gb4245 as (select 0)
,kdr_scan_users AS (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_kinderini_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and scan_result IN ('New_Toy', 'Old_Toy')
    and (
      (scan_type='Deep Link' and UPPER(reference) LIKE '%KINDERINI%')
      OR scan_type IN ('Scan_Toy_Biscuit', 'Scan_QR_Biscuit')
    )
),
filter_sst AS (
  select * 
  from `gcp-bi-elephant-db-gold.applaydu.story_step_finished`
  join kdr_scan_users USING (user_id)
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_kinderini_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and environment_id='Kinderini'
),
tap_emotion_user AS (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.story_mode_triggered`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_kinderini_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and environment_id='Kinderini'
    and click_from IN (
      'Eduland Kinderini Menu - Cluster - Wonder',
      'Eduland Kinderini Menu - Cluster - Happiness',
      'Eduland Kinderini Menu - Cluster - Kindness',
      'Eduland Kinderini Menu - Cluster - Fearfulness'
    )
    and user_id IN (select DISTINCT user_id from kdr_scan_users)
),
result AS (
  select 
    'Kinderini Scan Users' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from kdr_scan_users
  UNION ALL 
  select 
    'Tap Emotion Cluster' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from tap_emotion_user
  UNION ALL
  select 
    'Drawing Start' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Drawing MIG' and user_selection='Started'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Drawing Stop' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Drawing MIG' and user_selection='Finished'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Finding Start' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Finding MIG' and user_selection='Started'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Finding Stop' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Finding MIG' and user_selection='Finished'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Catching Start' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Catching MIG' and user_selection='Started'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Catching Stop' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Catching MIG' and user_selection='Finished'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Diary Start' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Dairy Screen' and user_selection='Started'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
  UNION ALL
  select 
    'Diary Stop' AS `#`,
    COUNT(DISTINCT user_id) AS `Users`
  from filter_sst
  where story_step='Kinderini - Dairy Screen' and user_selection='Finished'
    and user_id IN (select DISTINCT user_id from tap_emotion_user)
)
--main query

select 319 as dashboard_id
		,4269 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Funnel Story mode for scan only' as kpi_name
		,`#` as value1_str,`Users` as value2
	from
	(
	
select * from result
)