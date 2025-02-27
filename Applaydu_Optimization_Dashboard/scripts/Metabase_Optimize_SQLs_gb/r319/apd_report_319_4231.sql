insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
WITH gb4231 as (select 0)
,scan_user AS (
  select DISTINCT user_id
  from (
    select user_id
    from `gcp-bi-elephant-db-gold.applaydu.custom_install_referral`
    join (
      select DISTINCT user_id 
      from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    ) USING (user_id)
    where utm_campaign LIKE '%KCLTS%'
      and version>='5.0.0'
      and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
      and date(client_time)<current_date()
      and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
      and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
      and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
      and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    UNION ALL 
    select user_id
    from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
    join (
      select DISTINCT user_id 
      from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    ) USING (user_id)
    where (
      (reference LIKE '%KCLTS%' and NOT (game_id !=81335 and scan_type='Deep Link')) 
      OR scan_type='Scan_QR_LS'
    )
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and scan_result IN ('New_Toy', 'Old_Toy')
  )
),
tbl_users_launch_lets_story AS (
  select 
    COUNT(DISTINCT user_id) AS users,
    COUNT(0) AS lets_story_sessions
  from `gcp-bi-elephant-db-gold.applaydu.visit_screen`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where (screen_from IN ('World Map', 'Mini Game Screen') OR screen_from LIKE 'Eduland%Minigame Menu') 
    and screen_to='Eduland Lets Story'
    and version>='5.0.0'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and user_id IN (select user_id from scan_user)
),
story_creation_started AS (
  select COUNT(DISTINCT user_id) AS users
  from `gcp-bi-elephant-db-gold.applaydu.visit_screen`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where screen_to='Eduland Lets Story - Story Creation'
    and version>='5.0.0'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and user_id IN (select user_id from scan_user)
),
story_creation_finished AS (
  select COUNT(DISTINCT user_id) AS users
  from `gcp-bi-elephant-db-gold.applaydu.activity_finished`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where activity_01='Experience - Lets Story - New Story Created'
    and version>='5.0.0'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and user_id IN (select user_id from scan_user)
),
tbl_illustration_book_started AS (
  select 
    COUNT(DISTINCT user_id) AS users,
    COUNT(0) AS lets_story_sessions
  from `gcp-bi-elephant-db-gold.applaydu.visit_screen`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where screen_to='Eduland Lets Story - Story Reading'
    and version>='5.0.0'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and user_id IN (select user_id from scan_user)
),
tbl_illustration_book_finished AS (
  select COUNT(DISTINCT user_id) AS users
  from `gcp-bi-elephant-db-gold.applaydu.illustration_book_finished`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where story_title LIKE 'Experience - Lets Story%'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and (end_cause='Finished' OR max_page_reached=total_page_available)
    and user_id IN (select user_id from scan_user)
)
,result as (
select 'Access Story Creation' AS `User Type`, (select users from story_creation_started) AS users
UNION ALL
select 'Access Story Reading' AS `User Type`, (select users from tbl_illustration_book_started) AS users
UNION ALL
select 'Finish reading at least 1 story' AS `User Type`, (select users from tbl_illustration_book_finished) AS users
ORDER BY users DESC
)
--main query

select 319 as dashboard_id
		,4231 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'[Lets Story] Scan Users that created the story' as kpi_name
		,`User Type` as value1_str,users as value2
	from
	(
	
select * from result
)