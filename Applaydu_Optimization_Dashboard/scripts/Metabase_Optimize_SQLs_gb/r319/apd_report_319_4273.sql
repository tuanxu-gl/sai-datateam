insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
WITH gb4273 as (select 0)
,tbl_ls_scan_users AS (
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
tbl_ls_user_activity AS (
  select 
    t.*,
    d1.name AS language,
    d2.name AS location,
    d3.name AS hero,
    d4.name AS sidekick,
    d5.name AS plot,
    d6.name AS theme
  from `gcp-bi-elephant-db-gold.applaydu.activity_finished` t
  LEFT join `gcp-bi-elephant-db-gold.dimensions.element` d1 ON d1.id=activity_01_value 
  LEFT join `gcp-bi-elephant-db-gold.dimensions.element` d2 ON d2.id=activity_02_value 
  LEFT join `gcp-bi-elephant-db-gold.dimensions.element` d3 ON d3.id=activity_03_value 
  LEFT join `gcp-bi-elephant-db-gold.dimensions.element` d4 ON d4.id=activity_04_value 
  LEFT join `gcp-bi-elephant-db-gold.dimensions.element` d5 ON d5.id=activity_05_value 
  LEFT join `gcp-bi-elephant-db-gold.dimensions.element` d6 ON d6.id=activity_06_value 
  where activity_01='Experience - Lets Story - New Story Created'
    and version>='5.0.0'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and user_id IN (select DISTINCT user_id from tbl_ls_scan_users)
)
--main query

select 319 as dashboard_id
		,4273 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Number of stories created by language of Scan User' as kpi_name
		,`Language` as value1_str,`Stories` as value2
	from
	(
	
select n2.name AS `Language`, COUNT(0) AS `Stories`
from tbl_ls_user_activity
LEFT join UNNEST([
  STRUCT('en' AS id, 'English' AS name),
  STRUCT('es' AS id, 'Spanish' AS name),
  STRUCT('de' AS id, 'German' AS name),
  STRUCT('it' AS id, 'Italian' AS name),
  STRUCT('pt' AS id, 'Portuguese' AS name),
  STRUCT('fr' AS id, 'French' AS name),
  STRUCT('pl' AS id, 'Polish' AS name),
  STRUCT('ko' AS id, 'Korean' AS name),
  STRUCT('hu' AS id, 'Hungarian' AS name),
  STRUCT('nl' AS id, 'Dutch' AS name),
  STRUCT('zh-Hant' AS id, 'Traditional Chinese' AS name),
  STRUCT('ar' AS id, 'Arabic' AS name),
  STRUCT('zh-Hans' AS id, 'Simplified Chinese' AS name)
]) AS n2 ON n2.id=tbl_ls_user_activity.language
GROUP BY `Language`
ORDER BY `Stories` DESC
)