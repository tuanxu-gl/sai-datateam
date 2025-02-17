insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
-- v4.0.0
WITH gb4258 as (select 0)
,tbl_ua AS (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
  and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
) 
,USER_LAUNCH AS (
  select DISTINCT user_id
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume` 
  join tbl_ua USING (user_id)
  where launch_type='first_launch'
  and version>='5.0.0' and version<'9.0.0'
  and date(client_time)>=(select cast(ivalue as DATE) from `applaydu.tbl_variables` where ikey='apd_v4_start_date') 
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and game_id IN (81335, 81337, 85837) 
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
)
,UNIQUE_DA_USER AS (
  select DISTINCT ul.user_id
  from `gcp-bi-elephant-db-gold.applaydu.disclaimer_acceptance` AS fe
  RIGHT join user_launch ul ON fe.user_id=ul.user_id
  where version>='5.0.0' and version<'9.0.0'
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and game_id IN (81335, 81337, 85837) 
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
)
,choose_scan_free AS (
  select DISTINCT udu.user_id
  from `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe
  RIGHT join unique_da_user AS udu ON fe.user_id=udu.user_id
  where ftue_stage='Start'
  and ftue_steps='Choose Scan Or Unlock'
  and version>='5.0.0' and version<'9.0.0'
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
)
,deeplink_user AS (
  select user_id
  from `gcp-bi-elephant-db-gold.applaydu.dlc_download`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and user_status='FTUE'
  and unlock_cause='Deep_Link'
  and version>='5.0.0' and version<'9.0.0'
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  EXCEPT DISTINCT
  select DISTINCT user_id
  from choose_scan_free
)
,TIME_CONTROL AS (
  select user_id 
  from `gcp-bi-elephant-db-gold.applaydu.time_control_access`
  INNER join unique_da_user USING (user_id)
  where user_status='FTUE'
  and version>='5.0.0' and version<'9.0.0'
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
)     
,ALL_DATA AS (
  --FTUE flow start
  (  
  select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ftue_steps
    ,'Disclaimer_Acceptance','Parental Agreement ' || ftue_stage)
    ,'Choose Avatar Gender', 'Gender + Difficulty page ' || ftue_stage)
    ,'Avatar_Creation', 'Avatar Creation ' || ftue_stage)
    ,'Enter Avatar Name', 'Avatar Name ' || ftue_stage)
    ,'Avatar_Creation', 'Character Creation ' || ftue_stage)
    ,'Email Registration', 'Email Registration ' || ftue_stage)
    ,'Age Confirmation after Email', 'Age Confirmation after Email ' || ftue_stage)
    ,'Choose Scan Or Unlock', 'Choose No Toy Or Scan ' || ftue_stage)
     AS `Users`
    , COUNT(DISTINCT udu.user_id) AS `Users each step`
  from `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe
  RIGHT join unique_da_user AS udu ON fe.user_id=udu.user_id
  where ftue_stage='Start'
  and ftue_steps IN ('Choose Avatar Gender', 'Avatar_Creation','Enter Avatar Name','Email Registration')
  and version>='5.0.0' and version<'9.0.0'
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  GROUP BY ftue_steps, ftue_stage
  ORDER BY `Users each step` DESC
  )
  UNION ALL
  --Deep link users directly jump to Eduland 
  (  
  select 'Deeplink' AS `Users`
    , COUNT(DISTINCT de.user_id) AS `Users each step`
  from deeplink_user de
  RIGHT join unique_da_user AS udu ON de.user_id=udu.user_id
  GROUP BY `Users`
  ORDER BY `Users each step` DESC
  )
  UNION ALL
  (
  select 'New Users Launch' AS `Users`, COUNT(DISTINCT user_id) AS `Users each step`
  from user_launch
  )
)
--main query

select 319 as dashboard_id
		,4258 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Funnel v4 - Deeplink users' as kpi_name
		,`Users` as value1_str,`Users each step` as value2
	from
	(
	
select *
from
(
select *
from all_data
where `Users`='New Users Launch' OR (`Users` LIKE '%Start' and `Users` NOT IN ('Camera Permission Start'))
OR `Users` IN ('Deeplink')
UNION all
select 'Time Control' AS `Users`, COUNT(DISTINCT user_id) AS `Users each step`
from time_control
UNION all
select 'Parental Agreement Start' AS `Users`, COUNT(DISTINCT user_id) AS `Users each step`
from unique_da_user
ORDER BY 
(
CASE 
  WHEN `Users`='New Users Launch' THEN 0
  WHEN `Users`='Parental Agreement Start' THEN 1
  WHEN `Users`='Gender + Difficulty page Start' THEN 2
  WHEN `Users`='Gender + Difficulty page Finish' THEN 3
  WHEN `Users`='Time Control' THEN 4
  WHEN `Users`='Avatar Creation Start' THEN 5
  WHEN `Users`='Avatar Creation Finish' THEN 6
  WHEN `Users`='Avatar Name Start' THEN 7
  WHEN `Users`='Avatar Name Finish' THEN 8
  WHEN `Users`='Email Registration Start' THEN 9
  WHEN `Users`='Email Registration Finish' THEN 10
  WHEN `Users`='Age Confirmation after Email Start' THEN 11
  WHEN `Users`='Age Confirmation after Email Finish' THEN 12  
  WHEN `Users`='Choose No Toy Or Scan Start' THEN 13
  WHEN `Users`='Choose No Toy Or Scan Finish' THEN 14
  WHEN `Users`='Choose to Scan' THEN 15
  WHEN `Users`='Choose to Unlock Free Toy' THEN 16
  WHEN `Users`='Camera Permission Start' THEN 17
  WHEN `Users`='Camera Permission Finish' THEN 18
  WHEN `Users`='Camera Access explanation screen Start' THEN 19
  WHEN `Users`='Camera Access explanation screen Finish' THEN 20
  WHEN `Users`='Scan Section Start' THEN 21
  WHEN `Users`='Scan Section Finish' THEN 22
  WHEN `Users`='Scan toy result Start' THEN 23
  WHEN `Users`='Scan toy result Finish' THEN 24
  WHEN `Users`='Unlock toy screen Start' THEN 25
  WHEN `Users`='Unlock toy screen Finish' THEN 26
  WHEN `Users`='Simple Toy AR Start' THEN 27
  WHEN `Users`='Simple Toy AR Finish' THEN 28 
  WHEN `Users`='World Map final Start' THEN 29
  WHEN `Users`='World Map final Finish' THEN 30 
  WHEN `Users`='Users Scanned successfully toy 1st time' THEN 31
  WHEN `Users`='Users Scanned successfully toy 2nd time' THEN 32 
  WHEN `Users`='Users Scanned successfully toy 3rd time' THEN 33 
END
)
)
)