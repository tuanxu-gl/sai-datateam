insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6)
WITH gb4263 as (select 0)
,tbl_story_mode_finished as (
 select user_id,FED_ID,PLATFORM,game_id,EVENT_ID,min(client_time),min(server_time),version,country,SESSION_ID
,min(TOKEN),AVATAR_GENDER,END_CAUSE,TOY_NAME,STORY_STEP,avg(TIME_TO_FINISH)
,ACTIVITY_01,ACTIVITY_01_VALUE,ACTIVITY_02,ACTIVITY_02_VALUE,ACTIVITY_03,ACTIVITY_03_VALUE,ACTIVITY_04
,ACTIVITY_04_VALUE,ACTIVITY_05,ACTIVITY_05_VALUE,AVATAR_ONESIE,click_from,ENVIRONMENT_ID,min(EVENT_client_time_LOCAL)
,avg(REALTIME_SPENT),min(load_time),ACTIVITY_06,ACTIVITY_06_VALUE,ACTIVITY_07,ACTIVITY_07_VALUE
,ACTIVITY_08,ACTIVITY_08_VALUE,ACTIVITY_09,ACTIVITY_09_VALUE,ACTIVITY_10,ACTIVITY_10_VALUE,TOY_UNLOCKED_METHOD,from_scene
 from gcp-bi-elephant-db-gold.applaydu.story_mode_finished 
 where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and (version>='5.0.0' and date(client_time)>='2024-08-28') and version<'5.2.0' 
 and (environment_id='Experience - Dino Museum' and version>='4.7.0')
 group by all
)
,real_story_mode_finished as (
select user_id,game_id,event_id,version,country,session_id,avatar_gender,end_cause,toy_name,story_step,realtime_spent,environment_id,client_time,toy_unlocked_method,count(*) as dup
from 
 (
 -- exclude Dino Exp
 select * from gcp-bi-elephant-db-gold.applaydu.story_mode_finished
 where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and
 environment_id like 'Natoons v4%' or
 -- DUPLICATIONS in those Experience
 (environment_id like '%Travel%' and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
 (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
 (environment_id NOT IN ('Savannah','Space','Ocean','Jungle','Magic Land','Experience - Dino Museum') and (environment_id not LIKE '%Travel%') ) or 
 (environment_id='Kinderini' and date(client_time)>=(select date(ivalue) from gcp-gfb-sai-tracking-gold.applaydu.tbl_variables where ikey='apd_kinderini_start_date') ) or 
 (environment_id='Eduland Lets Story' and date(client_time)>=(select date(ivalue) from gcp-gfb-sai-tracking-gold.applaydu.tbl_variables where ikey='apd_v5_lets_story_start_date'))
 -- Dino 
 union all
 -- version<'5.0.0' or (version>='5.2.0' and date(client_time)>='2024-10-19') (normal)
 select * from gcp-bi-elephant-db-gold.applaydu.story_mode_finished 
 where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and (version<'5.0.0' or (version>='5.2.0' and date(client_time)>='2024-10-19'))
 and (environment_id='Experience - Dino Museum' and version>='4.7.0')
 union all
 -- (version>='5.0.0' and date(client_time)>='2024-08-28') and version<'5.2.0'
 select * from tbl_story_mode_finished
 )
group by all
)
,result as (
select case when environment_id like 'Natoons v4%' then 'Natoons Experience'
 when environment_id like '%Travel%' then 'Travel Experience'
 when environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') then 'Fantasy Experience'
 when environment_id like '%Space%' and environment_id<>'Space' then 'Space Experience'
 when environment_id='Experience - Dino Museum' then 'Dino Experience - since v4.7.0'
 when environment_id='Kinderini' then 'Kinderini'
 when environment_id='Eduland Lets Story' then 'Lets Story'
 else null end as environment
,count (0) as `No of Session`,count (distinct user_id) as `No of Users`
,count (0)/count (distinct user_id) as `Sessions per user`,sum(realtime_spent)/count (distinct user_id)/60 as `Time spent per user in min`
,sum(realtime_spent)/count (0)/60 as `Time spent per session in min`
--,concat(floor(`Time spent per user in min`),' min ',round((`Time spent per user in min` - floor(`Time spent per user in min`)) * 60),' sec') as `Time spent per user (min - sec)`
--,concat(floor(`Time spent per session in min`),' min ',round((`Time spent per session in min` - floor(`Time spent per session in min`)) * 60),' sec') as `Time spent per session (min - sec)`
from real_story_mode_finished
 join (select distinct user_id from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) ) using (user_id)
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
 and (version>='4.0.0' and date(client_time)>='2023-08-22') and version<'9.0.0' and date(client_time)<current_date()
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 ) and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
 and version in (select version from `applaydu.tbl_version_filter` where 2=2 )
 and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 2=2  )
 and realtime_spent>=0
group by 1
)
--main query

select 319 as dashboard_id
		,4264 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Time spent per user and Time spent per session by Dedicated Experience' as kpi_name
		,environment as value1_str,`No of Session` as value2,`No of Users` as value3,`Sessions per user` as value4,`Time spent per user in min` as value5,`Time spent per session in min` as value6
	from
	(
	
select * 
--,concat(floor(`Time spent per user in min`),' min ',round((`Time spent per user in min` - floor(`Time spent per user in min`)) * 60),' sec') as `Time spent per user in min sec`
--,concat(floor(`Time spent per session in min`),' min ',round((`Time spent per session in min` - floor(`Time spent per session in min`)) * 60),' sec') as `Time spent per session in min sec`
from result 
where Environment is not null
order by `Time spent per user in min` desc
)