insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6,value7_str,value8_str)
with q3142 as (select 0),
tbl_STORY_MODE_FINISHED as (
  select USER_ID, FED_ID, PLATFORM, GAME_ID, EVENT_ID, min(client_time), min(SERVER_TIME), VERSION, country, SESSION_ID
    , min(TOKEN), AVATAR_GENDER, END_CAUSE, TOY_NAME, STORY_STEP, avg(TIME_TO_FINISH)
    , ACTIVITY_01, ACTIVITY_01_VALUE, ACTIVITY_02, ACTIVITY_02_VALUE, ACTIVITY_03, ACTIVITY_03_VALUE, ACTIVITY_04
    , ACTIVITY_04_VALUE, ACTIVITY_05, ACTIVITY_05_VALUE, AVATAR_ONESIE, CLICK_FROM, ENVIRONMENT_ID, min(EVENT_client_time_LOCAL)
    , avg(REALTIME_SPENT), min(LOAD_TIME), ACTIVITY_06, ACTIVITY_06_VALUE, ACTIVITY_07, ACTIVITY_07_VALUE
    , ACTIVITY_08, ACTIVITY_08_VALUE, ACTIVITY_09, ACTIVITY_09_VALUE, ACTIVITY_10, ACTIVITY_10_VALUE, TOY_UNLOCKED_METHOD, FROM_SCENE
  from APPLAYDU.STORY_MODE_FINISHED 
  where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') and (version >='5.0.0' and client_time >= '2024-08-28') and version < '5.2.0' 
    and (environment_id = 'Experience - Dino Museum' and (version >= '4.7.0' and client_time >= '2024-06-11'))
  group by all
)
,REAL_STORY_MODE_FINISHED as (
select user_id,game_id,event_id,version,country,session_id, avatar_gender, end_cause, toy_name, story_step, realtime_spent, environment_id, server_time,client_time, toy_unlocked_method, count(*) as dup
FROM 
  (
  -- exclude Dino Exp
  select * from APPLAYDU.STORY_MODE_FINISHED
  where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') and (
    environment_id like 'Natoons v4%' or
  -- DUPLICATIONS in those Experience
    (environment_id like '%Travel%' and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
    (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
    (environment_id NOT IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') and (environment_id not LIKE '%Travel%') ) or 
    (environment_id = 'Kinderini' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date') ) or 
    (environment_id = 'Eduland Lets Story' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date'))
    )
  -- Dino   
  union all
  -- version < '5.0.0' or version >= '5.2.0' (normal)
  select * from APPLAYDU.STORY_MODE_FINISHED 
  where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') and (version < '5.0.0' or (version >='5.2.0' and client_time >= '2024-10-19'))
   and (environment_id = 'Experience - Dino Museum' and (version >= '4.7.0' and client_time >= '2024-06-11'))
  union all
  -- version >='5.0.0' and version < '5.2.0'
  select * from tbl_STORY_MODE_FINISHED
  )
group by all
)
, result as (
select case when environment_id like 'Natoons v4%' then 'Natoons Experience'
      when environment_id like '%Travel%' then 'Travel Experience'
      when environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') then 'Fantasy Experience'
      when environment_id like '%Space%' and environment_id <> 'Space' then 'Space Experience'
      when environment_id = 'Experience - Dino Museum' then 'Dino Experience - since v4.7.0'
      when environment_id = 'Kinderini' then 'Kinderini'
      when environment_id = 'Eduland Lets Story' then 'Lets Story'
      else null end as environment
  , count (*) as "No of Session"
  , count (distinct user_id) as "No of Users"
  , "No of Session"/"No of Users" as "Sessions per user"
  , sum(realtime_spent)/"No of Users"/60 as "Time spent per user (min)"
  , sum(realtime_spent)/"No of Session"/60 as "Time spent per session (min)"
  , concat(floor("Time spent per user (min)"), ' min ',round(("Time spent per user (min)" - floor("Time spent per user (min)")) * 60) ,' sec') as "Time spent per user (min - sec)"
  , concat(floor("Time spent per session (min)"), ' min ',round(("Time spent per session (min)" - floor("Time spent per session (min)")) * 60) ,' sec') as "Time spent per session (min - sec)"
from REAL_STORY_MODE_FINISHED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
  and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and client_time < CURRENT_DATE()
  and realtime_spent >= 0
group by 1
)
--main query

select 14 as dashboard_id
		,3142 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Time spent per user and Time spent per session by Dedicated Experience' as kpi_name
		,environment as value1_str,"No of Session" as value2,"No of Users" as value3,"Sessions per user" as value4,"Time spent per user (min)" as value5,"Time spent per session (min)" as value6,"Time spent per user (min - sec)" as value7_str,"Time spent per session (min - sec)" as value8_str
	from
	(
	
select * from result where Environment is not null
order by "Time spent per user (min)" desc
)