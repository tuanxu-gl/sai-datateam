insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,value1,value2,value3,value4,value5,value6)
WITH q3183 as (select 3183),
tbl_STORY_MODE_FINISHED as (
    select USER_ID, FED_ID, PLATFORM, GAME_ID, EVENT_ID, min(CLIENT_TIME), min(SERVER_TIME), VERSION, COUNTRY, SESSION_ID
        , min(TOKEN), AVATAR_GENDER, END_CAUSE, TOY_NAME, STORY_STEP, avg(TIME_TO_FINISH)
        , ACTIVITY_01, ACTIVITY_01_VALUE, ACTIVITY_02, ACTIVITY_02_VALUE, ACTIVITY_03, ACTIVITY_03_VALUE, ACTIVITY_04
        , ACTIVITY_04_VALUE, ACTIVITY_05, ACTIVITY_05_VALUE, AVATAR_ONESIE, CLICK_FROM, ENVIRONMENT_ID, min(EVENT_CLIENT_TIME_LOCAL)
        , avg(REALTIME_SPENT), min(LOAD_TIME), ACTIVITY_06, ACTIVITY_06_VALUE, ACTIVITY_07, ACTIVITY_07_VALUE
        , ACTIVITY_08, ACTIVITY_08_VALUE, ACTIVITY_09, ACTIVITY_09_VALUE, ACTIVITY_10, ACTIVITY_10_VALUE, TOY_UNLOCKED_METHOD, FROM_SCENE
    from APPLAYDU.STORY_MODE_FINISHED  
    where (version >= '5.0.0' and client_time >= '2024-08-28') and version < '5.2.0' 
        and (environment_id = 'Experience - Dino Museum' and (version >= '4.7.0' and client_time >= '2024-06-11'))
		and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
    group by all
)

,REAL_STORY_MODE_FINISHED as (
SELECT user_id,game_id,event_id,version,country,session_id, avatar_gender, end_cause, toy_name, story_step, realtime_spent, environment_id, client_time,SERVER_TIME, toy_unlocked_method, count(*) as dup
FROM 
    (
    -- exclude Dino Exp
    select * from APPLAYDU.STORY_MODE_FINISHED
    WHERE CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
	and (
        environment_id like 'Natoons v4%' or
    -- DUPLICATIONS in those Experience
        (environment_id like '%Travel%' and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
        (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
        (environment_id NOT IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') AND (environment_id not LIKE '%Travel%') ) or 
        (environment_id = 'Kinderini' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date') ) or 
        (environment_id = 'Eduland Lets Story' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date'))
        )
    -- Dino     
    union all
    -- version < '5.0.0' or (version >= '5.2.0' and client_time >= '2024-10-19') (normal)
    select * from APPLAYDU.STORY_MODE_FINISHED  
    WHERE (version < '5.0.0' or (version >= '5.2.0' and client_time >= '2024-10-19'))
     and (environment_id = 'Experience - Dino Museum' and (version >= '4.7.0' and client_time >= '2024-06-11'))
      and   CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
    union all
    -- version >='5.0.0' and version < '5.2.0'
    select * from tbl_STORY_MODE_FINISHED
   )
group by all
)
, dedicated as (
SELECT  user_id, realtime_spent
from REAL_STORY_MODE_FINISHED
where  1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0'
    and realtime_spent >= 0
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
union all
SELECT  user_id, realtime_spent
from APPLAYDU.ILLUSTRATION_BOOK_FINISHED
where 1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' 
    and realtime_spent >= 0
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)

, minigame_done as (
select "APPLAYDU"."MINIGAME_FINISHED"."USER_ID" AS "USER_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."GAME_ID" AS "GAME_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."CLIENT_TIME" AS "CLIENT_TIME", 
"APPLAYDU"."MINIGAME_FINISHED"."SERVER_TIME" AS "SERVER_TIME", 
"APPLAYDU"."MINIGAME_FINISHED"."VERSION" AS "VERSION", 
"APPLAYDU"."MINIGAME_FINISHED"."COUNTRY" AS "COUNTRY", 
"APPLAYDU"."MINIGAME_FINISHED"."SCENE_NAME" AS "SCENE_NAME", 
"APPLAYDU"."MINIGAME_FINISHED"."CLICK_FROM" AS "CLICK_FROM", 
case when REALTIME_SPENT is null then time_to_finish else REALTIME_SPENT end AS realtime_spent, 
"APPLAYDU"."MINIGAME_FINISHED"."LOAD_TIME" AS "LOAD_TIME", 
case when "FROM_SCENE" is null then 'Not yet available' else "FROM_SCENE" end as "FROM_SCENE", 
FROM "ELEPHANT_DB"."APPLAYDU"."MINIGAME_FINISHED"
where CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)
, minigame as (
SELECT  user_id, realtime_spent
FROM minigame_done
where 1=1 
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' 
    and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas')
    and FROM_SCENE <> 'Eduland AvatarHouse' -- EXCLUDE Minigame Drawing in AvatarHouse from Minigame
    and scene_name not like '%Playability%' 
    and( (scene_name <> 'Move Ahead'and realtime_spent >= 0) or ( scene_name = 'Move Ahead' and  realtime_spent::int > 12) )-- to exclude users quitting during loading screen + cover 99% users
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)
, toy_fs as (
--tracking TOY_FRIENDSHIP_FINISHED before v4.6.1
select user_id, time_spent, to_varchar(CLIENT_TIME) as tfs_session
from APPLAYDU.TOY_FRIENDSHIP_FINISHED
where 1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '4.6.1'
    and scene_name like 'Eduland%'
    and time_spent >= 0 and time_spent <7200
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
--change tracking ACTIVITY_FINISHED since v4.6.1
union all
select user_id, time_spent, to_varchar(CLIENT_TIME) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1
    and (version >= '4.6.1' and client_time >= '2024-03-11') and version < '5.2.0' 
    and feature = 'Toy Friendship'
    and activity_01 = 'TFS Current Heart Point' -- first row data only for Natoons and Fantasy event returns 2 rows
    and time_spent >= 0 and time_spent <7200
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
union all 
--change tracking ACTIVITY_FINISHED since v5.2.0
select user_id, sum(time_spent) as total_time, to_varchar(CLIENT_TIME) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1
    and (version >= '5.2.0' and client_time >= '2024-10-19') and version < '5.4.0'
    and feature = 'Toy Friendship'
    and activity_01 = 'TFS Minigame Index Check'
    and time_spent >= 0 and time_spent <7200    
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
group by all
union all 
--change tracking ACTIVITY_FINISHED since v5.4.0
select user_id, sum(time_spent) as total_time,  to_varchar(activity_10_value) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1
    and (version >= '5.4.0' and client_time >= '2024-12-04') and version < '9.0.0' 
    and feature = 'Toy Friendship'
    and activity_01 = 'TFS Minigame Index Check'
    and time_spent >= 0 and time_spent <7200    
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
group by 1,3
)

, ar_mode as (
select user_id, realtime_spent
FROM APPLAYDU.AR_MODE_FINISHED
where 1=1 
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' 
    and realtime_spent >= 0
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
union all
select user_id, realtime_spent
from APPLAYDU.FACE_MASK_FINISHED
where 1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' 
    and realtime_spent >= 0
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)

, avatar_house as (
-- OLD TRACKING: before 4.5.0 use visit_screen
select  user_id
from APPLAYDU.VISIT_SCREEN
where 1=1 
    and screen_to like 'Eduland%Avatar%'
    and (version >= '4.3.0' and client_time >= '2023-11-24') and version < '4.5.0'
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
union all
select  user_id
from APPLAYDU.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent >=0 and from_scene <> 'Inkmagination' -- when users from AH Drawing to AH, we do not count as a new session
    and (version >= '4.3.0' and client_time >= '2024-02-05') and version < '9.0.0' 
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)

, avatarhouse_time  as (-- include Minigame Drawing from AH 
-- OLD TRACKING: before 4.5.0 use visit_screen
select  time_spent
from APPLAYDU.VISIT_SCREEN
where 1=1 and time_spent >= 0 and time_spent < 36000
    and (screen_from like '%Avatar%' or (screen_from like '%Ink%' and screen_to like '%Avatar%')) -- INCLUDE TIME SPENT IN MINIGAME DRAWING IN AH
    and (version >= '4.3.0' and client_time >= '2023-11-24') and version < '4.5.0'  
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
-- NEW TRACKING: after 4.5.0 use AH + MINIGAME DRAWING AH
union all
select  time_spent
from APPLAYDU.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent >=0
    and (version >= '4.3.0' and client_time >= '2024-02-05') and version < '9.0.0'
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
union all -- INCLUDE TIME_SPENT MINIGAME DRAWING in AH
select REALTIME_SPENT
from APPLAYDU.MINIGAME_FINISHED
where scene_name = 'Inkmagination'
    and from_scene = 'Eduland AvatarHouse'
    and (version >= '4.3.0' and client_time >= '2024-02-05') and version < '9.0.0' 
	and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)
, parental_section as (
select user_id, realtime_spent
from APPLAYDU.PARENTAL_SECTION
where 1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' 
    and realtime_spent>= 0
		and CLIENT_TIME >= 'istart_date' and CLIENT_TIME < dateadd(day, 1, 'iend_date')
)
select 14 as dashboard_id
    ,3183 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Sessions and Sessions per User by Feature' as kpi_name
    ,feature as dimension1
    ,session as value1
    ,"Sessions per user" as value2
    , "Time spent per user" as value3
    , "Session Duration" as value4
    , "Time spent per user (min - sec)" as value5
    , "Session Duration (min - sec)" as value6
   
from
(
select 'Dedicated Experience' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as "Sessions per user"
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as "Time spent per user"
                                , sum(realtime_spent)/session/60 as "Session Duration"
                                ,concat(floor("Time spent per user"), ' min ',round(("Time spent per user" - floor("Time spent per user")) * 60) ,' sec') as "Time spent per user (min - sec)"
                                ,concat(floor("Session Duration"), ' min ',round(("Session Duration" - floor("Session Duration")) * 60) ,' sec') as "Session Duration (min - sec)"
from dedicated 

union
select 'AR' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as "Sessions per user"
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as "Time spent per user"
                                , sum(realtime_spent)/session/60 as "Session Duration"
                                ,concat(floor("Time spent per user"), ' min ',round(("Time spent per user" - floor("Time spent per user")) * 60) ,' sec') as "Time spent per user (min - sec)"
                                ,concat(floor("Session Duration"), ' min ',round(("Session Duration" - floor("Session Duration")) * 60) ,' sec') as "Session Duration (min - sec)"
from ar_mode  

union
select 'Minigame' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as "Sessions per user"
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as "Time spent per user"
                                , sum(realtime_spent)/session/60 as "Session Duration"
                                ,concat(floor("Time spent per user"), ' min ',round(("Time spent per user" - floor("Time spent per user")) * 60) ,' sec') as "Time spent per user (min - sec)"
                                ,concat(floor("Session Duration"), ' min ',round(("Session Duration" - floor("Session Duration")) * 60) ,' sec') as "Session Duration (min - sec)"
from minigame 

union
select 'Toy Friendship' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as "Sessions per user"
                                , sum(time_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as "Time spent per user"
                                , sum(time_spent)/session/60 as "Session Duration"
                                ,concat(floor("Time spent per user"), ' min ',round(("Time spent per user" - floor("Time spent per user")) * 60) ,' sec') as "Time spent per user (min - sec)"
                                ,concat(floor("Session Duration"), ' min ',round(("Session Duration" - floor("Session Duration")) * 60) ,' sec') as "Session Duration (min - sec)"
from toy_fs  

union
select 'Avatar House' as feature, count (*) as session, session/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as "Sessions per user"
                                , (select sum(time_spent) from avatarhouse_time) / (case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as "Time spent per user"
                                , (select sum(time_spent) from avatarhouse_time) /session/60 as "Session Duration"
                                ,concat(floor("Time spent per user"), ' min ',round(("Time spent per user" - floor("Time spent per user")) * 60) ,' sec') as "Time spent per user (min - sec)"
                                ,concat(floor("Session Duration"), ' min ',round(("Session Duration" - floor("Session Duration")) * 60) ,' sec') as "Session Duration (min - sec)"
from avatar_house  

union
select 'Parental Section' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as "Sessions per user"
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as "Time spent per user"
                                , sum(realtime_spent)/session/60 as "Session Duration"
                                ,concat(floor("Time spent per user"), ' min ',round(("Time spent per user" - floor("Time spent per user")) * 60) ,' sec') as "Time spent per user (min - sec)"
                                ,concat(floor("Session Duration"), ' min ',round(("Session Duration" - floor("Session Duration")) * 60) ,' sec') as "Session Duration (min - sec)"
from parental_section 

order by session desc

)