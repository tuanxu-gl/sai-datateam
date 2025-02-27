insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6_str,value7_str)
with r3094 as( 
select dimension1 as feature,
 value1 as session,
 value2 as `Sessions per user`,
 value3 as `Time spent per user`,
 value4 as `Session Duration`,
value5 as `Time spent per user (min - sec)`,
value6 as `Session Duration (min - sec)`
from APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date=GREATEST((select min(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ), '2020-08-10')
  and end_date=LEAST((select max(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ), DATEADD(day, -4, current_date()))
  and ((select count(0) from tbl_country_filter )=(select count(0) from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ))
  and dashboard_id=14 and query_id=3183 --using the same source
)
,tbl_check_preprocess_report as
(
select CASE 
  WHEN (
    select COUNT(0) 
    from APPLAYDU_NOT_CERTIFIED.apd_report_14
    where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and start_date=GREATEST((select min(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ), '2020-08-10')
    and end_date=LEAST((select max(server_date) from applaydu_not_certified.tbl_date_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ), DATEADD(day, -4, current_date()))
    and ((select count(0) from tbl_country_filter )=(select count(0) from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ))
     and dashboard_id=14 and query_id=3183
  )>0 
  THEN 'Available data from preprocess report'
  ELSE 'N/A'
END as available 
)
--select * from tbl_check_preprocess_report
,tbl_STORY_MODE_FINISHED as (
  select USER_ID, FED_ID, PLATFORM, GAME_ID, EVENT_ID, min(client_time), min(SERVER_TIME), VERSION, country, SESSION_ID
    , min(TOKEN), AVATAR_GENDER, END_CAUSE, TOY_NAME, STORY_STEP, avg(TIME_TO_FINISH)
    , ACTIVITY_01, ACTIVITY_01_VALUE, ACTIVITY_02, ACTIVITY_02_VALUE, ACTIVITY_03, ACTIVITY_03_VALUE, ACTIVITY_04
    , ACTIVITY_04_VALUE, ACTIVITY_05, ACTIVITY_05_VALUE, AVATAR_ONESIE, CLICK_from, ENVIRONMENT_ID, min(EVENT_client_time_LOCAL)
    , avg(REALTIME_SPENT), min(LOAD_TIME), ACTIVITY_06, ACTIVITY_06_VALUE, ACTIVITY_07, ACTIVITY_07_VALUE
    , ACTIVITY_08, ACTIVITY_08_VALUE, ACTIVITY_09, ACTIVITY_09_VALUE, ACTIVITY_10, ACTIVITY_10_VALUE, TOY_UNLOCKED_METHOD, from_SCENE
  from APPLAYDU.STORY_MODE_FINISHED 
  where version>='5.0.0' and version<'5.2.0' 
    and (environment_id='Experience - Dino Museum' and version>='4.7.0')
  group by all
)
,REAL_STORY_MODE_FINISHED as (
select user_id,game_id,event_id,version,country,session_id, avatar_gender, end_cause, toy_name, story_step, realtime_spent, environment_id, client_time,server_time, toy_unlocked_method, count(*) as dup
from 
  (
  -- exclude Dino Exp
  select * from APPLAYDU.STORY_MODE_FINISHED
  where (select available from tbl_check_preprocess_report)='N/A'
		and (
			environment_id like 'Natoons v4%' or
		-- DUPLICATIONS in those Experience
			(environment_id like '%Travel%' and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
			(environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
			(environment_id NOT in ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') and (environment_id not LIKE '%Travel%') ) or 
			(environment_id='Kinderini' and client_time>=(select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_kinderini_start_date') ) or 
			(environment_id='Eduland Lets Story' and client_time>=(select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='apd_v5_lets_story_start_date'))
    )
  -- Dino   
  union all
  -- version<'5.0.0' or version>='5.2.0' (normal)
  select * from APPLAYDU.STORY_MODE_FINISHED 
  where (version<'5.0.0' or version>='5.2.0')
   and (environment_id='Experience - Dino Museum' and version>='4.7.0')
  union all
  -- version>='5.0.0' and version<'5.2.0'
  select * from tbl_STORY_MODE_FINISHED
  )
group by all
)
, dedicated as (
select user_id, realtime_spent
from REAL_STORY_MODE_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and version>='4.0.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and realtime_spent>=0
union all
select user_id, realtime_spent
from APPLAYDU.ILLUSTRATION_BOOK_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and version>='4.0.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and realtime_spent>=0
)
, minigame_done as (
select `APPLAYDU`.`MINIGAME_FINISHED`.`USER_ID` AS `USER_ID`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`GAME_ID` AS `GAME_ID`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`client_time` AS `client_time`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`SERVER_TIME` AS `SERVER_TIME`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`VERSION` AS `VERSION`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`country` AS `country`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`SCENE_NAME` AS `SCENE_NAME`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`CLICK_from` AS `CLICK_from`, 
case when REALTIME_SPENT is null then time_to_finish else REALTIME_SPENT end AS realtime_spent, 
`APPLAYDU`.`MINIGAME_FINISHED`.`LOAD_TIME` AS `LOAD_TIME`, 
case when `from_SCENE` is null then 'Not yet available' else `from_SCENE` end as `from_SCENE`, 
from `ELEPHANT_DB`.`APPLAYDU`.`MINIGAME_FINISHED`
where (select available from tbl_check_preprocess_report)='N/A'
)
, minigame as (
select user_id, realtime_spent
from minigame_done
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  and version>='4.0.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Inkmagination_Xmas')
  and from_SCENE<>'Eduland AvatarHouse' -- EXCLUDE Minigame Drawing in AvatarHouse from Minigame
  and scene_name not like '%Playability%' 
  and( (scene_name<>'Move Ahead'and realtime_spent>=0) or ( scene_name='Move Ahead' and realtime_spent::int>12) )-- to exclude users quitting during loading screen + cover 99% users
)
, toy_fs as (
--tracking TOY_FRIENDSHIP_FINISHED before v4.6.1
select user_id, time_spent, to_varchar(client_time) as tfs_session
from APPLAYDU.TOY_FRIENDSHIP_FINISHED
cross join tbl_check_preprocess_report
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and tbl_check_preprocess_report.available='N/A' 
  and VERSION>='4.0.0' and version<'4.6.1' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and scene_name like 'Eduland%'
  and time_spent>=0 and time_spent<7200
--change tracking ACTIVITY_FINISHED since v4.6.1
union all
select user_id, time_spent, to_varchar(client_time) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and VERSION>='4.6.1' and version<'5.2.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and feature='Toy Friendship'
  and activity_01='TFS Current Heart Point' -- first row data only for Natoons and Fantasy event returns 2 rows
  and time_spent>=0 and time_spent<7200
union all 
--change tracking ACTIVITY_FINISHED since v5.2.0
select user_id, sum(time_spent) as total_time, to_varchar(client_time) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and VERSION>='5.2.0' and version<'5.4.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and feature='Toy Friendship'
  and activity_01='TFS Minigame Index Check'
  and time_spent>=0 and time_spent<7200  
group by all
union all 
--change tracking ACTIVITY_FINISHED since v5.4.0
select user_id, sum(time_spent) as total_time, to_varchar(activity_10_value) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and VERSION>='5.4.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and feature='Toy Friendship'
  and activity_01='TFS Minigame Index Check'
  and time_spent>=0 and time_spent<7200
group by 1,3
)
, ar_mode as (
select user_id, realtime_spent
from APPLAYDU.AR_MODE_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  and version>='4.0.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and realtime_spent>=0
union all
select user_id, realtime_spent
from APPLAYDU.FACE_MASK_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and version>='4.0.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and realtime_spent>=0
)
, avatar_house as (
-- OLD TRACKING: before 4.5.0 use visit_screen
select user_id
from APPLAYDU.VISIT_SCREEN
cross join tbl_check_preprocess_report
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and tbl_check_preprocess_report.available='N/A' 
  and screen_to like 'Eduland%Avatar%'
  and version>='4.3.0' and version<'4.5.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
union all
select user_id
from APPLAYDU.AVATAR_HOUSE_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and time_spent>=0 and from_scene<>'Inkmagination' -- when users from AH Drawing to AH, we do not count as a new session
  and version>='4.5.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
)
, avatarhouse_time as (-- include Minigame Drawing from AH 
-- OLD TRACKING: before 4.5.0 use visit_screen
select time_spent
from APPLAYDU.VISIT_SCREEN
cross join tbl_check_preprocess_report
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and tbl_check_preprocess_report.available='N/A' and time_spent>=0 and time_spent<36000
  and (screen_from like '%Avatar%' or (screen_from like '%Ink%' and screen_to like '%Avatar%')) -- INCLUDE TIME SPENT in MINIGAME DRAWING in AH
  and version>='4.3.0' and version<'4.5.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
-- NEW TRACKING: after 4.5.0 use AH + MINIGAME DRAWING AH
union all
select time_spent
from APPLAYDU.AVATAR_HOUSE_FINISHED
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and time_spent>=0
  and version>='4.5.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
union all -- INCLUDE TIME_SPENT MINIGAME DRAWING in AH
select REALTIME_SPENT
from APPLAYDU.MINIGAME_FINISHED
where scene_name='Inkmagination'
  and from_scene='Eduland AvatarHouse'
  and version>='4.5.0' and VERSION<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
)
, parental_section as (
select user_id, realtime_spent
from APPLAYDU.PARENTAL_SECTION
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and version>='4.0.0' and version<'9.0.0' and client_time<current_date()
  and version>=(select min(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) ) and version<=(select max(version) from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and country in (select country from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)  )
  and realtime_spent>=0
)
select *
from r3094
union
(select * from
(
select 'Dedicated Experience' as feature, count (*) as session, session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
                , sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                , sum(realtime_spent)/session/60 as `Session Duration`
                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from dedicated 
union
select 'AR' as feature, count (*) as session, session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
                , sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                , sum(realtime_spent)/session/60 as `Session Duration`
                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from ar_mode 
union
select 'Minigame' as feature, count (*) as session, session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
                , sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                , sum(realtime_spent)/session/60 as `Session Duration`
                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from minigame 
union
select 'Toy Friendship' as feature, count (*) as session, session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
                , sum(time_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                , sum(time_spent)/session/60 as `Session Duration`
                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from toy_fs 
union
select 'Avatar House' as feature, count (*) as session, session/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
                , (select sum(time_spent) from avatarhouse_time) / (case when count (distinct user_id)=0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                , (select sum(time_spent) from avatarhouse_time) /session/60 as `Session Duration`
                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from avatar_house 
union
select 'Parental Section' as feature, count (*) as session, session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
                , sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                , sum(realtime_spent)/session/60 as `Session Duration`
                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from parental_section 
)
where session>0
)
order by `Time spent per user` desc