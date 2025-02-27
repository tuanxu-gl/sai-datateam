DECLARE r14_q3094 ARRAY<STRUCT<feature STRING,`session` INT64,`Sessions per user` FLOAT64,`Time spent per user` FLOAT64,`Session Duration` FLOAT64,`Time spent per user min - sec` STRING,`Session Duration min - sec` STRING>>;
  DECLARE row_count FLOAT64;
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]);

  
  SET row_count = (
    SELECT COUNT(0) 
    FROM `applaydu.apd_report_14`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 14 
      AND query_id = 3094
  );
  
  IF row_count = 0 THEN
    SET r14_q3094 = (
      SELECT ARRAY(
        with r3094 as( 
SELECT dimension1 as feature,
 value1 as session,
 value2 as `Sessions per user`,
 value3 as `Time spent per user`,
 value4 as `Session Duration`,
value5 as `Time spent per user (min - sec)`,
value6 as `Session Duration (min - sec)`
FROM `gcp-gfb-sai-tracking-gold.applaydu`.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
    and ((SELECT count(0) from tbl_country_filter ) = (SELECT count(0) from tbl_country_filter where 2=2  [[AND {{icountry}}]]))
    and dashboard_id=14 and query_id = 3183 --using the same source
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM `gcp-gfb-sai-tracking-gold.applaydu`.apd_report_14
        WHERE 1=1
        AND start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
        and ((SELECT count(0) from tbl_country_filter ) = (SELECT count(0) from tbl_country_filter where 2=2  [[AND {{icountry}}]]))
         and dashboard_id=14 and query_id = 3183
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
--SELECT * from tbl_check_preprocess_report
,tbl_STORY_MODE_FINISHED as (
    SELECT USER_ID, FED_ID, PLATFORM, GAME_ID, EVENT_ID, min(CLIENT_TIME), min(SERVER_TIME), VERSION, COUNTRY, SESSION_ID
        , min(TOKEN), AVATAR_GENDER, END_CAUSE, TOY_NAME, STORY_STEP, avg(TIME_TO_FINISH)
        , ACTIVITY_01, ACTIVITY_01_VALUE, ACTIVITY_02, ACTIVITY_02_VALUE, ACTIVITY_03, ACTIVITY_03_VALUE, ACTIVITY_04
        , ACTIVITY_04_VALUE, ACTIVITY_05, ACTIVITY_05_VALUE, AVATAR_ONESIE, CLICK_FROM, ENVIRONMENT_ID, min(EVENT_CLIENT_TIME_LOCAL)
        , avg(REALTIME_SPENT), min(LOAD_TIME), ACTIVITY_06, ACTIVITY_06_VALUE, ACTIVITY_07, ACTIVITY_07_VALUE
        , ACTIVITY_08, ACTIVITY_08_VALUE, ACTIVITY_09, ACTIVITY_09_VALUE, ACTIVITY_10, ACTIVITY_10_VALUE, TOY_UNLOCKED_METHOD, FROM_SCENE
    from APPLAYDU.STORY_MODE_FINISHED  
    where version >='5.0.0' and version < '5.2.0' 
        and (environment_id = 'Experience - Dino Museum' and version >= '4.7.0')
		and (SELECT available from tbl_check_preprocess_report) = 'N/A'
    group by all
)
,REAL_STORY_MODE_FINISHED as (
SELECT user_id,game_id,event_id,version,country,session_id, avatar_gender, end_cause, toy_name, story_step, realtime_spent, environment_id, client_time,client_time, toy_unlocked_method, count(*) as dup
FROM 
    (
    -- exclude Dino Exp
    SELECT * from APPLAYDU.STORY_MODE_FINISHED
    WHERE (SELECT available from tbl_check_preprocess_report) = 'N/A'
		and (
			environment_id like 'Natoons v4%' or
		-- DUPLICATIONS in those Experience
			(environment_id like '%Travel%' and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
			(environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
			(environment_id NOT IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') AND (environment_id not LIKE '%Travel%') ) or 
			(environment_id = 'Kinderini' and client_time >= (SELECT ivalue from `gcp-gfb-sai-tracking-gold.applaydu`.TBL_VARIABLES where ikey = 'apd_kinderini_start_date') ) or 
			(environment_id = 'Eduland Lets Story' and client_time >= (SELECT ivalue from `gcp-gfb-sai-tracking-gold.applaydu`.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date'))
        )
    -- Dino     
    union all
    -- version < '5.0.0' or version >= '5.2.0' (normal)
    SELECT * from APPLAYDU.STORY_MODE_FINISHED  
    WHERE (version < '5.0.0' or version >= '5.2.0')
     and (environment_id = 'Experience - Dino Museum' and version >= '4.7.0')
     and (SELECT available from tbl_check_preprocess_report) = 'N/A'
    union all
    -- version >='5.0.0' and version < '5.2.0'
    SELECT * from tbl_STORY_MODE_FINISHED
    )
group by all
)
, dedicated as (
SELECT  user_id, realtime_spent
from REAL_STORY_MODE_FINISHED
where  1=1
    and version >= '4.0.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and realtime_spent >= 0
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
union all
SELECT  user_id, realtime_spent
from APPLAYDU.ILLUSTRATION_BOOK_FINISHED
where 1=1
    and version >= '4.0.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and realtime_spent >= 0
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
)
, minigame_done as (
SELECT `APPLAYDU`.`MINIGAME_FINISHED`.`USER_ID` AS `USER_ID`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`GAME_ID` AS `GAME_ID`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`CLIENT_TIME` AS `CLIENT_TIME`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`SERVER_TIME` AS `SERVER_TIME`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`VERSION` AS `VERSION`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`COUNTRY` AS `COUNTRY`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`SCENE_NAME` AS `SCENE_NAME`, 
`APPLAYDU`.`MINIGAME_FINISHED`.`CLICK_FROM` AS `CLICK_FROM`, 
case when REALTIME_SPENT is null then time_to_finish else REALTIME_SPENT end AS realtime_spent, 
`APPLAYDU`.`MINIGAME_FINISHED`.`LOAD_TIME` AS `LOAD_TIME`, 
case when `FROM_SCENE` is null then 'Not yet available' else `FROM_SCENE` end as `FROM_SCENE`, 
FROM `ELEPHANT_DB`.`APPLAYDU`.`MINIGAME_FINISHED`
where (SELECT available from tbl_check_preprocess_report) = 'N/A'
)
, minigame as (
SELECT  user_id, realtime_spent
FROM minigame_done
where 1=1 
    and version >= '4.0.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Inkmagination_Xmas')
    and FROM_SCENE <> 'Eduland AvatarHouse' -- EXCLUDE Minigame Drawing in AvatarHouse from Minigame
    and scene_name not like '%Playability%' 
    and( (scene_name <> 'Move Ahead'and realtime_spent >= 0) or ( scene_name = 'Move Ahead' and  realtime_spent::int > 12) )-- to exclude users quitting during loading screen + cover 99% users
)
, toy_fs as (
--tracking TOY_FRIENDSHIP_FINISHED before v4.6.1
SELECT user_id, time_spent, to_varchar(CLIENT_TIME) as tfs_session
from APPLAYDU.TOY_FRIENDSHIP_FINISHED
cross join tbl_check_preprocess_report
where 1=1 and tbl_check_preprocess_report.available = 'N/A' 
    and VERSION >= '4.0.0' and version < '4.6.1' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and scene_name like 'Eduland%'
    and time_spent >= 0 and time_spent <7200
--change tracking ACTIVITY_FINISHED since v4.6.1
union all
SELECT user_id, time_spent, to_varchar(CLIENT_TIME) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1
    and VERSION >= '4.6.1' and version < '5.2.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and feature = 'Toy Friendship'
    and activity_01 = 'TFS Current Heart Point' -- first row data only for Natoons and Fantasy event returns 2 rows
    and time_spent >= 0 and time_spent <7200
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
union all 
--change tracking ACTIVITY_FINISHED since v5.2.0
SELECT user_id, sum(time_spent) as total_time, to_varchar(CLIENT_TIME) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1
    and VERSION >= '5.2.0' and version < '5.4.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and feature = 'Toy Friendship'
    and activity_01 = 'TFS Minigame Index Check'
    and time_spent >= 0 and time_spent <7200    
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
group by all
union all 
--change tracking ACTIVITY_FINISHED since v5.4.0
SELECT user_id, sum(time_spent) as total_time,  to_varchar(activity_10_value) as tfs_session
from APPLAYDU.ACTIVITY_FINISHED
where 1=1
    and VERSION >= '5.4.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and feature = 'Toy Friendship'
    and activity_01 = 'TFS Minigame Index Check'
    and time_spent >= 0 and time_spent <7200
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'    
group by 1,3
)
, ar_mode as (
SELECT user_id, realtime_spent
FROM APPLAYDU.AR_MODE_FINISHED
where 1=1 
    and version >= '4.0.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and realtime_spent >= 0
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
union all
SELECT user_id, realtime_spent
from APPLAYDU.FACE_MASK_FINISHED
where 1=1
    and version >= '4.0.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and realtime_spent >= 0
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
)
, avatar_house as (
-- OLD TRACKING: before 4.5.0 use visit_screen
SELECT  user_id
from APPLAYDU.VISIT_SCREEN
cross join tbl_check_preprocess_report
where 1=1 and tbl_check_preprocess_report.available = 'N/A' 
    and screen_to like 'Eduland%Avatar%'
    and version >= '4.3.0' and version < '4.5.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
union all
SELECT  user_id
from APPLAYDU.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent >=0 and from_scene <> 'Inkmagination' -- when users from AH Drawing to AH, we do not count as a new session
    and version >= '4.5.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
)
, avatarhouse_time  as (-- include Minigame Drawing from AH 
-- OLD TRACKING: before 4.5.0 use visit_screen
SELECT  time_spent
from APPLAYDU.VISIT_SCREEN
cross join tbl_check_preprocess_report
where 1=1 and tbl_check_preprocess_report.available = 'N/A' and time_spent >= 0 and time_spent < 36000
    and (screen_from like '%Avatar%' or (screen_from like '%Ink%' and screen_to like '%Avatar%')) -- INCLUDE TIME SPENT IN MINIGAME DRAWING IN AH
    and version >= '4.3.0' and version < '4.5.0'  and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
-- NEW TRACKING: after 4.5.0 use AH + MINIGAME DRAWING AH
union all
SELECT  time_spent
from APPLAYDU.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent >=0
    and version >= '4.5.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
union all -- INCLUDE TIME_SPENT MINIGAME DRAWING in AH
SELECT REALTIME_SPENT
from APPLAYDU.MINIGAME_FINISHED
where scene_name = 'Inkmagination'
    and from_scene = 'Eduland AvatarHouse'
    and version >= '4.5.0' and VERSION < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
)
, parental_section as (
SELECT user_id, realtime_spent
from APPLAYDU.PARENTAL_SECTION
where 1=1
    and version >= '4.0.0' and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (SELECT min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) and version <= (SELECT max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
    and version in (SELECT version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (SELECT country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and realtime_spent>= 0
	and (SELECT available from tbl_check_preprocess_report) = 'N/A'
)
SELECT *
from r3094
union
(SELECT * from
(
SELECT 'Dedicated Experience' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as `Sessions per user`
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                                , sum(realtime_spent)/session/60 as `Session Duration`
                                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from dedicated 
union
SELECT 'AR' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as `Sessions per user`
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                                , sum(realtime_spent)/session/60 as `Session Duration`
                                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from ar_mode  
union
SELECT 'Minigame' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as `Sessions per user`
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                                , sum(realtime_spent)/session/60 as `Session Duration`
                                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from minigame 
union
SELECT 'Toy Friendship' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as `Sessions per user`
                                , sum(time_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                                , sum(time_spent)/session/60 as `Session Duration`
                                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from toy_fs  
union
SELECT 'Avatar House' as feature, count (*) as session, session/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as `Sessions per user`
                                , (SELECT sum(time_spent) from avatarhouse_time) / (case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                                , (SELECT sum(time_spent) from avatarhouse_time) /session/60 as `Session Duration`
                                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from avatar_house  
union
SELECT 'Parental Section' as feature, count (*) as session, session/ (case when count (distinct user_id) = 0 then null else count (distinct user_id) end) as `Sessions per user`
                                , sum(realtime_spent)/(case when count (distinct user_id) = 0 then null else count (distinct user_id) end)/60 as `Time spent per user`
                                , sum(realtime_spent)/session/60 as `Session Duration`
                                ,concat(floor(`Time spent per user`), ' min ',round((`Time spent per user` - floor(`Time spent per user`)) * 60) ,' sec') as `Time spent per user (min - sec)`
                                ,concat(floor(`Session Duration`), ' min ',round((`Session Duration` - floor(`Session Duration`)) * 60) ,' sec') as `Session Duration (min - sec)`
from parental_section 
)
where session > 0
)
order by `Time spent per user` desc
      )
    );
    
  ELSE
    SET r14_q3094 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as feature, CAST(value2 as INT64) as `session`, CAST(value3 as FLOAT64) as `Sessions per user`, CAST(value4 as FLOAT64) as `Time spent per user`, CAST(value5 as FLOAT64) as `Session Duration`, CAST(value6_str as STRING) as `Time spent per user min - sec`, CAST(value7_str as STRING) as `Session Duration min - sec`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 3094 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q3094);
  