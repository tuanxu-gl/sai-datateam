DECLARE r319_q4230 ARRAY<STRUCT<feature STRING,session INT64,`Sessions per user` FLOAT64,`Time spent per user min` FLOAT64,`Session Duration` FLOAT64,`Time spent per user min - sec` STRING,`Session Duration min` STRING>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4230
);

IF row_count = 0 THEN
  SET r319_q4230 = (
    SELECT ARRAY(
      WITH gb4230 as (SELECT 4230),
tbl_story_mode_finished as (
 SELECT user_id,FED_ID,PLATFORM,game_id,EVENT_ID,min(client_time),min(server_time),version,country,SESSION_ID
,min(TOKEN),AVATAR_GENDER,END_CAUSE,TOY_NAME,STORY_STEP,avg(TIME_TO_FINISH)
,ACTIVITY_01,ACTIVITY_01_VALUE,ACTIVITY_02,ACTIVITY_02_VALUE,ACTIVITY_03,ACTIVITY_03_VALUE,ACTIVITY_04
,ACTIVITY_04_VALUE,ACTIVITY_05,ACTIVITY_05_VALUE,AVATAR_ONESIE,click_from,ENVIRONMENT_ID,min(EVENT_client_time_LOCAL)
,avg(REALTIME_SPENT),min(load_time),ACTIVITY_06,ACTIVITY_06_VALUE,ACTIVITY_07,ACTIVITY_07_VALUE
,ACTIVITY_08,ACTIVITY_08_VALUE,ACTIVITY_09,ACTIVITY_09_VALUE,ACTIVITY_10,ACTIVITY_10_VALUE,TOY_UNLOCKED_METHOD,from_scene
 from gcp-bi-elephant-db-gold.applaydu.story_mode_finished 
 where 1=1 and (version >='5.0.0' AND DATE(client_time) >= '2024-08-28') and version<'5.2.0' 
 and (environment_id='Experience - Dino Museum' and version>='4.7.0')
 group by all
)
,real_story_mode_finished as (
SELECT user_id,game_id,event_id,version,country,session_id,avatar_gender,end_cause,toy_name,story_step,realtime_spent,environment_id,client_time,toy_unlocked_method,count(*) as dup
FROM 
 (
 -- exclude Dino Exp
 SELECT * from gcp-bi-elephant-db-gold.applaydu.story_mode_finished
 WHERE 1=1 and
 environment_id like 'Natoons v4%' or
 -- DUPLICATIONS in those Experience
 (environment_id like '%Travel%' and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
 (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
 (environment_id NOT IN ('Savannah','Space','Ocean','Jungle','Magic Land','Experience - Dino Museum') AND (environment_id not LIKE '%Travel%') ) or 
 (environment_id='Kinderini' and date(client_time)>=(SELECT date(ivalue) from gcp-gfb-sai-tracking-gold.applaydu.tbl_variables where ikey='apd_kinderini_start_date') ) or 
 (environment_id='Eduland Lets Story' and date(client_time)>=(SELECT date(ivalue) from gcp-gfb-sai-tracking-gold.applaydu.tbl_variables where ikey='apd_v5_lets_story_start_date'))
 -- Dino 
 union all
 -- version<'5.0.0' or (version >='5.2.0' AND DATE(client_time) >= '2024-10-19') (normal)
 SELECT * from gcp-bi-elephant-db-gold.applaydu.story_mode_finished 
 WHERE 1=1 and (version<'5.0.0' or (version >='5.2.0' AND DATE(client_time) >= '2024-10-19'))
 and (environment_id='Experience - Dino Museum' and version>='4.7.0')
 union all
 -- (version >='5.0.0' AND DATE(client_time) >= '2024-08-28') and version<'5.2.0'
 SELECT * from tbl_story_mode_finished
 )
group by all
)
,dedicated as (
SELECT user_id,realtime_spent
from real_story_mode_finished
where (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
union all
SELECT user_id,realtime_spent
from gcp-bi-elephant-db-gold.applaydu.ILLUSTRATION_BOOK_FINISHED
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
)
,minigame_done as (
SELECT user_id AS user_id,
game_id AS game_id,
client_time AS client_time,server_time AS server_time,
version AS version,
country AS country,
scene_name AS scene_name,
click_from AS click_from,
case when REALTIME_SPENT is null then time_to_finish else REALTIME_SPENT end AS realtime_spent,
load_time AS load_time,
case when from_scene is null then 'Not yet available' else from_scene end as from_scene,
FROM gcp-bi-elephant-db-gold.applaydu.minigame_finished
where 1=1
)
,minigame as (
SELECT user_id,realtime_spent
FROM minigame_done
where (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas')
 and from_scene<>'Eduland AvatarHouse' -- EXCLUDE Minigame Drawing in AvatarHouse from Minigame
 and scene_name not like '%Playability%' 
 and( (scene_name<>'Move Ahead'and realtime_spent>=0) or ( scene_name='Move Ahead' and realtime_spent>12) )-- to exclude users quitting during loading screen + cover 99% users
)
,toy_fs as (
--tracking TOY_FRIENDSHIP_FINISHED before v4.6.1
SELECT user_id,time_spent,CAST(client_time AS STRING) AS tfs_session
from gcp-bi-elephant-db-gold.applaydu.TOY_FRIENDSHIP_FINISHED
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'4.6.1' and date(client_time)<CURRENT_DATE()
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and scene_name like 'Eduland%'
 and time_spent>=0 and time_spent<7200
--change tracking ACTIVITY_FINISHED since v4.6.1
union all
SELECT user_id,time_spent,CAST(client_time AS STRING) AS tfs_session
from gcp-bi-elephant-db-gold.applaydu.ACTIVITY_FINISHED
where 1=1
 and (version >='4.6.1' AND DATE(client_time) >= '2024-03-11') and version<'5.2.0' and date(client_time)<CURRENT_DATE()
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and feature='Toy Friendship'
 and activity_01='TFS Current Heart Point' -- first row data only for Natoons and Fantasy event returns 2 rows
 and time_spent>=0 and time_spent<7200
union all 
--change tracking ACTIVITY_FINISHED since v5.2.0
SELECT user_id,sum(time_spent) as total_time,CAST(client_time AS STRING) AS tfs_session
from gcp-bi-elephant-db-gold.applaydu.ACTIVITY_FINISHED
where 1=1
 and (version >='5.2.0' AND DATE(client_time) >= '2024-10-19') and version<'5.4.0' and date(client_time)<CURRENT_DATE()
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and feature='Toy Friendship'
 and activity_01='TFS Minigame Index Check'
 and time_spent>=0 and time_spent<7200 
group by all
union all 
--change tracking ACTIVITY_FINISHED since v5.4.0
SELECT user_id,sum(time_spent) as total_time,CAST(activity_10_value AS STRING) AS tfs_session
from gcp-bi-elephant-db-gold.applaydu.ACTIVITY_FINISHED
where 1=1
 and (version >='5.4.0' AND DATE(client_time) >= '2024-12-04') and version<'9.0.0' and date(client_time)<CURRENT_DATE()
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and feature='Toy Friendship'
 and activity_01='TFS Minigame Index Check'
 and time_spent>=0 and time_spent<7200 
group by 1,3
)
,ar_mode as (
SELECT user_id,realtime_spent
FROM gcp-bi-elephant-db-gold.applaydu.AR_MODE_FINISHED
where 1=1 
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
union all
SELECT user_id,realtime_spent
from gcp-bi-elephant-db-gold.applaydu.FACE_MASK_FINISHED
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
)
,avatar_house as (
-- OLD TRACKING: before 4.5.0 use visit_screen
SELECT user_id
from gcp-bi-elephant-db-gold.applaydu.VISIT_SCREEN
where 1=1 
 and screen_to like 'Eduland%Avatar%'
 and (version >='4.3.0' AND DATE(client_time) >= '2023-11-24') and version<'4.5.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
union all
SELECT user_id
from gcp-bi-elephant-db-gold.applaydu.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent>=0 and from_scene<>'Inkmagination' -- when users from AH Drawing to AH,we do not count as a new session
 and (version >='4.5.0' AND DATE(client_time) >= '2024-02-05') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
)
,avatarhouse_time as (-- include Minigame Drawing from AH 
-- OLD TRACKING: before 4.5.0 use visit_screen
SELECT time_spent
from gcp-bi-elephant-db-gold.applaydu.VISIT_SCREEN
where 1=1 and time_spent>=0 and time_spent<36000
 and (screen_from like '%Avatar%' or (screen_from like '%Ink%' and screen_to like '%Avatar%')) -- INCLUDE TIME SPENT IN MINIGAME DRAWING IN AH
 and (version >='4.3.0' AND DATE(client_time) >= '2023-11-24') and version<'4.5.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
-- NEW TRACKING: after 4.5.0 use AH + MINIGAME DRAWING AH
union all
SELECT time_spent
from gcp-bi-elephant-db-gold.applaydu.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent>=0
 and (version >='4.5.0' AND DATE(client_time) >= '2024-02-05') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
union all -- INCLUDE TIME_SPENT MINIGAME DRAWING in AH
SELECT REALTIME_SPENT
from gcp-bi-elephant-db-gold.applaydu.minigame_finished
where scene_name='Inkmagination'
 and from_scene='Eduland AvatarHouse'
 and (version >='4.5.0' AND DATE(client_time) >= '2024-02-05') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
)
,parental_section as (
SELECT user_id,realtime_spent
from gcp-bi-elephant-db-gold.applaydu.PARENTAL_SECTION
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
)
,result as
(
SELECT 'Dedicated Experience' as feature
,count (*) as session
,count (*)/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user min`
--,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/count (*)/60 as `Session Duration`
--,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from dedicated 
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union all
SELECT 'AR' as feature,count (*) as session
,count (*)/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user min`
--,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/count (*)/60 as `Session Duration`
--,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from ar_mode 
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union all
SELECT 'Minigame' as feature
,count (*) as session
,count (*)/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user min`
--,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/count (*)/60 as `Session Duration`
--,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from minigame 
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union all
SELECT 'Toy Friendship' as feature,count (*) as session
,count (*)/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(time_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user min`
--,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(time_spent)/count (*)/60 as `Session Duration`
--,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from toy_fs 
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
group by 1
union all
SELECT 'Avatar House' as feature,count (*) as session
,count (*)/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,(SELECT sum(time_spent) from avatarhouse_time) / (case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user min`
--,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,(SELECT sum(time_spent) from avatarhouse_time) /count (*)/60 as `Session Duration`
--,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from avatar_house 
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union all
SELECT 'Parental Section' as feature,count (*) as session
,count (*)/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user min`
--,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/count (*)/60 as `Session Duration`
--,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from parental_section 
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
)
--main query
SELECT AS STRUCT *
,concat(floor(`Time spent per user min`),' min ',round((`Time spent per user min` - floor(`Time spent per user min`)) * 60),' sec') as `Time spent per user min - sec`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration min`
 from result
order by session desc
    )
  );
  
ELSE
  SET r319_q4230 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as feature, CAST(value2 as INT64) as session, CAST(value3 as FLOAT64) as `Sessions per user`, CAST(value4 as FLOAT64) as `Time spent per user min`, CAST(value5 as FLOAT64) as `Session Duration`, CAST(value6_str as STRING) as `Time spent per user min - sec`, CAST(value7_str as STRING) as `Session Duration min`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4230 
  );
END IF;

SELECT * FROM UNNEST(r319_q4230);
