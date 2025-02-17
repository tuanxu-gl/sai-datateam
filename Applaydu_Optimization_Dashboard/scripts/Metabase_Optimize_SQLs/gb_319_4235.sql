WITH q4235 as (select 4235),
tbl_story_mode_finished as (
 select user_id,FED_ID,PLATFORM,game_id,EVENT_ID,min(client_time),min(client_time),version,country,SESSION_ID
,min(TOKEN),AVATAR_GENDER,END_CAUSE,TOY_NAME,STORY_STEP,avg(TIME_TO_FINISH)
,ACTIVITY_01,ACTIVITY_01_VALUE,ACTIVITY_02,ACTIVITY_02_VALUE,ACTIVITY_03,ACTIVITY_03_VALUE,ACTIVITY_04
,ACTIVITY_04_VALUE,ACTIVITY_05,ACTIVITY_05_VALUE,AVATAR_ONESIE,click_from,ENVIRONMENT_ID,min(EVENT_client_time_LOCAL)
,avg(REALTIME_SPENT),min(load_time),ACTIVITY_06,ACTIVITY_06_VALUE,ACTIVITY_07,ACTIVITY_07_VALUE
,ACTIVITY_08,ACTIVITY_08_VALUE,ACTIVITY_09,ACTIVITY_09_VALUE,ACTIVITY_10,ACTIVITY_10_VALUE,TOY_UNLOCKED_METHOD,from_scene
 from gcp-bi-elephant-db-gold.applaydu.story_mode_finished 
 where (version >='5.0.0' AND DATE(client_time) >= '2024-08-28') and version<'5.2.0' 
 and (environment_id='Experience - Dino Museum' and version>='4.7.0')
 group by all
)
,real_story_mode_finished as (
SELECT user_id,game_id,event_id,version,country,session_id,avatar_gender,end_cause,toy_name,story_step,realtime_spent,environment_id,client_time,toy_unlocked_method,count(*) as dup
FROM 
 (
 -- exclude Dino Exp
 select * from gcp-bi-elephant-db-gold.applaydu.story_mode_finished
 WHERE 
 environment_id like 'Natoons v4%' or
 -- DUPLICATIONS in those Experience
 (environment_id like '%Travel%' and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
 (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause<>'Finished' or (end_cause='Finished' and story_step='Ending') ) ) or
 (environment_id NOT IN ('Savannah','Space','Ocean','Jungle','Magic Land','Experience - Dino Museum') AND (environment_id not LIKE '%Travel%') ) or 
 (environment_id='Kinderini' and date(client_time)>=(select date(ivalue) from gcp-gfb-sai-tracking-gold.applaydu.tbl_variables where ikey='apd_kinderini_start_date') ) or 
 (environment_id='Eduland Lets Story' and date(client_time)>=(select date(ivalue) from gcp-gfb-sai-tracking-gold.applaydu.tbl_variables where ikey='apd_v5_lets_story_start_date'))
 -- Dino 
 union all
 -- version<'5.0.0' or (version >='5.2.0' AND DATE(client_time) >= '2024-10-19') (normal)
 select * from gcp-bi-elephant-db-gold.applaydu.story_mode_finished 
 WHERE (version<'5.0.0' or (version >='5.2.0' AND DATE(client_time) >= '2024-10-19'))
 and (environment_id='Experience - Dino Museum' and version>='4.7.0')
 union all
 -- (version >='5.0.0' AND DATE(client_time) >= '2024-08-28') and version<'5.2.0'
 select * from tbl_story_mode_finished
 )
group by all
)
,dedicated as (
SELECT user_id,realtime_spent
from real_story_mode_finished
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
union all
SELECT user_id,realtime_spent
from gcp-bi-elephant-db-gold.applaydu.ILLUSTRATION_BOOK_FINISHED
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
)
,minigame_done as (
select user_id AS user_id,
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
)
,minigame as (
SELECT user_id,realtime_spent
FROM minigame_done
where 1=1 
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas')
 and from_scene<>'Eduland AvatarHouse' -- EXCLUDE Minigame Drawing in AvatarHouse from Minigame
 and scene_name not like '%Playability%' 
 and( (scene_name<>'Move Ahead'and realtime_spent>=0) or ( scene_name='Move Ahead' and realtime_spent>12) )-- to exclude users quitting during loading screen + cover 99% users
)
,toy_fs as (
--tracking TOY_FRIENDSHIP_FINISHED before v4.6.1
select user_id,sum(time_spent) as total_time,count (*) as session
from gcp-bi-elephant-db-gold.applaydu.TOY_FRIENDSHIP_FINISHED
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'4.6.1' and date(client_time)<CURRENT_DATE()
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and scene_name like 'Eduland%'
 and time_spent>=0 and time_spent<7200
group by 1
--change tracking ACTIVITY_FINISHED since v4.6.1
union all
select user_id,sum(time_spent) as total_time,count (*) as session
from gcp-bi-elephant-db-gold.applaydu.ACTIVITY_FINISHED
where 1=1
 and (version >='4.6.1' AND DATE(client_time) >= '2024-03-11') and version<'9.0.0' and date(client_time)<CURRENT_DATE()
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and feature='Toy Friendship'
 and activity_01='TFS Current Heart Point' -- first row data only for Natoons and Fantasy event returns 2 rows
 and time_spent>=0 and time_spent<7200
group by 1
union all
--change tracking ACTIVITY_FINISHED since v5.2.0
select user_id,total_time,session 
from (
(
select user_id,sum(time_spent) as total_time
from gcp-bi-elephant-db-gold.applaydu.ACTIVITY_FINISHED
where 1=1
 and (version >='5.2.0' AND DATE(client_time) >= '2024-10-19') and version<'5.4.0' and date(client_time)<CURRENT_DATE()
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and feature='Toy Friendship'
 and activity_01='TFS Minigame Index Check'
 and time_spent>=0 and time_spent<7200 
group by 1
) a join (
select user_id,count(token) as session
from gcp-bi-elephant-db-gold.applaydu.TOY_FRIENDSHIP_STARTED
where 1=1
 and (version >='5.2.0' AND DATE(client_time) >= '2024-10-19') and version<'5.4.0'
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) 
 and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]])
 and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]) 
 and date(client_time)>='2020-08-10' and date(client_time)<CURRENT_DATE()
group by 1
) b using (user_id))
union all
--change tracking ACTIVITY_FINISHED since v5.4.0
select user_id,sum(session_timespent) as total_time,count(session_id) as session
from(
 select user_id,ACTIVITY_10_VALUE as session_id,sum(time_spent) as session_timespent
 from gcp-bi-elephant-db-gold.applaydu.ACTIVITY_FINISHED
 where 1=1
 and (version >='5.4.0' AND DATE(client_time) >= '2024-12-04') and version<'9.0.0' and date(client_time)<CURRENT_DATE()
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and feature='Toy Friendship'
 and activity_01='TFS Minigame Index Check'
 and time_spent>=0 and time_spent<7200 
 group by 1,2
)
group by 1
)
,ar_mode as (
select user_id,realtime_spent
FROM gcp-bi-elephant-db-gold.applaydu.AR_MODE_FINISHED
where 1=1 
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
union all
select user_id,realtime_spent
from gcp-bi-elephant-db-gold.applaydu.FACE_MASK_FINISHED
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
)
,avatar_house as (
-- OLD TRACKING: before 4.5.0 use visit_screen
select user_id
from gcp-bi-elephant-db-gold.applaydu.VISIT_SCREEN
where 1=1 
 and screen_to like 'Eduland%Avatar%'
 and (version >='4.3.0' AND DATE(client_time) >= '2023-11-24') and version<'4.5.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
union all
select user_id
from gcp-bi-elephant-db-gold.applaydu.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent>=0 and from_scene<>'Inkmagination' -- when users from AH Drawing to AH,we do not count as a new session
 and (version >='4.5.0' AND DATE(client_time) >= '2024-02-05') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
)
,avatarhouse_time as (-- include Minigame Drawing from AH 
-- OLD TRACKING: before 4.5.0 use visit_screen
select time_spent
from gcp-bi-elephant-db-gold.applaydu.VISIT_SCREEN
where 1=1 and time_spent>=0 and time_spent<36000
 and (screen_from like '%Avatar%' or (screen_from like '%Ink%' and screen_to like '%Avatar%')) -- INCLUDE TIME SPENT IN MINIGAME DRAWING IN AH
 and (version >='4.3.0' AND DATE(client_time) >= '2023-11-24') and version<'4.5.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
-- NEW TRACKING: after 4.5.0 use AH + MINIGAME DRAWING AH
union all
select time_spent
from gcp-bi-elephant-db-gold.applaydu.AVATAR_HOUSE_FINISHED
where 1=1 and time_spent>=0
 and (version >='4.5.0' AND DATE(client_time) >= '2024-02-05') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
union all -- INCLUDE TIME_SPENT MINIGAME DRAWING in AH
select REALTIME_SPENT
from gcp-bi-elephant-db-gold.applaydu.minigame_finished
where scene_name='Inkmagination'
 and from_scene='Eduland AvatarHouse'
 and (version >='4.5.0' AND DATE(client_time) >= '2024-02-05') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
)
,parental_section as (
select user_id,realtime_spent
from gcp-bi-elephant-db-gold.applaydu.PARENTAL_SECTION
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
)
select 'Dedicated Experience' as feature,count (*) as session,session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user (min)`
,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/session/60 as `Session Duration`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from dedicated 
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union
select 'AR' as feature,count (*) as session,session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user (min)`
,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/session/60 as `Session Duration`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from ar_mode 
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union
select 'Minigame' as feature,count (*) as session,session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user (min)`
,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/session/60 as `Session Duration`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from minigame 
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union
select 'Toy Friendship' as feature,sum(session) as session_,session_/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(total_time)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user (min)`
,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(total_time)/session_/60 as `Session Duration`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from toy_fs 
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
group by 1
union
select 'Avatar House' as feature,count (*) as session,session/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,(select sum(time_spent) from avatarhouse_time) / (case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user (min)`
,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,(select sum(time_spent) from avatarhouse_time) /session/60 as `Session Duration`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from avatar_house 
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
union
select 'Parental Section' as feature,count (*) as session,session/ (case when count (distinct user_id)=0 then null else count (distinct user_id) end) as `Sessions per user`
,sum(realtime_spent)/(case when count (distinct user_id)=0 then null else count (distinct user_id) end) /60 as `Time spent per user (min)`
,concat(floor(`Time spent per user (min)`),' min ',round((`Time spent per user (min)` - floor(`Time spent per user (min)`)) * 60),' sec') as `Time spent per user (min - sec)`
,sum(realtime_spent)/session/60 as `Session Duration`
,concat(floor(`Session Duration`),' min ',round((`Session Duration` - floor(`Session Duration`)) * 60),' sec') as `Session Duration (min)`
from parental_section 
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
order by `Time spent per user (min)` desc