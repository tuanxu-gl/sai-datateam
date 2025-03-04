DECLARE r319_q4264 ARRAY<STRUCT<environment STRING,`No of Session` INT64,`No of Users` INT64,`Sessions per user` FLOAT64,`Time spent per user in min` FLOAT64,`Time spent per session in min` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4264
);

IF row_count = 0 THEN
  SET r319_q4264 = (
    SELECT ARRAY(
      WITH gb4263 as (SELECT 0)
,tbl_story_mode_finished as (
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
,result as (
SELECT case when environment_id like 'Natoons v4%' then 'Natoons Experience'
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
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
where 1=1
 and (version >='4.0.0' AND DATE(client_time) >= '2023-08-22') and version<'9.0.0' and date(client_time)<CURRENT_DATE()
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 and realtime_spent>=0
group by 1
)
--main query
SELECT AS STRUCT * 
--,concat(floor(`Time spent per user in min`),' min ',round((`Time spent per user in min` - floor(`Time spent per user in min`)) * 60),' sec') as `Time spent per user in min sec`
--,concat(floor(`Time spent per session in min`),' min ',round((`Time spent per session in min` - floor(`Time spent per session in min`)) * 60),' sec') as `Time spent per session in min sec`
from result 
where Environment is not null
order by `Time spent per user in min` desc
    )
  );
  
ELSE
  SET r319_q4264 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as environment, CAST(value2 as INT64) as `No of Session`, CAST(value3 as INT64) as `No of Users`, CAST(value4 as FLOAT64) as `Sessions per user`, CAST(value5 as FLOAT64) as `Time spent per user in min`, CAST(value6 as FLOAT64) as `Time spent per session in min`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4264 
  );
END IF;

SELECT * FROM UNNEST(r319_q4264);
