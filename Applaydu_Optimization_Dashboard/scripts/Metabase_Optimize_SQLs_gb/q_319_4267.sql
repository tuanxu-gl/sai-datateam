DECLARE r319_q4267 ARRAY<STRUCT<`Minigame` STRING,`Users count` INT64,`Sessions` INT64,`Sessions per user` FLOAT64,total_time_spent INT64,`Average time spent per game per user minute` FLOAT64,`Average time spent per game per session minute` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4267
);

IF row_count = 0 THEN
  SET r319_q4267 = (
    SELECT ARRAY(
      with gb4267 as (SELECT 0)
--main query
SELECT AS STRUCT replace(replace(replace(replace(replace(replace(scene_name
,'Vocabulary','Word Explorer')
,'Princess Tower','Tower Block')
,'Code spark','Codeplaydu')
,'Chase Ace','Chase & Ace')
,'Happos Runner','Happos Fun Run')
,'Natoons','Food Quest')
 as `Minigame`
,count(distinct user_id) as `Users count`
,count(0) as `Sessions`
,count(0)/count(distinct user_id) as `Sessions per user`
,sum(COALESCE(realtime_spent,time_to_finish)) as total_time_spent
,(sum(COALESCE(realtime_spent,time_to_finish))/count(distinct user_id)/60) as `Average time spent per game per user minute`
,(sum(COALESCE(realtime_spent,time_to_finish))/count(0)/60) as `Average time spent per game per session minute`
--,concat(floor(`Average time spent per game per user (minute)`),' min ',round((`Average time spent per game per user (minute)` - floor(`Average time spent per game per user (minute)`)) * 60),' sec') as `Average time spent per game per user (min - sec)`
--,concat(floor(`Average time spent per game per session (minute)`),' min ',round((`Average time spent per game per session (minute)` - floor(`Average time spent per game per session (minute)`)) * 60),' sec') as `Average time spent per game per session (min - sec)`
FROM `gcp-bi-elephant-db-gold.applaydu.minigame_finished` t
 join (SELECT distinct user_id from `gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY` where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
 join `applaydu.tbl_shop_filter` on `applaydu.tbl_shop_filter`.game_id=t.game_id and `applaydu.tbl_shop_filter`.country=t.country
WHERE 1=1
 and (date(client_time)>='2020-08-10' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
 and version>=(SELECT min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(SELECT max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 [[AND {{ishopfilter}}]]
 and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas','Link The World','Wheel','Wheel Map')
 and scene_name not like '%Playability%' and scene_name not like 'Stylin%'
 and from_scene not in ('Eduland AvatarHouse' ) --remove from dedicated experience 'Story Crafting','Story Natoons Documentary'?
 and ( ( scene_name='Move Ahead' and realtime_spent>12) -- to exclude users quitting during loading screen + cover 99% users
 or (scene_name<>'Move Ahead' and realtime_spent>=0 and realtime_spent<36000) )
-- and (realtime_spent is not null and realtime_spent>=0 and realtime_spent<36000)
group by scene_name
order by `Average time spent per game per user minute` desc
    )
  );
  
ELSE
  SET r319_q4267 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Minigame`, CAST(value2 as INT64) as `Users count`, CAST(value3 as INT64) as `Sessions`, CAST(value4 as FLOAT64) as `Sessions per user`, CAST(value5 as INT64) as total_time_spent, CAST(value6 as FLOAT64) as `Average time spent per game per user minute`, CAST(value7 as FLOAT64) as `Average time spent per game per session minute`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4267 
  );
END IF;

SELECT * FROM UNNEST(r319_q4267);
