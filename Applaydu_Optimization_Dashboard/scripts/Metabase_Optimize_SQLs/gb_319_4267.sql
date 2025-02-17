with minigame_done as (
select user_id AS user_id,
`FED_ID` AS `FED_ID`,
`PLATFORM` AS `PLATFORM`,
game_id AS game_id,
`EVENT_ID` AS `EVENT_ID`,
client_time AS client_time,server_time AS server_time,
version AS version,
country AS country,
`SESSION_ID` AS `SESSION_ID`,
`TOKEN` AS `TOKEN`,
`CURRENT_ELO` AS `CURRENT_ELO`,
`HINT_TRIGGERED` AS `HINT_TRIGGERED`,
`AVATAR_GENDER` AS `AVATAR_GENDER`,
`END_CAUSE` AS `END_CAUSE`,
scene_name AS scene_name,
`TIMES_REPLAYED` AS `TIMES_REPLAYED`,
`GAME_DIFFICULTY` AS `GAME_DIFFICULTY`,
`GAME_MISTAKES` AS `GAME_MISTAKES`,
`ACCESS_POINT` AS `ACCESS_POINT`,
`EVENT_client_time_LOCAL` AS `EVENT_client_time_LOCAL`,
click_from AS click_from,
`IS_MULTI` AS `IS_MULTI`,
`QUIT_PHASE` AS `QUIT_PHASE`,
`TOY_NAME` AS `TOY_NAME`,
case when REALTIME_SPENT is null then time_to_finish else REALTIME_SPENT end AS `REALTIME_SPENT`,
load_time AS load_time,
case when from_scene is null then 'Not yet available' else from_scene end as from_scene,
`AVATAR_DIFFICULTY` AS `AVATAR_DIFFICULTY`,
`TOY_UNLOCKED_METHOD` AS `TOY_UNLOCKED_METHOD`
FROM gcp-bi-elephant-db-gold.applaydu.minigame_finished
)
(SELECT replace(replace(replace(replace(replace(replace(scene_name
,'Vocabulary','Word Explorer')
,'Princess Tower','Tower Block')
,'Code spark','Codeplaydu')
,'Chase Ace','Chase & Ace')
,'Happos Runner','Happos Fun Run')
,'Natoons','Food Quest')
 as `Minigame`
,count(distinct user_id) as `Users count`
,count(0) as `Sessions`
,`Sessions`/`Users count` as `Sessions per user`
,sum(realtime_spent) as total_time_spent
,(total_time_spent/`Users count`/60) as `Average time spent per game per user (minute)`
,(total_time_spent/`Sessions`/60) as `Average time spent per game per session (minute)`
,concat(floor(`Average time spent per game per user (minute)`),' min ',round((`Average time spent per game per user (minute)` - floor(`Average time spent per game per user (minute)`)) * 60),' sec') as `Average time spent per game per user (min - sec)`
,concat(floor(`Average time spent per game per session (minute)`),' min ',round((`Average time spent per game per session (minute)` - floor(`Average time spent per game per session (minute)`)) * 60),' sec') as `Average time spent per game per session (min - sec)`
FROM minigame_done t
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
 join `applaydu.tbl_shop_filter` on `applaydu.tbl_shop_filter`.game_id=t.game_id and `applaydu.tbl_shop_filter`.country=t.country
WHERE 1=1
 and (date(client_time)>='2020-08-10' and date(client_time)<DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{from_version}}]]) and version<=(select max(version) from `applaydu.tbl_version_filter` where 1=1 [[AND {{to_version}}]])
 and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
 and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]]) and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and t.country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
 [[AND {{ishopfilter}}]]
 and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas','Link The World','Wheel','Wheel Map')
 and scene_name not like '%Playability%' and scene_name not like 'Stylin%'
 and from_scene not in ('Eduland AvatarHouse' ) --remove from dedicated experience 'Story Crafting','Story Natoons Documentary'?
 and ( ( scene_name='Move Ahead' and realtime_spent>12) -- to exclude users quitting during loading screen + cover 99% users
 or (scene_name<>'Move Ahead' and realtime_spent>=0 and realtime_spent<36000) )
 and realtime_spent>=0 and realtime_spent<36000
group by scene_name
)
order by `Average time spent per game per user (minute)` desc