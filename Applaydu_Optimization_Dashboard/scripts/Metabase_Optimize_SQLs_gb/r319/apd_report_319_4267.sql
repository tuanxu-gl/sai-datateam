insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6,value7)
with gb4267 as (select 0)
--main query

select 319 as dashboard_id
		,4267 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Average time spent per game per user' as kpi_name
		,`Minigame` as value1_str,`Users count` as value2,`Sessions` as value3,`Sessions per user` as value4,total_time_spent as value5,`Average time spent per game per user minute` as value6,`Average time spent per game per session minute` as value7
	from
	(
	
select replace(replace(replace(replace(replace(replace(scene_name
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
from `gcp-bi-elephant-db-gold.applaydu.minigame_finished` t
 join (select distinct user_id from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) ) using (user_id)
 join `applaydu.tbl_shop_filter` on `applaydu.tbl_shop_filter`.game_id=t.game_id and `applaydu.tbl_shop_filter`.country=t.country
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
 and (date(client_time)>='2020-08-10' )
 and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 ) and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
 and version in (select version from `applaydu.tbl_version_filter` where 2=2 )
 and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
 and t.country in (select country from `applaydu.tbl_country_filter` where 2=2  )
 and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas','Link The World','Wheel','Wheel Map')
 and scene_name not like '%Playability%' and scene_name not like 'Stylin%'
 and from_scene not in ('Eduland AvatarHouse' ) --remove from dedicated experience 'Story Crafting','Story Natoons Documentary'?
 and ( ( scene_name='Move Ahead' and realtime_spent>12) -- to exclude users quitting during loading screen + cover 99% users
 or (scene_name<>'Move Ahead' and realtime_spent>=0 and realtime_spent<36000) )
-- and (realtime_spent is not null and realtime_spent>=0 and realtime_spent<36000)
group by scene_name
order by `Average time spent per game per user minute` desc
)