insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4)
with q3064 as (select 0),
minigame_done as (
select "APPLAYDU"."MINIGAME_FINISHED"."USER_ID" AS "USER_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."FED_ID" AS "FED_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."PLATFORM" AS "PLATFORM", 
"APPLAYDU"."MINIGAME_FINISHED"."GAME_ID" AS "GAME_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."EVENT_ID" AS "EVENT_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."CLIENT_TIME" AS "CLIENT_TIME", 
"APPLAYDU"."MINIGAME_FINISHED"."SERVER_TIME" AS "SERVER_TIME", 
"APPLAYDU"."MINIGAME_FINISHED"."VERSION" AS "VERSION", 
"APPLAYDU"."MINIGAME_FINISHED"."COUNTRY" AS "COUNTRY", 
"APPLAYDU"."MINIGAME_FINISHED"."SESSION_ID" AS "SESSION_ID", 
"APPLAYDU"."MINIGAME_FINISHED"."TOKEN" AS "TOKEN", 
"APPLAYDU"."MINIGAME_FINISHED"."CURRENT_ELO" AS "CURRENT_ELO", 
"APPLAYDU"."MINIGAME_FINISHED"."HINT_TRIGGERED" AS "HINT_TRIGGERED", 
"APPLAYDU"."MINIGAME_FINISHED"."AVATAR_GENDER" AS "AVATAR_GENDER", 
"APPLAYDU"."MINIGAME_FINISHED"."END_CAUSE" AS "END_CAUSE", 
"APPLAYDU"."MINIGAME_FINISHED"."SCENE_NAME" AS "SCENE_NAME", 
"APPLAYDU"."MINIGAME_FINISHED"."TIMES_REPLAYED" AS "TIMES_REPLAYED", 
"APPLAYDU"."MINIGAME_FINISHED"."GAME_DIFFICULTY" AS "GAME_DIFFICULTY", 
"APPLAYDU"."MINIGAME_FINISHED"."GAME_MISTAKES" AS "GAME_MISTAKES", 
"APPLAYDU"."MINIGAME_FINISHED"."ACCESS_POINT" AS "ACCESS_POINT", 
"APPLAYDU"."MINIGAME_FINISHED"."EVENT_CLIENT_TIME_LOCAL" AS "EVENT_CLIENT_TIME_LOCAL", 
"APPLAYDU"."MINIGAME_FINISHED"."CLICK_FROM" AS "CLICK_FROM", 
"APPLAYDU"."MINIGAME_FINISHED"."IS_MULTI" AS "IS_MULTI", 
"APPLAYDU"."MINIGAME_FINISHED"."QUIT_PHASE" AS "QUIT_PHASE", 
"APPLAYDU"."MINIGAME_FINISHED"."TOY_NAME" AS "TOY_NAME", 
case when REALTIME_SPENT is null then time_to_finish else REALTIME_SPENT end AS "REALTIME_SPENT", 
"APPLAYDU"."MINIGAME_FINISHED"."LOAD_TIME" AS "LOAD_TIME", 
case when "FROM_SCENE" is null then 'Not yet available' else "FROM_SCENE" end as "FROM_SCENE", 
"APPLAYDU"."MINIGAME_FINISHED"."AVATAR_DIFFICULTY" AS "AVATAR_DIFFICULTY", 
"APPLAYDU"."MINIGAME_FINISHED"."TOY_UNLOCKED_METHOD" AS "TOY_UNLOCKED_METHOD"
FROM "ELEPHANT_DB"."APPLAYDU"."MINIGAME_FINISHED"
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
)
,
raw as 
(
SELECT user_id,replace(replace(replace(replace(replace(replace(scene_name
                    ,'Vocabulary','Word Explorer')
                    ,'Princess Tower','Tower Block')
                    ,'Code spark','Codeplaydu')
                    ,'Chase Ace','Chase & Ace')
                    ,'Happos Runner','Happos Fun Run')
                    ,'Natoons','Food Quest')
                     as scene_name, client_time, case when TIMEs_REPLAYED = 0 then 1
            when times_replayed < 2 then 2
            when times_replayed < 3 then 3
            when times_replayed < 4 then 4
            when times_replayed < 5 then 5
            when times_replayed < 6 then 6
            when times_replayed < 7 then 7
            when times_replayed < 8 then 8
            when times_replayed < 9 then 9
            when times_replayed < 10 then 10
            else 11 end as "Replay status" --11+
FROM   minigame_done t
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and (client_time >= '2020-08-10' and client_time < CURRENT_DATE()) and VERSION < '9.0.0'
    and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas','Link The World','Wheel','Wheel Map')
    and scene_name not like '%Playability%' and scene_name not like 'Stylin%'
    and from_scene not in ('Story Crafting','Story Natoons Documentary')  --remove from dedicated experience
    and not (scene_name = 'PaleoKid' and (version >= '4.7.0' and client_time >= '2024-06-11'))
)
, replay as (
select user_id, scene_name, sum("Replay status") as Total_Replay
from raw
group by 1,2
)
--main query

select 14 as dashboard_id
		,3064 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Users play at least 2 times by Mini Game' as kpi_name
		,minigame as value1_str,"Users replay" as value2,"Total users" as value3,percentage as value4
	from
	(
	
select scene_name as minigame
    , count (distinct (case when Total_Replay > 1 then user_id else null end) ) as "Users replay"
    , count (distinct user_id) as "Total users"
    ,  "Users replay" / "Total users" as percentage
from replay
group by 1
order by percentage desc
)