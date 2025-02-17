with r451 as(
SELECT value1_str as "Minigame",value2 as "Users count",value3 as "Sessions",value4 as "Sessions per user",value5 as total_time_spent,value6 as "Average time spent per game per user (minute)",value7 as "Average time spent per game per session (minute)",value8_str as "Average time spent per game per user (min - sec)",value9_str as "Average time spent per game per session (min - sec)"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 451 
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
        WHERE 1=1
        AND start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
		 and dashboard_id=14 and query_id = 451
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q451 as (select 0),
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
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'
)

select * from r451
union
select * from
(

(SELECT replace(replace(replace(replace(replace(replace(scene_name
                    ,'Vocabulary','Word Explorer')
                    ,'Princess Tower','Tower Block')
                    ,'Code spark','Codeplaydu')
                    ,'Chase Ace','Chase & Ace')
                    ,'Happos Runner','Happos Fun Run')
                    ,'Natoons','Food Quest')
                     as "Minigame"
    , count(distinct USER_ID)   as "Users count"
    , count(0)   as "Sessions"
    ,"Sessions"/"Users count" as "Sessions per user"
    , sum(realtime_spent) as total_time_spent
    , (total_time_spent/"Users count"/60) as  "Average time spent per game per user (minute)"
    , (total_time_spent/"Sessions"/60) as  "Average time spent per game per session (minute)"
    ,concat(floor("Average time spent per game per user (minute)"), ' min ',round(("Average time spent per game per user (minute)" - floor("Average time spent per game per user (minute)")) * 60) ,' sec') as "Average time spent per game per user (min - sec)"
    ,concat(floor("Average time spent per game per session (minute)"), ' min ',round(("Average time spent per game per session (minute)" - floor("Average time spent per game per session (minute)")) * 60) ,' sec') as "Average time spent per game per session (min - sec)"
FROM   minigame_done t
        join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country = t.country
WHERE  1=1
    and (client_time >= '2020-08-10' and client_time < dateadd(day, -3, CURRENT_DATE()))
    and version >= (select min(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{from_version}}]]) and version <= (select max(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{to_version}}]])
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
    and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and t.country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    [[AND {{ishopfilter}}]]
    and scene_name not in ('Main Menu','NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas','Link The World','Wheel','Wheel Map')
    and scene_name not like '%Playability%' and scene_name not like 'Stylin%'
    and from_scene not in ('Eduland AvatarHouse' )  --remove from dedicated experience 'Story Crafting','Story Natoons Documentary'?
    and ( ( scene_name = 'Move Ahead' and  realtime_spent::int > 12) -- to exclude users quitting during loading screen + cover 99% users
        or (scene_name <> 'Move Ahead'   and realtime_spent::int >= 0    and realtime_spent::int < 36000) )
    and realtime_spent::int >= 0    and realtime_spent::int < 36000
    and not (scene_name = 'PaleoKid' and (version >= '4.7.0' and client_time >= '2024-06-11'))
group by scene_name
)
order by "Average time spent per game per user (minute)" desc
)
where "Minigame" > 0
