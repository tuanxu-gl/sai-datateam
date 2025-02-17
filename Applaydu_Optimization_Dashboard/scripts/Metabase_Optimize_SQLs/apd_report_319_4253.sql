insert into APPLAYDU_NOT_CERTIFIED.apd_report_319 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,value1,value2)


with launch_resume as(
SELECT  'Active' as feature , user_id
FROM APPLAYDU.LAUNCH_RESUME
       
where 1=1 
  and version >= '4.0.0' and version < '9.0.0'
  and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v4_start_date')  
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
  and time_spent::float >= 0 and time_spent::float < 86400
)
, dedicate as (
SELECT  'Dedicated Exp' as feature, user_id
from APPLAYDU.STORY_MODE_TRIGGERED
where  1=1
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
-- STORY_MODE_TRIGGERED missing data on some scene_name so we union STORY_MODE_FINISHED to retrieve some
union
SELECT distinct 'Dedicated Exp' as feature, user_id
from APPLAYDU.STORY_MODE_FINISHED
where  1=1
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
union
SELECT  'Dedicated Exp' as feature, user_id
from APPLAYDU.ILLUSTRATION_BOOK_TRIGGERED
where 1=1
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
)

, ar_mode as
(
SELECT  'AR' as feature, user_id
FROM APPLAYDU.AR_MODE_TRIGGERED
where 1=1 
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    

union
SELECT distinct 'AR' as feature, user_id
from APPLAYDU.FACE_MASK_TRIGGERED
where 1=1
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
)

, minigame as
(
SELECT  'Minigame' as feature,  user_id
FROM APPLAYDU.MINIGAME_STARTED
where 1=1 
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
    and scene_name not in ('NBA_1','NBA_2','Happos Runner','Natoon RunnerV2','Inkmagination_Xmas','Main Menu')
    and scene_name not like '%Playability%'
)
,tbl_toy_friendship as (
SELECT 'Toy FS' as feature,  user_id
from APPLAYDU.TOY_FRIENDSHIP_STARTED
where 1=1 
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
  --and scene_name like 'Eduland%'
)
, tbl_avatar as (
select  'Avatar House' as feature, user_id
from APPLAYDU.VISIT_SCREEN
where 1=1 
    and screen_to like 'Eduland%Avatar%'
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    

union
select distinct 'Avatar House' as feature,  user_id
from APPLAYDU.avatar_house_triggered
where 1=1 
  and version >= '4.5.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
)
, parental_section as (
select distinct 'Parental' as feature, user_id
from APPLAYDU.PARENTAL_SECTION
where 1=1
    and version >= '4.0.0' and version < '9.0.0' 
     
    
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    
)

 
select 319 as dashboard_id
    ,4253 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'[Monthly Report]% Users Access the Feature' as kpi_name
    ,feature as dimension1
    ,users as value1
    ,"% User Access the Feature" as value2
from
(

select 'Dedicated Experience' as feature
    , (select count (distinct user_id) from dedicate 
        ) as Users
    , Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"
union

select 'AR' as feature
, (select count (distinct user_id) from ar_mode
        ) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'Minigame' as feature
, (select count (distinct user_id) from minigame
        ) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'Toy Friendship' as feature
, (select count (distinct user_id) from tbl_toy_friendship
        ) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'Avatar House' as feature
, (select count (distinct user_id) from tbl_avatar
        ) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'Parental' as feature
, (select count (distinct user_id) from parental_section
        ) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

order by 2 desc
)