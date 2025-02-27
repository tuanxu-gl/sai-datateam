insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3)
--main query
WITH launch_resume AS (
  select 'Active' AS feature, user_id
from `gcp-bi-elephant-db-gold.applaydu.launch_resume`
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and version>='4.0.0'
  and date(client_time)>=(select CAST(ivalue AS DATE) from `applaydu.tbl_variables` where ikey='apd_v4_start_date')
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
),
dedicate AS (
  select 'Dedicated Exp' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.story_mode_triggered`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
  UNION all
  select DISTINCT 'Dedicated Exp' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
  UNION all
  select 'Dedicated Exp' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.illustration_book_triggered`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
),
ar_mode AS (
  select 'AR' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.ar_mode_triggered`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
  UNION all
  select DISTINCT 'AR' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.face_mask_triggered`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
),
minigame AS (
  select 'Minigame' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.minigame_started`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
    and scene_name NOT in ('NBA_1', 'NBA_2', 'Happos Runner', 'Natoon RunnerV2', 'Inkmagination_Xmas', 'Main Menu')
    and scene_name NOT LIKE '%Playability%'
  UNION ALL 
  select scene_name AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.micro_game_triggered`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
    and scene_name='Minigame - Kinderini - Drawing'
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
),
tbl_toy_friendship AS (
  select 'Toy FS' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.toy_friendship_started`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
),
tbl_avatar AS (
  select 'Avatar House' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.visit_screen`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and screen_to LIKE 'Eduland%Avatar%'
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
  UNION all
  select DISTINCT 'Avatar House' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.avatar_house_triggered`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
),
parental_section AS (
  select DISTINCT 'Parental' AS feature, user_id
  from `gcp-bi-elephant-db-gold.applaydu.parental_section`
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and version>='4.3.0'
    and date(client_time)>='2024-02-05'
)
,result as
(
select 'Dedicated Experience' AS feature, (select COUNT(DISTINCT user_id) from dedicate) AS Users, (select COUNT(DISTINCT user_id) from dedicate) / (select COUNT(DISTINCT user_id) from launch_resume) AS `% User Access the Feature`
  UNION all
  select 'AR' AS feature, (select COUNT(DISTINCT user_id) from ar_mode) AS Users, (select COUNT(DISTINCT user_id) from ar_mode) / (select COUNT(DISTINCT user_id) from launch_resume) AS `% User Access the Feature`
  UNION all
  select 'Minigame' AS feature, (select COUNT(DISTINCT user_id) from minigame) AS Users, (select COUNT(DISTINCT user_id) from minigame) / (select COUNT(DISTINCT user_id) from launch_resume) AS `% User Access the Feature`
  UNION all
  select 'Toy Friendship' AS feature, (select COUNT(DISTINCT user_id) from tbl_toy_friendship) AS Users, (select COUNT(DISTINCT user_id) from tbl_toy_friendship) / (select COUNT(DISTINCT user_id) from launch_resume) AS `% User Access the Feature`
  UNION all
  select 'Avatar House' AS feature, (select COUNT(DISTINCT user_id) from tbl_avatar) AS Users, (select COUNT(DISTINCT user_id) from tbl_avatar) / (select COUNT(DISTINCT user_id) from launch_resume) AS `% User Access the Feature`
  UNION all
  select 'Parental' AS feature, (select COUNT(DISTINCT user_id) from parental_section) AS Users, (select COUNT(DISTINCT user_id) from parental_section) / (select COUNT(DISTINCT user_id) from launch_resume) AS `% User Access the Feature`
)
--main query

select 14 as dashboard_id
            ,3180 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'% Users Access the Feature' as kpi_name
            ,CAST(feature as STRING) as value1_str,`Users` as value2,`% User Access the Feature` as value3
        from
        (
        
select * from result
)