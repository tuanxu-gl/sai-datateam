insert into APPLAYDU_NOT_CERTIFIED.apd_report_idashboard_id (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,value1) 

WITH q3852 AS (SELECT 3852),
tbl_users_launch_lets_story AS (
    SELECT user_id, client_time, version,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY client_time) AS rn
    FROM applaydu.visit_screen
    WHERE (screen_from IN ('World Map', 'Mini Game Screen') OR screen_from LIKE 'Eduland%Minigame Menu') 
        AND screen_to = 'Eduland Lets Story'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
), 
tbl_users_launch_lets_story_first_time AS (
    SELECT DISTINCT user_id
    FROM tbl_users_launch_lets_story
    WHERE rn = 1
), 
pick_character AS (
    SELECT DISTINCT user_id 
    FROM applaydu.activity_finished 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE scene_name = 'Eduland Lets Story'
        AND activity_05 = 'Experience - Lets Story - Initial Character Selection'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
),
age_selection AS (
    SELECT DISTINCT user_id 
    FROM applaydu.activity_finished 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE scene_name = 'Eduland Lets Story'
        AND activity_01 = 'Experience - Lets Story - Reading Level'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
),
story_creation_started AS (
    SELECT COUNT(DISTINCT user_id) AS users
    FROM applaydu.visit_screen 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE screen_to = 'Eduland Lets Story - Story Creation'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
),
story_creation_finished AS (
    SELECT COUNT(DISTINCT user_id) AS users
    FROM applaydu.activity_finished 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE activity_01 = 'Experience - Lets Story - New Story Created'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
),
tbl_illustration_book_started AS (
    SELECT COUNT(DISTINCT user_id) AS users
    FROM applaydu.visit_screen 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE screen_to = 'Eduland Lets Story - Story Reading'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
),
tbl_illustration_book_finished AS (
    SELECT COUNT(DISTINCT user_id) AS users
    FROM applaydu.illustration_book_finished 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE story_title LIKE 'Experience - Lets Story%'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
        AND (end_cause = 'finished' OR max_page_reached = total_page_available)
),
mig_after_read_started AS (
    SELECT COUNT(DISTINCT user_id) AS users
    FROM applaydu.visit_screen 
    JOIN tbl_users_launch_lets_story_first_time USING (user_id)
    WHERE screen_from = 'Eduland Lets Story - Story Reading'
        AND screen_to = 'Eduland Lets Story - Minigame'
        AND (version >= '5.0.0' AND client_time >= '2024-08-28')
        AND client_time < DATEADD(day, 1, 'iend_date')
)
select 292 as dashboard_id
    ,3852 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Funnel for FTUE enter Lets Story Eduland' as kpi_name
    ,"User Type" as dimension1
    ,users as value1
from
(
    SELECT 'Launch Lets Story' AS "User Type",  
    (SELECT COUNT(DISTINCT user_id) 
     FROM tbl_users_launch_lets_story
     WHERE client_time < DATEADD(day, 1, 'iend_date')
    ) AS users 
UNION 
SELECT 'Launch Lets Story First time' AS "User Type",  
    (SELECT COUNT(DISTINCT user_id) 
     FROM tbl_users_launch_lets_story_first_time
    ) AS users 
UNION 
SELECT 'Pick Free Character' AS "User Type",  
    (SELECT COUNT(DISTINCT user_id) 
     FROM pick_character
    ) AS users 
UNION 
SELECT 'Age Selection' AS "User Type",  
    (SELECT COUNT(DISTINCT user_id) 
     FROM age_selection
    ) AS users 
UNION 
SELECT 'Story Creation' AS "User Type",  
    (SELECT users 
     FROM story_creation_started
    ) AS users 
UNION 
SELECT 'Access Story Reading' AS "User Type",  
    (SELECT users 
     FROM tbl_illustration_book_started
    ) AS users
UNION 
SELECT 'Finish Story Reading' AS "User Type",  
    (SELECT users 
     FROM tbl_illustration_book_finished
    ) AS users
UNION 
SELECT 'Play Minigames' AS "User Type",  
    (SELECT users 
     FROM mig_after_read_started
    ) AS users 
ORDER BY users DESC
)