DECLARE r319_q4270 ARRAY<STRUCT<`Users` STRING,`Users each step` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4270
);

IF row_count = 0 THEN
  SET r319_q4270 = (
    SELECT ARRAY(
      WITH gb4270 as (select 0)
,tbl_ua AS (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity`
    WHERE 1=1 [[AND {{iinstallsource}}]]
), 
USER_LAUNCH AS (
    SELECT DISTINCT user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` 
    JOIN tbl_ua USING (user_id)
    WHERE launch_type = 'first_launch'
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND game_id IN (81335, 81337, 85837)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
)
, 
UNIQUE_DA_USER AS (
    SELECT DISTINCT ul.user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.disclaimer_acceptance` AS fe  
    RIGHT JOIN user_launch ul ON fe.user_id = ul.user_id
    WHERE version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND game_id IN (81335, 81337, 85837)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
), 
SCAN_USER AS (
    SELECT DISTINCT udu.user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe 
    RIGHT JOIN unique_da_user AS udu ON fe.user_id = udu.user_id
    WHERE ftue_stage = 'Finish'
        AND ftue_steps = 'Choose Scan Or Unlock' AND user_selection = 'No-Deeplink and Scan'
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
), 
TIME_CONTROL AS (
    SELECT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.time_control_access`
    INNER JOIN unique_da_user USING (user_id)
    WHERE user_status = 'FTUE'
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
), 
ALL_DATA AS (
    -- FTUE flow start
    (
    SELECT 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ftue_steps
            ,'Disclaimer_Acceptance','Parental Agreement ' || ftue_stage)
            ,'Choose Avatar Gender', 'Gender + Difficulty page ' || ftue_stage)
            ,'Avatar_Creation', 'Avatar Creation ' || ftue_stage)
            ,'Enter Avatar Name', 'Avatar Name ' || ftue_stage)
            ,'Avatar_Creation', 'Character Creation ' || ftue_stage)
            ,'Email Registration', 'Email Registration ' || ftue_stage)
            ,'Age Confirmation after Email', 'Age Confirmation after Email ' || ftue_stage)
            ,'Choose Scan Or Unlock', 'Choose No Toy Or Scan ' || ftue_stage)
         AS Users,
        COUNT(DISTINCT udu.user_id) AS `Users each step`
    FROM `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe 
    RIGHT JOIN unique_da_user AS udu ON fe.user_id = udu.user_id
    WHERE ftue_stage = 'Start'
        AND ftue_steps IN ('Choose Avatar Gender', 'Avatar_Creation','Enter Avatar Name', 'Email Registration', 'Choose Scan Or Unlock')
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
    GROUP BY ftue_steps, ftue_stage
    ORDER BY `Users each step` DESC
    )
    UNION ALL 
    (
    -- AR FTUE start
    SELECT 
        REPLACE(REPLACE(ftue_steps
            ,'Camera_Permission', 'Camera Permission ' || ftue_stage)
            ,'Scan Section', 'Scan Section '|| ftue_stage)
         AS Users,
        COUNT(DISTINCT su.user_id) AS `Users each step`
    FROM `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe 
    RIGHT JOIN scan_user AS su ON fe.user_id = su.user_id
    WHERE ftue_stage = 'Start'
        AND ftue_steps IN ('Camera_Permission', 'Scan Section')
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
        [[AND game_id = {{ggi}}]]
    GROUP BY ftue_steps, ftue_stage
    ORDER BY `Users each step` DESC
    )
    UNION ALL
    -- Scan finish
    (
    SELECT 
        REPLACE(REPLACE(REPLACE(user_selection
            ,'No-Deeplink and Scan', 'Choose to Scan')
            ,'No-Deeplink and Worldmap', 'Choose to Unlock Free Toy')
            ,'Deeplink', 'Deeplink')
         AS Users,
        COUNT(DISTINCT udu.user_id) AS `Users each step`
    FROM `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe 
    RIGHT JOIN unique_da_user AS udu ON fe.user_id = udu.user_id
    WHERE ftue_stage = 'Finish'
        AND ftue_steps IN ('Choose Scan Or Unlock')
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
    GROUP BY Users
    ORDER BY `Users each step` DESC
    )
    UNION ALL 
    -- AR FTUE Finish
    (
    SELECT 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ftue_steps
            ,'Camera_Permission', 'Camera Permission ' || ftue_stage)
            ,'Scan Section', 'Scan Section '|| ftue_stage)
            ,'Scan toy result', 'Scan toy result '|| ftue_stage)
            ,'Unlock toy screen', 'Unlock toy screen '|| ftue_stage)
            ,'AR_Mode', 'Simple Toy AR '|| ftue_stage)
         AS Users,
        COUNT(DISTINCT su.user_id) AS `Users each step`
    FROM `gcp-bi-elephant-db-gold.applaydu.ftue_event` AS fe  
    RIGHT JOIN scan_user AS su ON fe.user_id = su.user_id
    WHERE ftue_stage = 'Finish'
        AND (ftue_steps IN ('Camera_Permission')
            OR ftue_steps IN ('Scan Section', 'Scan toy result','Unlock toy screen', 'AR_Mode') AND user_selection LIKE 'No-Deeplink and Scan:%')
        AND version >= '5.0.0' AND version < '9.0.0'
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        [[AND game_id = {{ggi}}]]
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{inotcountry}}]] [[AND {{inotregion}}]])
    GROUP BY ftue_steps, ftue_stage
    ORDER BY `Users each step` DESC
    )
    UNION ALL
    SELECT 'New Users Launch' AS Users, COUNT(DISTINCT user_id) AS `Users each step`
    FROM user_launch
)
--main query
SELECT AS STRUCT `Users` ,`Users each step`
FROM all_data
WHERE Users IN ('New Users Launch','Choose to Scan', 'Unlock toy screen Finish') OR (Users LIKE '%Start' AND Users NOT IN ('Choose No Toy Or Scan Start'))
UNION all
(
SELECT 'Time Control' AS Users, COUNT(DISTINCT user_id) AS `Users each step`
FROM time_control
)
UNION all
(
SELECT 'Parental Agreement Start' AS Users, COUNT(DISTINCT user_id) AS `Users each step`
FROM unique_da_user
)
ORDER BY 
    CASE 
        WHEN Users = 'New Users Launch' THEN 0
        WHEN Users = 'Parental Agreement Start' THEN 1
        WHEN Users = 'Gender + Difficulty page Start' THEN 2
        WHEN Users = 'Gender + Difficulty page Finish' THEN 3
        WHEN Users = 'Time Control' THEN 4
        WHEN Users = 'Avatar Creation Start' THEN 5
        WHEN Users = 'Avatar Creation Finish' THEN 6
        WHEN Users = 'Avatar Name Start' THEN 7
        WHEN Users = 'Avatar Name Finish' THEN 8
        WHEN Users = 'Email Registration Start' THEN 9
        WHEN Users = 'Email Registration Finish' THEN 10
        WHEN Users = 'Age Confirmation after Email Start' THEN 11
        WHEN Users = 'Age Confirmation after Email Finish' THEN 12    
        WHEN Users = 'Choose No Toy Or Scan Start' THEN 13
        WHEN Users = 'Choose No Toy Or Scan Finish' THEN 14
        WHEN Users = 'Choose to Scan' THEN 15
        WHEN Users = 'Choose to Unlock Free Toy' THEN 16
        WHEN Users = 'Camera Permission Start' THEN 17
        WHEN Users = 'Camera Permission Finish' THEN 18
        WHEN Users = 'Camera Access explanation screen Start' THEN 19
        WHEN Users = 'Camera Access explanation screen Finish' THEN 20
        WHEN Users = 'Scan Section Start' THEN 21
        WHEN Users = 'Scan Section Finish' THEN 22
        WHEN Users = 'Scan toy result Start' THEN 23
        WHEN Users = 'Scan toy result Finish' THEN 24
        WHEN Users = 'Unlock toy screen Start' THEN 25
        WHEN Users = 'Unlock toy screen Finish' THEN 26
        WHEN Users = 'Simple Toy AR Start' THEN 27
        WHEN Users = 'Simple Toy AR Finish' THEN 28 
        WHEN Users = 'World Map final Start' THEN 29
        WHEN Users = 'World Map final Finish' THEN 30 
        WHEN Users = 'Users Scanned successfully toy 1st time' THEN 31
        WHEN Users = 'Users Scanned successfully toy 2nd time' THEN 32 
        WHEN Users = 'Users Scanned successfully toy 3rd time' THEN 33 
    END
    )
  );
  
ELSE
  SET r319_q4270 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Users`, CAST(value2 as INT64) as `Users each step`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4270 
  );
END IF;

SELECT * FROM UNNEST(r319_q4270);
