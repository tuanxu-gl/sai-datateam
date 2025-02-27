--main query
WITH launch_resume AS (
   SELECT 'Active' AS feature, user_id
FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume`
WHERE 1=1
    AND version >= '4.0.0'
    AND DATE(client_time) >= (SELECT CAST(ivalue AS DATE) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v4_start_date')
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
    AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
),
dedicate AS (
    SELECT 'Dedicated Exp' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    UNION all
    SELECT DISTINCT 'Dedicated Exp' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    UNION all
    SELECT 'Dedicated Exp' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.illustration_book_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),
ar_mode AS (
    SELECT 'AR' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.ar_mode_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    UNION all
    SELECT DISTINCT 'AR' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.face_mask_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),
minigame AS (
    SELECT 'Minigame' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.minigame_started`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND scene_name NOT IN ('NBA_1', 'NBA_2', 'Happos Runner', 'Natoon RunnerV2', 'Inkmagination_Xmas', 'Main Menu')
        AND scene_name NOT LIKE '%Playability%'
    UNION ALL 
    SELECT scene_name AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.micro_game_triggered`
    WHERE 1=1 
        AND scene_name = 'Minigame - Kinderini - Drawing'
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),
tbl_toy_friendship AS (
    SELECT 'Toy FS' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_friendship_started`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),
tbl_avatar AS (
    SELECT 'Avatar House' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.visit_screen`
    WHERE 1=1
        AND screen_to LIKE 'Eduland%Avatar%'
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    UNION all
    SELECT DISTINCT 'Avatar House' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.avatar_house_triggered`
    WHERE 1=1 
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),
parental_section AS (
    SELECT DISTINCT 'Parental' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.parental_section`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
        AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(server_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(server_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
)
,result as
(
SELECT 'Dedicated Experience' AS feature, (SELECT COUNT(DISTINCT user_id) FROM dedicate) AS Users, (SELECT COUNT(DISTINCT user_id) FROM dedicate) / (SELECT COUNT(DISTINCT user_id) FROM launch_resume) AS `% User Access the Feature`
    UNION all
    SELECT 'AR' AS feature, (SELECT COUNT(DISTINCT user_id) FROM ar_mode) AS Users, (SELECT COUNT(DISTINCT user_id) FROM ar_mode) / (SELECT COUNT(DISTINCT user_id) FROM launch_resume) AS `% User Access the Feature`
    UNION all
    SELECT 'Minigame' AS feature, (SELECT COUNT(DISTINCT user_id) FROM minigame) AS Users, (SELECT COUNT(DISTINCT user_id) FROM minigame) / (SELECT COUNT(DISTINCT user_id) FROM launch_resume) AS `% User Access the Feature`
    UNION all
    SELECT 'Toy Friendship' AS feature, (SELECT COUNT(DISTINCT user_id) FROM tbl_toy_friendship) AS Users, (SELECT COUNT(DISTINCT user_id) FROM tbl_toy_friendship) / (SELECT COUNT(DISTINCT user_id) FROM launch_resume) AS `% User Access the Feature`
    UNION all
    SELECT 'Avatar House' AS feature, (SELECT COUNT(DISTINCT user_id) FROM tbl_avatar) AS Users,  (SELECT COUNT(DISTINCT user_id) FROM tbl_avatar) / (SELECT COUNT(DISTINCT user_id) FROM launch_resume) AS `% User Access the Feature`
    UNION all
    SELECT 'Parental' AS feature, (SELECT COUNT(DISTINCT user_id) FROM parental_section) AS Users, (SELECT COUNT(DISTINCT user_id) FROM parental_section) / (SELECT COUNT(DISTINCT user_id) FROM launch_resume) AS `% User Access the Feature`
)
--main query
select * from result