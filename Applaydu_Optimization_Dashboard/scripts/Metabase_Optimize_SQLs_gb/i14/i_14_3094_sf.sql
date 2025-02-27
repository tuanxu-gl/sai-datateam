with tbl_STORY_MODE_FINISHED as (
    SELECT user_id, fed_id, platform, game_id, event_id, MIN(client_time), MIN(server_time), version, country, session_id,
           MIN(token), avatar_gender, end_cause, toy_name, story_step, AVG(time_to_finish),
           activity_01, activity_01_value, activity_02, activity_02_value, activity_03, activity_03_value, activity_04,
           activity_04_value, activity_05, activity_05_value, avatar_onesie, click_from, environment_id, MIN(event_client_time_local),
           AVG(realtime_spent), MIN(load_time), activity_06, activity_06_value, activity_07, activity_07_value,
           activity_08, activity_08_value, activity_09, activity_09_value, activity_10, activity_10_value, toy_unlocked_method, from_scene
    FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
    WHERE 1=1 and version >= '5.0.0' AND version < '5.2.0'
      AND (environment_id = 'Experience - Dino Museum' AND version >= '4.7.0')
    GROUP BY ALL
),

REAL_STORY_MODE_FINISHED as (
    SELECT user_id, game_id, event_id, version, country, session_id, avatar_gender, end_cause, toy_name, story_step, realtime_spent, environment_id, client_time, server_time, toy_unlocked_method, COUNT(*) as dup
    FROM (
        SELECT * FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
        WHERE 1=1 and (
            environment_id LIKE 'Natoons v4%' OR
            (environment_id LIKE '%Travel%' AND (end_cause <> 'Finished' OR (end_cause = 'Finished' AND story_step = 'Ending'))) OR
            (environment_id IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land') AND (end_cause <> 'Finished' OR (end_cause = 'Finished' AND story_step = 'Ending'))) OR
            (environment_id NOT IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') AND (environment_id NOT LIKE '%Travel%')) OR
            (environment_id = 'Kinderini' AND server_time >= (SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey = 'apd_kinderini_start_date')) OR
            (environment_id = 'Eduland Lets Story' AND server_time >= (SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date'))
        )
        UNION ALL
        SELECT * FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
        WHERE 1=1 and (version < '5.0.0' OR version >= '5.2.0')
          AND (environment_id = 'Experience - Dino Museum' AND version >= '4.7.0')
        UNION ALL
        SELECT * FROM tbl_STORY_MODE_FINISHED
    )
    GROUP BY ALL
),

dedicated as (
    SELECT user_id, realtime_spent
    FROM REAL_STORY_MODE_FINISHED
    WHERE version >= '4.0.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND realtime_spent >= 0

    UNION ALL
    SELECT user_id, realtime_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.illustration_book_finished`
    WHERE 1=1
      AND version >= '4.0.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND realtime_spent >= 0
),

minigame_done as (
    SELECT user_id, game_id, client_time, server_time, version, country, scene_name, click_from,
           CASE WHEN realtime_spent IS NULL THEN time_to_finish ELSE realtime_spent END AS realtime_spent,
           load_time, CASE WHEN from_scene IS NULL THEN 'Not yet available' ELSE from_scene END AS from_scene
    FROM `gcp-bi-elephant-db-gold.applaydu.minigame_finished`
	where 1=1
),

minigame as (
    SELECT user_id, realtime_spent
    FROM minigame_done
    WHERE version >= '4.0.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND scene_name NOT IN ('Main Menu', 'NBA_1', 'NBA_2', 'Happos Runner', 'Inkmagination_Xmas')
      AND from_scene <> 'Eduland AvatarHouse'
      AND scene_name NOT LIKE '%Playability%'
      AND ((scene_name <> 'Move Ahead' AND realtime_spent >= 0) OR (scene_name = 'Move Ahead' AND CAST(realtime_spent AS INT) > 12))
),

toy_fs as (
    SELECT user_id, time_spent, CAST(client_time AS STRING) AS tfs_session
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_friendship_finished`
    WHERE 1=1
      AND version >= '4.0.0' AND version < '4.6.1' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND scene_name LIKE 'Eduland%'
      AND time_spent >= 0 AND time_spent < 7200

    UNION ALL
    SELECT user_id, time_spent, CAST(client_time AS STRING) AS tfs_session
    FROM `gcp-bi-elephant-db-gold.applaydu.activity_finished`
    WHERE 1=1
      AND version >= '4.6.1' AND version < '5.2.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND feature = 'Toy Friendship'
      AND activity_01 = 'TFS Current Heart Point'
      AND time_spent >= 0 AND time_spent < 7200

    UNION ALL
    SELECT user_id, SUM(time_spent) AS total_time, CAST(client_time AS STRING) AS tfs_session
    FROM `gcp-bi-elephant-db-gold.applaydu.activity_finished`
    WHERE 1=1
      AND version >= '5.2.0' AND version < '5.4.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND feature = 'Toy Friendship'
      AND activity_01 = 'TFS Minigame Index Check'
      AND time_spent >= 0 AND time_spent < 7200
    GROUP BY ALL

    UNION ALL
    SELECT user_id, SUM(time_spent) AS total_time, CAST(activity_10_value AS STRING) AS tfs_session
    FROM `gcp-bi-elephant-db-gold.applaydu.activity_finished`
    WHERE 1=1
      AND version >= '5.4.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND feature = 'Toy Friendship'
      AND activity_01 = 'TFS Minigame Index Check'
      AND time_spent >= 0 AND time_spent < 7200
    GROUP BY 1, 3
),

ar_mode as (
    SELECT user_id, realtime_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.ar_mode_finished`
    WHERE 1=1
      AND version >= '4.0.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND realtime_spent >= 0

    UNION ALL
    SELECT user_id, realtime_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.face_mask_finished`
    WHERE 1=1
      AND version >= '4.0.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND realtime_spent >= 0
),

avatar_house as (
    SELECT user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.visit_screen`
    WHERE 1=1
      AND screen_to LIKE 'Eduland%Avatar%'
      AND version >= '4.3.0' AND version < '4.5.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])

    UNION ALL
    SELECT user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.avatar_house_finished`
    WHERE 1=1
      AND time_spent >= 0 AND from_scene <> 'Inkmagination'
      AND version >= '4.5.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),

avatarhouse_time as (
    SELECT time_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.visit_screen`
    WHERE 1=1
      AND time_spent >= 0 AND time_spent < 36000
      AND (screen_from LIKE '%Avatar%' OR (screen_from LIKE '%Ink%' AND screen_to LIKE '%Avatar%'))
      AND version >= '4.3.0' AND version < '4.5.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])

    UNION ALL
    SELECT time_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.avatar_house_finished`
    WHERE 1=1
      AND time_spent >= 0
      AND version >= '4.5.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])

    UNION ALL
    SELECT realtime_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.minigame_finished`
    WHERE 1=1 and scene_name = 'Inkmagination'
      AND from_scene = 'Eduland AvatarHouse'
      AND version >= '4.5.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
),

parental_section as (
    SELECT user_id, realtime_spent
    FROM `gcp-bi-elephant-db-gold.applaydu.parental_section`
    WHERE 1=1
      AND version >= '4.0.0' AND version < '9.0.0' AND server_time < CURRENT_DATE()
      AND version >= (SELECT MIN(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) AND version <= (SELECT MAX(version) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
      AND version IN (SELECT version FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
      AND server_time >= (SELECT MIN(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
      AND country IN (SELECT country FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
      AND realtime_spent >= 0
)

--main query
SELECT * FROM (
        SELECT 'Dedicated Experience' AS feature, COUNT(*) AS session, session / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) AS `Sessions per user`,
               SUM(realtime_spent) / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) / 60 AS `Time spent per user`,
               SUM(realtime_spent) / session / 60 AS `Session Duration`,
               CONCAT(FLOOR(`Time spent per user`), ' min ', ROUND((`Time spent per user` - FLOOR(`Time spent per user`)) * 60), ' sec') AS `Time spent per user (min - sec)`,
               CONCAT(FLOOR(`Session Duration`), ' min ', ROUND((`Session Duration` - FLOOR(`Session Duration`)) * 60), ' sec') AS `Session Duration (min - sec)`
        FROM dedicated

        UNION all
        SELECT 'AR' AS feature, COUNT(*) AS session, session / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) AS `Sessions per user`,
               SUM(realtime_spent) / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) / 60 AS `Time spent per user`,
               SUM(realtime_spent) / session / 60 AS `Session Duration`,
               CONCAT(FLOOR(`Time spent per user`), ' min ', ROUND((`Time spent per user` - FLOOR(`Time spent per user`)) * 60), ' sec') AS `Time spent per user (min - sec)`,
               CONCAT(FLOOR(`Session Duration`), ' min ', ROUND((`Session Duration` - FLOOR(`Session Duration`)) * 60), ' sec') AS `Session Duration (min - sec)`
        FROM ar_mode

        UNION all
        SELECT 'Minigame' AS feature, COUNT(*) AS session, session / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) AS `Sessions per user`,
               SUM(realtime_spent) / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) / 60 AS `Time spent per user`,
               SUM(realtime_spent) / session / 60 AS `Session Duration`,
               CONCAT(FLOOR(`Time spent per user`), ' min ', ROUND((`Time spent per user` - FLOOR(`Time spent per user`)) * 60), ' sec') AS `Time spent per user (min - sec)`,
               CONCAT(FLOOR(`Session Duration`), ' min ', ROUND((`Session Duration` - FLOOR(`Session Duration`)) * 60), ' sec') AS `Session Duration (min - sec)`
        FROM minigame

        UNION all
        SELECT 'Toy Friendship' AS feature, COUNT(*) AS session, session / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) AS `Sessions per user`,
               SUM(time_spent) / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) / 60 AS `Time spent per user`,
               SUM(time_spent) / session / 60 AS `Session Duration`,
               CONCAT(FLOOR(`Time spent per user`), ' min ', ROUND((`Time spent per user` - FLOOR(`Time spent per user`)) * 60), ' sec') AS `Time spent per user (min - sec)`,
               CONCAT(FLOOR(`Session Duration`), ' min ', ROUND((`Session Duration` - FLOOR(`Session Duration`)) * 60), ' sec') AS `Session Duration (min - sec)`
        FROM toy_fs

        UNION all
        SELECT 'Avatar House' AS feature, COUNT(*) AS session, session / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) AS `Sessions per user`,
               (SELECT SUM(time_spent) FROM avatarhouse_time) / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) / 60 AS `Time spent per user`,
               (SELECT SUM(time_spent) FROM avatarhouse_time) / session / 60 AS `Session Duration`,
               CONCAT(FLOOR(`Time spent per user`), ' min ', ROUND((`Time spent per user` - FLOOR(`Time spent per user`)) * 60), ' sec') AS `Time spent per user (min - sec)`,
               CONCAT(FLOOR(`Session Duration`), ' min ', ROUND((`Session Duration` - FLOOR(`Session Duration`)) * 60), ' sec') AS `Session Duration (min - sec)`
        FROM avatar_house

        UNION all
        SELECT 'Parental Section' AS feature, COUNT(*) AS session, session / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) AS `Sessions per user`,
               SUM(realtime_spent) / (CASE WHEN COUNT(DISTINCT user_id) = 0 THEN NULL ELSE COUNT(DISTINCT user_id) END) / 60 AS `Time spent per user`,
               SUM(realtime_spent) / session / 60 AS `Session Duration`,
               CONCAT(FLOOR(`Time spent per user`), ' min ', ROUND((`Time spent per user` - FLOOR(`Time spent per user`)) * 60), ' sec') AS `Time spent per user (min - sec)`,
               CONCAT(FLOOR(`Session Duration`), ' min ', ROUND((`Session Duration` - FLOOR(`Session Duration`)) * 60), ' sec') AS `Session Duration (min - sec)`
        FROM parental_section
        )
   