DECLARE r14_q3180 ARRAY<STRUCT<feature STRING,`Users` INT64,`% User Access the Feature` FLOAT64>>;
  DECLARE row_count FLOAT64;
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]);

  
  SET row_count = (
    SELECT COUNT(0) 
    FROM `applaydu.apd_report_14`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 14 
      AND query_id = 3180
  );
  
  IF row_count = 0 THEN
    SET r14_q3180 = (
      SELECT ARRAY(
        --main query
WITH launch_resume AS (
   SELECT 'Active' AS feature, user_id
FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume`
WHERE 1=1
    AND version >= '4.0.0'
    AND DATE(client_time) >= ('2023-08-22')
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version >= (ifrom_version) 
    AND version <= (ito_version)
    AND version IN UNNEST(iversions)
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND country IN UNNEST(icountry_region)
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
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
    UNION all
    SELECT DISTINCT 'Dedicated Exp' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
    UNION all
    SELECT 'Dedicated Exp' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.illustration_book_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
),
ar_mode AS (
    SELECT 'AR' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.ar_mode_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
    UNION all
    SELECT DISTINCT 'AR' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.face_mask_triggered`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
),
minigame AS (
    SELECT 'Minigame' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.minigame_started`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
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
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
),
tbl_toy_friendship AS (
    SELECT 'Toy FS' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_friendship_started`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
),
tbl_avatar AS (
    SELECT 'Avatar House' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.visit_screen`
    WHERE 1=1
        AND screen_to LIKE 'Eduland%Avatar%'
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
    UNION all
    SELECT DISTINCT 'Avatar House' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.avatar_house_triggered`
    WHERE 1=1 
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
),
parental_section AS (
    SELECT DISTINCT 'Parental' AS feature, user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.parental_section`
    WHERE 1=1
        AND version >= '4.3.0'
        AND DATE(client_time) >= '2024-02-05'
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version >= (ifrom_version) 
        AND version <= (ito_version)
        AND version IN UNNEST(iversions)
        AND DATE(client_time) >= istart_date
        AND DATE(client_time) < iend_date
        AND country IN UNNEST(icountry_region)
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
SELECT AS STRUCT * from result
      )
    );
    
  ELSE
    SET r14_q3180 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as feature, CAST(value2 as INT64) as `Users`, CAST(value3 as FLOAT64) as `% User Access the Feature`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 3180 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q3180) order by `% User Access the Feature` desc;
  