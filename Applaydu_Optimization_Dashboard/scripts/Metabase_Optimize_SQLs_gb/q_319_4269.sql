DECLARE r319_q4269 ARRAY<STRUCT<`#` STRING,`Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4269
);

IF row_count = 0 THEN
  SET r319_q4269 = (
    SELECT ARRAY(
      WITH gb4245 as (SELECT 0)
,kdr_scan_users AS (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 [[AND {{iinstallsource}}]]
    ) USING (user_id)
    WHERE DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_kinderini_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND scan_result IN ('New_Toy', 'Old_Toy')
        AND (
            (scan_type = 'Deep Link' AND UPPER(reference) LIKE '%KINDERINI%')
            OR scan_type IN ('Scan_Toy_Biscuit', 'Scan_QR_Biscuit')
        )
),
filter_sst AS (
    SELECT * 
    FROM `gcp-bi-elephant-db-gold.applaydu.story_step_finished`
    JOIN kdr_scan_users USING (user_id)
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 [[AND {{iinstallsource}}]]
    ) USING (user_id)
    WHERE DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_kinderini_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND environment_id = 'Kinderini'
),
tap_emotion_user AS (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_triggered`
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 [[AND {{iinstallsource}}]]
    ) USING (user_id)
    WHERE DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_kinderini_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND environment_id = 'Kinderini'
        AND click_from IN (
            'Eduland Kinderini Menu - Cluster - Wonder',
            'Eduland Kinderini Menu - Cluster - Happiness',
            'Eduland Kinderini Menu - Cluster - Kindness',
            'Eduland Kinderini Menu - Cluster - Fearfulness'
        )
        AND user_id IN (SELECT DISTINCT user_id FROM kdr_scan_users)
),
result AS (
    SELECT 
        'Kinderini Scan Users' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM kdr_scan_users
    UNION ALL 
    SELECT 
        'Tap Emotion Cluster' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM tap_emotion_user
    UNION ALL
    SELECT 
        'Drawing Start' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Drawing MIG' AND user_SELECTion = 'Started'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Drawing Stop' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Drawing MIG' AND user_SELECTion = 'Finished'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Finding Start' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Finding MIG' AND user_SELECTion = 'Started'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Finding Stop' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Finding MIG' AND user_SELECTion = 'Finished'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Catching Start' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Catching MIG' AND user_SELECTion = 'Started'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Catching Stop' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Catching MIG' AND user_SELECTion = 'Finished'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Diary Start' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Dairy Screen' AND user_SELECTion = 'Started'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
    UNION ALL
    SELECT 
        'Diary Stop' AS `#`,
        COUNT(DISTINCT user_id) AS `Users`
    FROM filter_sst
    WHERE story_step = 'Kinderini - Dairy Screen' AND user_SELECTion = 'Finished'
        AND user_id IN (SELECT DISTINCT user_id FROM tap_emotion_user)
)
--main query
SELECT AS STRUCT * FROM result
    )
  );
  
ELSE
  SET r319_q4269 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `#`, CAST(value2 as INT64) as `Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4269 
  );
END IF;

SELECT * FROM UNNEST(r319_q4269);
