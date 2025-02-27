DECLARE r319_q4262 ARRAY<STRUCT<`avg_time_story_exp` FLOAT64,`Average time full story experience` STRING>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4262
);

IF row_count = 0 THEN
  SET r319_q4262 = (
    SELECT ARRAY(
      WITH gb4262 as (SELECT 0)
,scan_kdr_users AS (
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
            OR scan_type IN ('Scan_QR_Biscuit', 'Scan_Toy_Biscuit')
        )
)
--main query
SELECT AS STRUCT 
    SUM(realtime_spent) / COUNT(DISTINCT user_id) AS avg_time_story_exp,
    CONCAT(
        FORMAT_TIMESTAMP('%M', TIMESTAMP_SECONDS(CAST(SUM(realtime_spent) / COUNT(DISTINCT user_id) AS INT))), ' min ',
        FORMAT_TIMESTAMP('%S', TIMESTAMP_SECONDS(CAST(SUM(realtime_spent) / COUNT(DISTINCT user_id) AS INT))), ' sec'
    ) AS `Average time full story experience`
FROM `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 [[AND {{iinstallsource}}]]
) USING (user_id)
WHERE environment_id = 'Kinderini'
    AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_kinderini_start_date')
    AND DATE(client_time) < CURRENT_DATE()
    AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND user_id IN (SELECT DISTINCT user_id FROM scan_kdr_users)
    )
  );
  
ELSE
  SET r319_q4262 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as FLOAT64) as `avg_time_story_exp`, CAST(value2_str as STRING) as `Average time full story experience`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4262 
  );
END IF;

SELECT * FROM UNNEST(r319_q4262);
