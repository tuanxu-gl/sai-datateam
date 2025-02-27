DECLARE r319_q4275 ARRAY<STRUCT<`Total Scans` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4275
);

IF row_count = 0 THEN
  SET r319_q4275 = (
    SELECT ARRAY(
      WITH gb4275 as (SELECT 0)
,tbl_utm AS (
    SELECT 
        user_id,
        game_id,
        client_time,
        utm_campaign AS pack,
        'Deep Link' AS scan_type
    FROM `gcp-bi-elephant-db-gold.applaydu.custom_install_referral`
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 [[AND {{iinstallsource}}]]
    ) USING (user_id)
    WHERE utm_campaign LIKE '%CLTS%'
        AND game_id <> 81335
        AND version >= '5.0.0'
        AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
),
tbl_scan_mode AS (
    SELECT 
        user_id,
        game_id,
        client_time,
        RIGHT(reference, 7) AS pack,
        scan_type
    FROM `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 [[AND {{iinstallsource}}]]
    ) USING (user_id)
    WHERE reference LIKE '%CLTS%'
        AND NOT (game_id != 81335 AND scan_type = 'Deep Link')
        AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND scan_result IN ('New_Toy', 'Old_Toy')
)
--main query
SELECT AS STRUCT COUNT(0) AS `Total Scans`
FROM (
    SELECT * FROM tbl_utm 
    UNION ALL 
    SELECT * FROM tbl_scan_mode
)
    )
  );
  
ELSE
  SET r319_q4275 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as `Total Scans`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4275 
  );
END IF;

SELECT * FROM UNNEST(r319_q4275);
