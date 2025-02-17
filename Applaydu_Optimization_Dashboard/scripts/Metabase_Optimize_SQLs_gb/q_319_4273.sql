DECLARE r319_q4273 ARRAY<STRUCT<`Language` STRING,`Stories` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4273
);

IF row_count = 0 THEN
  SET r319_q4273 = (
    SELECT ARRAY(
      WITH gb4273 as (SELECT 0)
,tbl_ls_scan_users AS (
    SELECT DISTINCT user_id
    FROM (
        SELECT user_id
        FROM `gcp-bi-elephant-db-gold.applaydu.custom_install_referral`
        JOIN (
            SELECT DISTINCT user_id 
            FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
            WHERE 1=1 [[AND {{iinstallsource}}]]
        ) USING (user_id)
        WHERE utm_campaign LIKE '%KCLTS%'
            AND version >= '5.0.0'
            AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')
            AND DATE(client_time) < CURRENT_DATE()
            AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
            AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
            AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        UNION ALL 
        SELECT user_id
        FROM `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
        JOIN (
            SELECT DISTINCT user_id 
            FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
            WHERE 1=1 [[AND {{iinstallsource}}]]
        ) USING (user_id)
        WHERE (
            (reference LIKE '%KCLTS%' AND NOT (game_id != 81335 AND scan_type = 'Deep Link')) 
            OR scan_type = 'Scan_QR_LS'
        )
        AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND scan_result IN ('New_Toy', 'Old_Toy')
    )
),
tbl_ls_user_activity AS (
    SELECT 
        t.*,
        d1.name AS language,
        d2.name AS location,
        d3.name AS hero,
        d4.name AS sidekick,
        d5.name AS plot,
        d6.name AS theme
    FROM `gcp-bi-elephant-db-gold.applaydu.activity_finished` t
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.element` d1 ON d1.id = activity_01_value 
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.element` d2 ON d2.id = activity_02_value 
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.element` d3 ON d3.id = activity_03_value 
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.element` d4 ON d4.id = activity_04_value 
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.element` d5 ON d5.id = activity_05_value 
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.element` d6 ON d6.id = activity_06_value 
    WHERE activity_01 = 'Experience - Lets Story - New Story Created'
        AND version >= '5.0.0'
        AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')
        AND DATE(client_time) < CURRENT_DATE()
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND user_id IN (SELECT DISTINCT user_id FROM tbl_ls_scan_users)
)
--main query
SELECT AS STRUCT n2.name AS `Language`, COUNT(0) AS `Stories`
FROM tbl_ls_user_activity
LEFT JOIN UNNEST([
    STRUCT('en' AS id, 'English' AS name),
    STRUCT('es' AS id, 'Spanish' AS name),
    STRUCT('de' AS id, 'German' AS name),
    STRUCT('it' AS id, 'Italian' AS name),
    STRUCT('pt' AS id, 'Portuguese' AS name),
    STRUCT('fr' AS id, 'French' AS name),
    STRUCT('pl' AS id, 'Polish' AS name),
    STRUCT('ko' AS id, 'Korean' AS name),
    STRUCT('hu' AS id, 'Hungarian' AS name),
    STRUCT('nl' AS id, 'Dutch' AS name),
    STRUCT('zh-Hant' AS id, 'Traditional Chinese' AS name),
    STRUCT('ar' AS id, 'Arabic' AS name),
    STRUCT('zh-Hans' AS id, 'Simplified Chinese' AS name)
]) AS n2 ON n2.id = tbl_ls_user_activity.language
GROUP BY `Language`
ORDER BY `Stories` DESC
    )
  );
  
ELSE
  SET r319_q4273 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Language`, CAST(value2 as INT64) as `Stories`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4273 
  );
END IF;

SELECT * FROM UNNEST(r319_q4273);
