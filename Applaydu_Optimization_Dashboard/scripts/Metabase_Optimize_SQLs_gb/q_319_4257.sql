DECLARE r319_q4257 ARRAY<STRUCT<`Month` INT64,`Year` INT64,`Time` STRING,`Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4257
);

IF row_count = 0 THEN
  SET r319_q4257 = (
    SELECT ARRAY(
      with gb4257 as (SELECT 0)
--main query
SELECT AS STRUCT 
    EXTRACT(MONTH FROM client_time) AS Month,
    EXTRACT(YEAR FROM client_time) AS Year,
    CONCAT(EXTRACT(YEAR FROM client_time), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS Time,
    COUNT(DISTINCT user_id) AS users
FROM `gcp-bi-elephant-db-gold.applaydu.custom_install_referral`
JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 [[AND {{iinstallsource}}]]
) USING (user_id)
WHERE utm_campaign LIKE '%CLTS%'
    AND version >= '5.0.0'
    AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v5_lets_story_start_date')
    AND DATE(client_time) < CURRENT_DATE()
    AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
GROUP BY Month, Year, Time
ORDER BY Month, Year ASC
    )
  );
  
ELSE
  SET r319_q4257 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as `Month`, CAST(value2 as INT64) as `Year`, CAST(value3_str as STRING) as `Time`, CAST(value4 as INT64) as `Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4257 
  );
END IF;

SELECT * FROM UNNEST(r319_q4257);
