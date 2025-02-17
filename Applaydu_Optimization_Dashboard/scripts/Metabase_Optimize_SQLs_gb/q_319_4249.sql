DECLARE r319_q4249 ARRAY<STRUCT<`Month` INT64,`Year` INT64,`Time` STRING,`Monthly Active Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4249
);

IF row_count = 0 THEN
  SET r319_q4249 = (
    SELECT ARRAY(
      with gb4249 as (SELECT 0)
--main query
SELECT AS STRUCT 
    EXTRACT(MONTH FROM client_time) AS `Month`,
    EXTRACT(YEAR FROM client_time) AS `Year`,
    CONCAT(EXTRACT(YEAR FROM client_time), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
    COUNT(DISTINCT user_id) AS `Monthly Active Users`
FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 
    AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
) USING (user_id)
JOIN `applaydu.tbl_shop_filter` using (game_id ,country) 
WHERE 1=1
    AND NOT (t.game_id = 82471 AND client_time < '2020-12-14')
    AND (client_time >= '2020-08-10' AND client_time < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)))
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND client_time >= TIMESTAMP((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]))
    AND client_time < TIMESTAMP(DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY))
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])
    [[AND {{ishopfilter}}]]
GROUP BY all
ORDER BY `Year` ASC, `Month` ASC
    )
  );
  
ELSE
  SET r319_q4249 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as `Month`, CAST(value2 as INT64) as `Year`, CAST(value3_str as STRING) as `Time`, CAST(value4 as INT64) as `Monthly Active Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4249 
  );
END IF;

SELECT * FROM UNNEST(r319_q4249);
