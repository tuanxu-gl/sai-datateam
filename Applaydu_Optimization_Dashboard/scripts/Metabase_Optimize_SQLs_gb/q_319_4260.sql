DECLARE r319_q4260 ARRAY<STRUCT<year INT64,month INT64,`Time` STRING,`Persona_Type` STRING,`Total Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4260
);

IF row_count = 0 THEN
  SET r319_q4260 = (
    SELECT ARRAY(
      WITH gb4260 as (SELECT 0)
,unlock AS (
    SELECT DISTINCT user_id, COUNT(*) AS `Number of Toys Unlocked`
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    WHERE (unlock_cause = 'QR Code'
        OR unlock_cause = 'Toy Scan' 
        OR unlock_cause = 'Deep_Link')
        AND isnewtoy = 1
        AND client_time >= CAST((SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey='persona_starting_date') AS TIMESTAMP)
        AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1  [[AND {{ishopfilter}}]])
    GROUP BY user_id
),
Persona AS (
    SELECT user_id, 
           CASE WHEN `Number of Toys Unlocked` IN (1,2,3) THEN 'Persona #2'
                ELSE 'Persona #3' END AS `Persona Type`
    FROM unlock
)
--main query
SELECT AS STRUCT EXTRACT(YEAR FROM client_time) AS year,
       EXTRACT(MONTH FROM client_time) AS month,
       CONCAT(EXTRACT(YEAR FROM client_time), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
       CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END AS `Persona_Type`,
       COUNT(DISTINCT l.user_id) AS `Total Users`
FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` l
JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 
    AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
) USING (user_id)
LEFT JOIN Persona p ON l.user_id = p.user_id
WHERE l.user_id IS NOT NULL
AND client_time >= CAST((SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey='persona_starting_date') AS TIMESTAMP)
AND client_time >= TIMESTAMP((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]))
AND client_time < TIMESTAMP(DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY))
AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1  [[AND {{ishopfilter}}]])
AND CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END IN (SELECT persona FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` WHERE 1=1 [[AND {{ipersona}}]])
AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{inotcountry}}]])
AND client_time >= TIMESTAMP(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), month), INTERVAL 2 YEAR))
AND client_time < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), month))
GROUP BY year, month, `Time`, `Persona_Type`
ORDER BY year ASC, month ASC, `Persona_Type` ASC
    )
  );
  
ELSE
  SET r319_q4260 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as INT64) as year, CAST(value2 as INT64) as month, CAST(value3_str as STRING) as `Time`, CAST(value4_str as STRING) as `Persona_Type`, CAST(value5 as INT64) as `Total Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4260 
  );
END IF;

SELECT * FROM UNNEST(r319_q4260);
