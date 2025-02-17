DECLARE r319_q4247 ARRAY<STRUCT<`Persona_Type` STRING,`Total Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4247
);

IF row_count = 0 THEN
  SET r319_q4247 = (
    SELECT ARRAY(
      WITH gb4247 as (SELECT 0)
,unlock AS (
    SELECT DISTINCT user_id, COUNT(*) AS `Number of Toys Unlocked`
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    WHERE (unlock_cause = 'QR Code'
        OR unlock_cause = 'Toy Scan' 
        OR unlock_cause = 'Deep_Link')
        AND isnewtoy = 1
        AND client_time >= CAST((SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey='persona_starting_date') AS TIMESTAMP)
    GROUP BY user_id
),
Persona AS (
    SELECT user_id, 
           CASE WHEN `Number of Toys Unlocked` IN (1,2,3) THEN 'Persona #2'
                ELSE 'Persona #3' END AS `Persona Type`
    FROM unlock
)
--main query
SELECT AS STRUCT CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END AS `Persona_Type`,
       COUNT(DISTINCT l.user_id) AS `Total Users`
FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` l
LEFT JOIN Persona p ON l.user_id = p.user_id
JOIN (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    WHERE 1=1 
    AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
    AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
) install ON l.user_id = install.user_id
WHERE l.user_id IS NOT NULL
AND client_time >= CAST((SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey='persona_starting_date') AS TIMESTAMP)
AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
--AND `Persona_Type` IN (SELECT persona FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` WHERE 1=1 [[AND {{ipersona}}]])
AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
GROUP BY `Persona_Type`
having  `Persona_Type` IN (SELECT persona FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` WHERE 1=1 [[AND {{ipersona}}]])
ORDER BY `Persona_Type`
    )
  );
  
ELSE
  SET r319_q4247 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Persona_Type`, CAST(value2 as INT64) as `Total Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4247 
  );
END IF;

SELECT * FROM UNNEST(r319_q4247);
