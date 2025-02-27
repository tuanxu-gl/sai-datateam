DECLARE r319_q4234 ARRAY<STRUCT<`country_name` STRING,`Persona #1` INT64,`Persona #2` INT64,`Persona #3` INT64,`Active Users` INT64,`% Persona 1` FLOAT64,`% Persona 2` FLOAT64,`% Persona 3` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4234
);

IF row_count = 0 THEN
  SET r319_q4234 = (
    SELECT ARRAY(
      WITH gb4234 as (SELECT 0)
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
),
tbl_launch_resume AS (
    SELECT user_id, COALESCE(c.name, 'Unknown') AS country_name, client_time
    FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` l
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 
        AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
        AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    ) USING (user_id)
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.country` c ON l.country = c.code
)
--main query
SELECT AS STRUCT country_name, 
       COALESCE(`Persona #1`, 0) AS `Persona #1`, 
       COALESCE(`Persona #2`, 0) AS `Persona #2`, 
       COALESCE(`Persona #3`, 0) AS `Persona #3`,
       (`Persona #1` + `Persona #2` + `Persona #3`) AS `Active Users`,
       (`Persona #1` / (`Persona #1` + `Persona #2` + `Persona #3`)) * 100 AS `% Persona 1`,
       (`Persona #2` / (`Persona #1` + `Persona #2` + `Persona #3`)) * 100 AS `% Persona 2`, 
       (`Persona #3` / (`Persona #1` + `Persona #2` + `Persona #3`)) * 100 AS `% Persona 3`
FROM (
    SELECT *
    FROM (
        SELECT country_name, 
               CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END AS `Persona_Type`, 
               COUNT(DISTINCT l.user_id) AS `No. of Users`
        FROM tbl_launch_resume l
        LEFT JOIN Persona p ON l.user_id = p.user_id
        WHERE l.user_id IS NOT NULL
        AND client_time >= CAST((SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey='persona_starting_date') AS TIMESTAMP)
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        --AND `Persona_Type` IN (SELECT persona FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` WHERE 1=1 [[AND {{ipersona}}]])
        AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
        GROUP BY country_name, `Persona_Type`
        having `Persona_Type` IN (SELECT persona FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` WHERE 1=1 [[AND {{ipersona}}]])
    )
    PIVOT(SUM(`No. of Users`) FOR `Persona_Type` IN ('Persona #1', 'Persona #2', 'Persona #3')) AS pivottable
)
ORDER BY `Active Users` DESC, `Persona #3` DESC 
LIMIT 20
    )
  );
  
ELSE
  SET r319_q4234 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `country_name`, CAST(value2 as INT64) as `Persona #1`, CAST(value3 as INT64) as `Persona #2`, CAST(value4 as INT64) as `Persona #3`, CAST(value5 as INT64) as `Active Users`, CAST(value6 as FLOAT64) as `% Persona 1`, CAST(value7 as FLOAT64) as `% Persona 2`, CAST(value8 as FLOAT64) as `% Persona 3`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4234 
  );
END IF;

SELECT * FROM UNNEST(r319_q4234);
