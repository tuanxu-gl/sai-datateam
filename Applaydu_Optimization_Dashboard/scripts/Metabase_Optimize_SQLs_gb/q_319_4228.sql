DECLARE r319_q4228 ARRAY<STRUCT<`One and Done` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4228
);

IF row_count = 0 THEN
  SET r319_q4228 = (
    SELECT ARRAY(
      WITH gb4228 as (select 0)
,user_type AS (
    SELECT 
        user_id, 
        COUNT(*) AS number_of_sessions, 
        'One and Done Users' AS UserType
    FROM 
        `gcp-bi-elephant-db-gold.applaydu.launch_resume` t 
    JOIN 
        (
            SELECT DISTINCT user_id 
            FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
            WHERE 1=1 
                AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
                AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
                AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        ) USING (user_id)
    WHERE 1=1 
        [[AND game_id = {{ggi}}]]
    GROUP BY 
        1
    HAVING 
        number_of_sessions = 1
),
main AS (
    SELECT 
        lr.*, 
        ut.UserType
    FROM 
        `gcp-bi-elephant-db-gold.applaydu.launch_resume` lr
    JOIN 
        (
            SELECT DISTINCT user_id 
            FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
            WHERE 1=1 
                AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
                AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
                AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        ) USING (user_id)
    LEFT JOIN 
        user_type ut USING (user_id)
    WHERE 1=1 
        [[AND game_id = {{ggi}}]]
    ORDER BY 
        lr.user_id
)
--main query
SELECT AS STRUCT 
    SUM(CASE WHEN UserType = 'One and Done Users' THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) AS `One and Done`
FROM 
    main t
WHERE 
    CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND (session_id = 1 OR CAST(time_between_sessions AS INT64) >= 30)
    AND DATE(client_time) >= '2020-08-10' 
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])  
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{inotcountry}}]])  
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])
    )
  );
  
ELSE
  SET r319_q4228 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1 as FLOAT64) as `One and Done`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4228 
  );
END IF;

SELECT * FROM UNNEST(r319_q4228);
