DECLARE r319_q4242 ARRAY<STRUCT<`Month` STRING,`Retention D1` STRING,`Retention D3` STRING,`Retention D7` STRING,`Retention D14` STRING,`Retention D28` STRING,`Retention D30` STRING,`D7 per D1` STRING>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4242
);

IF row_count = 0 THEN
  SET r319_q4242 = (
    SELECT ARRAY(
      WITH gb4242 as (SELECT 0)
,unlock AS (
    SELECT DISTINCT user_id, COUNT(*) AS `Number of Toys Unlocked`
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    WHERE (
        `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`.unlock_cause IN ('QR Code', 'Toy Scan', 'Deep_Link')
        AND `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`.isnewtoy = 1
    )
    AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey='persona_starting_date')
    GROUP BY user_id
    HAVING `Number of Toys Unlocked` > 0
)
,
launch_raw AS (
    SELECT user_id, DATE(client_time)AS login_Day, 
           MIN(DATE(client_time)) OVER (PARTITION BY user_id) AS `First Day`, 
           version
    FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume`
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 
        AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1 [[AND {{iinstallsource}}]])
        AND DATE(active_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(active_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    ) USING (user_id)
    JOIN unlock USING (user_id)
    WHERE `gcp-bi-elephant-db-gold.applaydu.launch_resume`.country IN ('IN', 'BR', 'RU', 'US', 'IT', 'MX')
    AND game_id IN (81337, 81335)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    GROUP BY user_id, login_Day, version,client_time
),
launch_with_version AS (
    SELECT user_id, login_Day, `First Day`, 
           DATE_DIFF(login_Day, `First Day`, DAY) AS Day_number
    FROM launch_raw
    WHERE `First Day` >= '2022-06-01' 
    AND `First Day` < DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
    AND `First Day` >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND `First Day` < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND `First Day` >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 30 MONTH)
    ORDER BY user_id, Day_number
)
,
retention AS (
    SELECT DATE_TRUNC(`First Day`, MONTH) AS Month,
           FORMAT_DATE('%A', `First Day`) AS `Weekday`, `First Day`,
           SUM(CASE WHEN Day_number = 0 THEN 1 ELSE 0 END) AS `No of New user Acquired`,
           SUM(CASE WHEN Day_number = 0 THEN 1 ELSE 0 END) AS Day_0,
           SUM(CASE WHEN Day_number = 1 THEN 1 ELSE 0 END) AS Day_1,
           SUM(CASE WHEN Day_number = 2 THEN 1 ELSE 0 END) AS Day_3,
           SUM(CASE WHEN Day_number = 3 THEN 1 ELSE 0 END) AS Day_7,
           SUM(CASE WHEN Day_number = 4 THEN 1 ELSE 0 END) AS Day_14,
           SUM(CASE WHEN Day_number = 5 THEN 1 ELSE 0 END) AS Day_28,
           SUM(CASE WHEN Day_number = 6 THEN 1 ELSE 0 END) AS Day_30
    FROM launch_with_version
    GROUP BY `First Day`
    HAVING `Weekday` IN ('Friday', 'Saturday')
    ORDER BY `First Day`
)
--main query
SELECT AS STRUCT cast (Month as string) as `Month`,
       CONCAT(ROUND(SUM(Day_1)/SUM(Day_0)*100, 2), '%') AS `Retention D1`,
       CONCAT(ROUND(SUM(Day_3)/SUM(Day_0)*100, 2), '%') AS `Retention D3`,
       CONCAT(ROUND(SUM(Day_7)/SUM(Day_0)*100, 2), '%') AS `Retention D7`,
       CONCAT(ROUND(SUM(Day_14)/SUM(Day_0)*100, 2), '%') AS `Retention D14`,
       CONCAT(ROUND(SUM(Day_28)/SUM(Day_0)*100, 2), '%') AS `Retention D28`,
       CONCAT(ROUND(SUM(Day_30)/SUM(Day_0)*100, 2), '%') AS `Retention D30`,
       CONCAT(ROUND(SUM(Day_7)/SUM(Day_1)*100, 2), '%') AS `D7 per D1`
FROM retention
GROUP BY `Month`
ORDER BY `Month` ASC
    )
  );
  
ELSE
  SET r319_q4242 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Month`, CAST(value2_str as STRING) as `Retention D1`, CAST(value3_str as STRING) as `Retention D3`, CAST(value4_str as STRING) as `Retention D7`, CAST(value5_str as STRING) as `Retention D14`, CAST(value6_str as STRING) as `Retention D28`, CAST(value7_str as STRING) as `Retention D30`, CAST(value8_str as STRING) as `D7 per D1`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4242 
  );
END IF;

SELECT * FROM UNNEST(r319_q4242);
