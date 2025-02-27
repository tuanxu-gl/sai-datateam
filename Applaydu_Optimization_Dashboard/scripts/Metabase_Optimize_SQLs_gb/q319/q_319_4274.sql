DECLARE r319_q4274 ARRAY<STRUCT<`Month` STRING,`Retention D1` FLOAT64,`Retention D7` FLOAT64,`Retention D28` FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4274
);

IF row_count = 0 THEN
  SET r319_q4274 = (
    SELECT ARRAY(
      with q4274 as (SELECT 0)
--main query
SELECT AS STRUCT 
    Month,
    SUM(Day_1) / SUM(Day_0) AS `Retention D1`,
    SUM(Day_7) / SUM(Day_0) AS `Retention D7`,
    SUM(Day_28) / SUM(Day_0) AS `Retention D28`
FROM (
    SELECT DATE_TRUNC(with_Day_number.`First Day`, MONTH) AS Month,
           FORMAT_TIMESTAMP('%A', with_Day_number.`First Day`) AS `Weekday`,
           `First Day`,
           SUM(CASE WHEN Day_number = 0 THEN 1 ELSE 0 END) AS `No. of New user Acquired`,
           SUM(CASE WHEN Day_number = 0 THEN 1 ELSE 0 END) AS Day_0,
           SUM(CASE WHEN Day_number = 1 THEN 1 ELSE 0 END) AS Day_1,
           SUM(CASE WHEN Day_number = 7 THEN 1 ELSE 0 END) AS Day_7,
           SUM(CASE WHEN Day_number = 28 THEN 1 ELSE 0 END) AS Day_28
    FROM (
        SELECT
            a.user_id,
            a.login_Day,
            b.first_day AS `First Day`,
            b.first_version AS first_version,
            b.first_country AS first_country,
            DATE_DIFF(a.login_Day, b.first_day, DAY) AS Day_number
        FROM (
            SELECT
                user_id,
                DATE_TRUNC(DATE(CLIENT_TIME), DAY) AS login_Day
            FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
            JOIN (
                SELECT DISTINCT user_id 
                FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
                WHERE 1=1 
                AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
            ) USING (user_id)
            JOIN `applaydu.tbl_shop_filter` using (game_id,country )
            GROUP BY user_id, DATE_TRUNC(DATE(CLIENT_TIME), DAY)
        ) a,
        (
            SELECT
                user_id,
                MIN(DATE_TRUNC(DATE(CLIENT_TIME), DAY)) AS first_day,
                MIN(version) AS first_version,
                MIN(t.country) AS first_country
            FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
            JOIN (
                SELECT DISTINCT user_id 
                FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
                WHERE 1=1 
                AND install_source IN (SELECT ua_filter FROM `applaydu.tbl_ua_filter` WHERE 1=1  [[AND {{iinstallsource}}]])
            ) USING (user_id)
            JOIN `applaydu.tbl_shop_filter` using (game_id,country )
            GROUP BY user_id
        ) b
        WHERE a.user_id = b.user_id
    ) AS with_Day_number
    WHERE FORMAT_TIMESTAMP('%A', with_Day_number.`First Day`) IN ('Friday', 'Saturday')
    AND date(with_Day_number.`First Day`) >= (DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 YEAR))
    AND date(with_Day_number.`First Day`) < (DATE_TRUNC(CURRENT_DATE(), MONTH))
    AND date(with_Day_number.`First Day`) >= ((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]))
    AND date(with_Day_number.`First Day`) < (DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY))
    AND first_country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND first_version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1  [[AND {{iversion}}]])
    [[AND {{ishopfilter}}]]
    GROUP BY `First Day`
    ORDER BY `First Day`
)
GROUP BY Month
ORDER BY Month
    )
  );
  
ELSE
  SET r319_q4274 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Month`, CAST(value2 as FLOAT64) as `Retention D1`, CAST(value3 as FLOAT64) as `Retention D7`, CAST(value4 as FLOAT64) as `Retention D28`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4274 
  );
END IF;

SELECT * FROM UNNEST(r319_q4274);
