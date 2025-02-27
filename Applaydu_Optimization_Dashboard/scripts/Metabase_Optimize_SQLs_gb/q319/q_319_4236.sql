DECLARE r319_q4236 ARRAY<STRUCT<`year_month` STRING,`Country` STRING,`Successfully Registered Email` INT64,`Verified email after registration` INT64,ratio FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4236
);

IF row_count = 0 THEN
  SET r319_q4236 = (
    SELECT ARRAY(
      --main query
SELECT AS STRUCT 
        CONCAT(EXTRACT(YEAR FROM client_time), '-', LPAD(CAST(EXTRACT(MONTH FROM client_time) AS STRING), 2, '0')) AS year_month,
        `gcp-bi-elephant-db-gold.dimensions.country`.name AS `Country`,
        COUNT(DISTINCT CASE 
            WHEN account_operation = 'Email registration' 
                AND result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
            THEN user_id 
            ELSE NULL 
        END) AS `Successfully Registered Email`,
        COUNT(DISTINCT CASE 
            WHEN account_operation = 'Email registration confirmation' 
                AND result = 'Success' 
            THEN user_id 
            ELSE NULL 
        END) AS `Verified email after registration`,
        COUNT(DISTINCT CASE 
            WHEN account_operation = 'Email registration confirmation' 
                AND result = 'Success' 
            THEN user_id 
            ELSE NULL 
        END) / COUNT(DISTINCT CASE 
            WHEN account_operation = 'Email registration' 
                AND result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
            THEN user_id 
            ELSE NULL 
        END) AS ratio
    FROM `gcp-bi-elephant-db-gold.applaydu.account_operation`
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.country` ON `gcp-bi-elephant-db-gold.applaydu.account_operation`.country = `gcp-bi-elephant-db-gold.dimensions.country`.code
    JOIN (
        SELECT DISTINCT user_id 
        FROM `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        WHERE 1=1 [[AND {{iinstallsource}}]]
    ) USING (user_id)
    WHERE 1=1 
        AND client_time >= TIMESTAMP(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 YEAR))
        AND client_time < TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), MONTH))
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND `gcp-bi-elephant-db-gold.dimensions.country`.name IN ('Brazil', 'United States', 'Russian Federation', 'Italy', 'United Kingdom', 'Germany', 'France', 'Mexico', 'Argentina', 'Canada')
    GROUP BY year_month, `Country`
    ORDER BY `Country` ASC
    )
  );
  
ELSE
  SET r319_q4236 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `year_month`, CAST(value2_str as STRING) as `Country`, CAST(value3 as INT64) as `Successfully Registered Email`, CAST(value4 as INT64) as `Verified email after registration`, CAST(value5 as FLOAT64) as ratio
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4236 
  );
END IF;

SELECT * FROM UNNEST(r319_q4236);
