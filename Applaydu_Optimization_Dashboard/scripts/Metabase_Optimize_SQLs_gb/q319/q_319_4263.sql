DECLARE r319_q4263 ARRAY<STRUCT<month STRING,`Successfully Registered Email` INT64,`Verified email after registration` INT64,ratio FLOAT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4263
);

IF row_count = 0 THEN
  SET r319_q4263 = (
    SELECT ARRAY(
      WITH gb4263 AS (SELECT 0),
regis_regis AS (
    SELECT 
        DATE_TRUNC(client_time, MONTH) AS month,
        COUNT(DISTINCT regis.user_id) AS `Successfully Registered Email`
    FROM `gcp-bi-elephant-db-gold.applaydu.account_operation` AS regis
    WHERE 1=1 
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
        AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
        AND client_time >= '2024-01-01'
        AND account_operation = 'Email registration'
        AND result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
    GROUP BY month
), 
regis_veri AS (
    SELECT 
        month,
        COUNT(DISTINCT `Verified`) AS `Verified`
    FROM (
        SELECT 
            DATE_TRUNC(client_time, MONTH) AS month,
            CAST(user_id AS STRING) AS `Verified`
        FROM `gcp-bi-elephant-db-gold.applaydu.account_operation`
        WHERE account_operation = 'Email registration confirmation'
            AND result = 'Success'
            AND 1=1
            AND DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_v4_start_date')
            AND DATE(client_time) < (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_be_parent_registration_start_date')
            AND DATE(client_time) < CURRENT_DATE()
            AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
            AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
            AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
            AND client_time >= '2024-01-01'
        UNION ALL       
        SELECT 
            DATE_TRUNC(client_time, MONTH) AS month,
            anon_id AS `Verified`
        FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats_subscriptions`
        WHERE DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_be_parent_registration_start_date')
            AND DATE(client_time) < CURRENT_DATE()
            AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
            AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    )  
    GROUP BY month
)
--main query
SELECT AS STRUCT 
    month,
    `Successfully Registered Email`,
    `Verified` AS `Verified email after registration`,
    `Verified` / `Successfully Registered Email` AS ratio
FROM regis_regis
LEFT JOIN regis_veri USING (month)
ORDER BY month DESC
    )
  );
  
ELSE
  SET r319_q4263 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as month, CAST(value2 as INT64) as `Successfully Registered Email`, CAST(value3 as INT64) as `Verified email after registration`, CAST(value4 as FLOAT64) as ratio
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4263 
  );
END IF;

SELECT * FROM UNNEST(r319_q4263);
