DECLARE r319_q4271 ARRAY<STRUCT<`Users` STRING,`Number of Users` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4271
);

IF row_count = 0 THEN
  SET r319_q4271 = (
    SELECT ARRAY(
      WITH gb4271 as (SELECT 0)
,total_apd AS (
    SELECT DISTINCT user_id 
    FROM `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
    JOIN `applaydu.tbl_shop_filter` ON `applaydu.tbl_shop_filter`.game_id = t.game_id AND `applaydu.tbl_shop_filter`.country = t.country
    WHERE 1=1
    AND session_id = 1
    AND NOT (t.game_id = 82471 AND DATE(client_time) < '2020-12-14')
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND date(client_time) >= '2024-01-01'
    [[AND {{ishopfilter}}]]
),
ftue_list AS (
    SELECT DISTINCT user_id
    FROM `gcp-bi-elephant-db-gold.applaydu.ftue_event`
    WHERE 1=1 
    AND user_id IN (SELECT * FROM total_apd)
    AND ftue_stage IN ('Start')
    AND ftue_steps IN ('Email Registration')
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND DATE(client_time) >= '2024-01-01'
    AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])
),
regis_regis AS (
    SELECT COUNT(DISTINCT regis.user_id) AS `Successfully Registered Email`
    FROM `gcp-bi-elephant-db-gold.applaydu.account_operation` AS regis
    WHERE 1=1 
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])
    AND DATE(client_time) >= '2024-01-01'
    AND account_operation = 'Email registration' 
    AND result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
),
regis_veri AS (
    SELECT COUNT(DISTINCT user_id) AS `Verified email after registration`
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
    AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])
    AND DATE(client_time) >= '2024-01-01'
),
regis_veri_be AS (
    SELECT COUNT(DISTINCT anon_id) AS `Verified email after registration`
    FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats_subscriptions`
    WHERE DATE(client_time) >= (SELECT DATE(ivalue) FROM `applaydu.tbl_variables` WHERE ikey = 'apd_be_parent_registration_start_date')
    AND DATE(client_time) < CURRENT_DATE()
    AND DATE(client_time) >= (SELECT MIN(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(DATE(server_date)) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` WHERE 1=1 [[AND {{ishopfilter}}]])
)
,result as
(
    SELECT 'New Users Launch' AS Users, COUNT(DISTINCT user_id) AS `Number of Users`
    FROM total_apd
    UNION ALL
    SELECT 'Email Registration Screen' AS Users, COUNT(DISTINCT user_id) AS `Number of Users`
    FROM ftue_list
    UNION ALL
    SELECT 'Finish Email Registration' AS Users, `Successfully Registered Email` AS `Number of Users`
    FROM regis_regis
    UNION ALL
    SELECT 'Email Verification' AS Users, (SELECT `Verified email after registration` FROM regis_veri) + (SELECT `Verified email after registration` FROM regis_veri_be) AS `Number of Users`
)
--main query
SELECT AS STRUCT * from result
    )
  );
  
ELSE
  SET r319_q4271 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Users`, CAST(value2 as INT64) as `Number of Users`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4271 
  );
END IF;

SELECT * FROM UNNEST(r319_q4271);
