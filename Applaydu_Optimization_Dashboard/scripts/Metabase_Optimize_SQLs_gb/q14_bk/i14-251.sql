DECLARE r14_q251 ARRAY<STRUCT<`Client time` STRING,`DAU` INT64>>;
  DECLARE istart_date date;
  DECLARE iend_date date;
  DECLARE row_count FLOAT64;
  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);

  SET row_count = (
    SELECT COUNT(0) 
    FROM `applaydu.apd_report_14`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 14 
      AND query_id = 251
  );
  
  IF row_count = 0 THEN
    SET r14_q251 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    cast(DATE(client_time) as string) AS `Client time`,
    COUNT(DISTINCT user_id) AS `DAU`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
WHERE 1=1 
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND NOT (t.game_id = 82471 AND client_time < '2020-12-14')
GROUP BY 
    `Client time`
ORDER BY 
    `Client time` ASC
      )
    );
    
  ELSE
    SET r14_q251 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as `Client time`, CAST(value2 as INT64) as `DAU`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 251 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q251);
  