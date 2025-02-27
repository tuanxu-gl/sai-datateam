DECLARE r14_q247 ARRAY<STRUCT<`rating` FLOAT64>>;
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
      AND query_id = 247
  );
  
  IF row_count = 0 THEN
    SET r14_q247 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT (CAST(custom_tracking AS FLOAT64)) AS rating -- Max Rating in a week
FROM 
  `applaydu.store_stats`
WHERE 
  event_id = 393584 
  AND kpi_name IN ('Total Average Rating')
  AND DATE(client_time) >= istart_date
  AND DATE(client_time) < iend_date
  AND version IN ('1.0.0')
  AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])
ORDER BY 
  client_time DESC
LIMIT 1
      )
    );
    
  ELSE
    SET r14_q247 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1 as FLOAT64) as `rating`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 247 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q247);
  