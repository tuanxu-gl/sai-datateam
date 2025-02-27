DECLARE r14_q54 ARRAY<STRUCT<`Country name` STRING,`Users` INT64>>;
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
      AND query_id = 54
  );
  
  IF row_count = 0 THEN
    SET r14_q54 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    d_country AS `Country name`,
    SUM(event_count) AS `Users`
FROM 
    `applaydu.store_stats`
WHERE 1=1
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])    
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_shop_filter` sf WHERE 1=1 [[AND {{ishopfilter}}]])
    AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` sf WHERE 1=1 [[AND {{ishopfilter}}]])
    AND event_id = 393584 
    AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
GROUP BY 
    d_country
      )
    );
    
  ELSE
    SET r14_q54 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as `Country name`, CAST(value2 as INT64) as `Users`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 54 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q54);
  