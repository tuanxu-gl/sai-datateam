DECLARE r14_q450 ARRAY<STRUCT<`Date` STRING,`Daily Downloads` INT64>>;
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
      AND query_id = 450
  );
  
  IF row_count = 0 THEN
    SET r14_q450 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    cast(DATE(client_time) as string) AS `Date`,
    SUM(event_count) AS `Daily Downloads`
FROM 
    `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
JOIN 
    `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name
WHERE 1=1 and
    event_id = 393584 
    AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version IN ('1.0.0')
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])    
    [[AND {{ishopfilter}}]]
GROUP BY 
    `Date`
      )
    );
    
  ELSE
    SET r14_q450 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as `Date`, CAST(value2 as INT64) as `Daily Downloads`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 450 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q450);
  