DECLARE r14_q243 ARRAY<STRUCT<`Country name` STRING,`Downloads` INT64>>;
  DECLARE row_count FLOAT64;
  DECLARE istart_date DATE;
  DECLARE iend_date DATE;
  DECLARE iversions ARRAY<STRING>;
  DECLARE ifrom_version STRING;
  DECLARE ito_version STRING;
  DECLARE icountry ARRAY<STRING>;
  DECLARE icountry_region ARRAY<STRING>;

  SET istart_date = (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iend_date = (SELECT DATE_ADD(MAX(server_date), INTERVAL 1 DAY) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]);
  SET iversions = ARRAY(SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{version}}]]);
  SET ifrom_version = (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET ito_version = (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]);
  SET icountry = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]);
  SET icountry_region = ARRAY(SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]);

  
  SET row_count = (
    SELECT COUNT(0) 
    FROM `applaydu.apd_report_14`
    WHERE 1=1 
      AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 14 
      AND query_id = 243
  );
  
  IF row_count = 0 THEN
    SET r14_q243 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    REPLACE(country_name, 'Undefined', '(no country code)') AS `Country name`,
    SUM(event_count) AS `Downloads`
FROM 
    `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 
    1=1  
    AND DATE(client_time) >= istart_date
    AND DATE(client_time) < iend_date
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])
    AND version IN UNNEST(iversions)
    AND event_id = 393584 
    AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version IN ('1.0.0')
GROUP BY 
    country_name
LIMIT 30
      )
    );
    
  ELSE
    SET r14_q243 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as `Country name`, CAST(value2 as INT64) as `Downloads`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 243 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q243);
  