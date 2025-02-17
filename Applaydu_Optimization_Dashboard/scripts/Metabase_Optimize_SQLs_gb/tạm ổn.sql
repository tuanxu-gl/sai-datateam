DECLARE store_stats_count ARRAY<STRUCT<Shop STRING, `Total Installations` INT64>>;

DECLARE row_count INT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4226
);

IF row_count = 0 THEN
  SET store_stats_count = (
    SELECT ARRAY(
      SELECT AS STRUCT
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(t.game_id AS STRING), '81335', 'App Store'), '81337', 'Google Play'), '82471', 'AppInChina'), '85247', 'AppInChina'), '84515', 'Samsung'), '85837', 'Amazon') AS Shop, 
        SUM(event_count) AS `Total Installations`
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
      JOIN 
        `applaydu.tbl_shop_filter` USING (game_id, country_name)
      WHERE 1=1 	
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])    
        AND event_id = 393584 
        AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads') 
        AND DATE(client_time) >= '2020-08-10' 
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version IN ('1.0.0') 
		[[AND {{ishopfilter}}]]
      GROUP BY Shop
    )
  );
ELSE
  SET store_stats_count = (
    SELECT ARRAY_AGG(
      STRUCT(
        value1_str AS Shop,
        CAST(value2 AS INT64) AS `Total Installations`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4226 
  );
END IF;

SELECT * FROM UNNEST(store_stats_count);