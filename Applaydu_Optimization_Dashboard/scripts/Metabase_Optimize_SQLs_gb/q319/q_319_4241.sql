DECLARE r319_q4241 ARRAY<STRUCT<country_name STRING,`Scan Toy` INT64,`Scan Leaflet` INT64,`Scan Deep Link` INT64,`Total scans` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4241
);

IF row_count = 0 THEN
  SET r319_q4241 = (
    SELECT ARRAY(
      --main query
SELECT AS STRUCT country_name,
    SUM(CASE WHEN scan_type IN ('Toy Scan', 'Scan_Toy') THEN total_scan ELSE 0 END) AS `Scan Toy`,
    SUM(CASE WHEN scan_type IN ('Scan_QR', 'QR Code', 'Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette') THEN total_scan ELSE 0 END) AS `Scan Leaflet`,
    SUM(CASE WHEN scan_type IN ('Deep_Link') THEN total_scan ELSE 0 END) AS `Scan Deep Link`,
    SUM(total_scan) AS `Total scans`
FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_sum_scan_unlock` t
JOIN `applaydu.tbl_shop_filter` a ON a.game_id = t.game_id AND a.country = t.country 
WHERE scan_type IN ('Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette', 'Scan_QR', 'QR Code', 'Toy Scan', 'Scan_Toy', 'Deep_Link')
    AND DATE(server_date) >= '2020-08-10' 
    AND DATE(server_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND DATE(server_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(server_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    [[AND {{ishopfilter}}]]
GROUP BY country_name
ORDER BY `Total scans` DESC
    )
  );
  
ELSE
  SET r319_q4241 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as country_name, CAST(value2 as INT64) as `Scan Toy`, CAST(value3 as INT64) as `Scan Leaflet`, CAST(value4 as INT64) as `Scan Deep Link`, CAST(value5 as INT64) as `Total scans`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4241 
  );
END IF;

SELECT * FROM UNNEST(r319_q4241);
