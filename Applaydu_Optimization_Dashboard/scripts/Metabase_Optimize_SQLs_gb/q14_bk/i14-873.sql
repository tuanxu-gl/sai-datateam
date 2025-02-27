DECLARE r14_q873 ARRAY<STRUCT<`Scan type` STRING,`Scans` INT64>>;
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
      AND query_id = 873
  );
  
  IF row_count = 0 THEN
    SET r14_q873 = (
      SELECT ARRAY(
        --main query
SELECT AS STRUCT 
    CASE 
        WHEN scan_type IN ('Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette') THEN 'Vignette'
        WHEN scan_type IN ('Scan_QR', 'QR Code', 'Deep_Link') THEN 'QR Code'
        WHEN scan_type IN ('Toy Scan', 'Scan_Toy') THEN 'Scan toy'
        ELSE 'Others' 
    END AS `Scan type`,
    SUM(total_scan) AS `Scans`
FROM 
    `applaydu.tbl_sum_scan_unlock` t
JOIN 
    `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country = t.country
WHERE 1=1
    AND scan_type IN ('Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette', 'Scan_QR', 'QR Code', 'Toy Scan', 'Scan_Toy', 'Deep_Link')
    AND DATE(server_date) >= istart_date
    AND DATE(server_date) < iend_date
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    [[AND {{ishopfilter}}]]
GROUP BY 
    `Scan type`
ORDER BY 
    `Scan type` ASC
      )
    );
    
  ELSE
    SET r14_q873 = (
      SELECT ARRAY_AGG(
        STRUCT(
           CAST(value1_str as STRING) as `Scan type`, CAST(value2 as INT64) as `Scans`
        )
      )
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14`
      WHERE 
        DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
        AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
        AND dashboard_id = 14 
        AND query_id = 873 
    );
  END IF;

  SELECT * FROM UNNEST(r14_q873);
  