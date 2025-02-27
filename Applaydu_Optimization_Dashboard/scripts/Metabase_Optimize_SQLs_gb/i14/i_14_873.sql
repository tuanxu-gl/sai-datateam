--main query
SELECT 
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
    AND DATE(server_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]])
    AND DATE(server_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    [[AND {{ishopfilter}}]]
GROUP BY 
    `Scan type`
ORDER BY 
    `Scan type` ASC