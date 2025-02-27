--main query
SELECT 
    DATE(client_time) AS `Client time`,
    COUNT(DISTINCT user_id) AS `DAU`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
WHERE 1=1 
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND NOT (t.game_id = 82471 AND client_time < '2020-12-14')
GROUP BY 
    DATE(client_time)
ORDER BY 
    DATE(client_time) ASC