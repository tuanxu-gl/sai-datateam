--main query
SELECT 
    EXTRACT(MONTH FROM client_time) AS `Month`,
    EXTRACT(YEAR FROM client_time) AS `Year`,
    CONCAT(CAST(EXTRACT(YEAR FROM client_time) AS STRING), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
    COUNT(DISTINCT user_id) AS `Monthly Active Users`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume`
WHERE 1=1
    AND NOT (game_id = 82471 AND client_time < '2020-12-14')
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
GROUP BY all
    