--main query
SELECT 
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cast(game_id as STRING), 
        '81335', 'App Store')
        ,'81337', 'Google Play')
        , '82471','AppInChina')
        , '84155','Google Play')
        , '84515','Samsung')
        , '84137','AppInChina') 
        , '85837','Amazon') AS `Shop`,
    COUNT(DISTINCT user_id) AS `Total Users`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume`
WHERE 1=1
    AND NOT (game_id = 82471 AND client_time < '2020-12-14')
    AND CAST(time_spent AS FLOAT64) >= 0
    AND CAST(time_spent AS FLOAT64) < 86400
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])    
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
    AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
GROUP BY 
    `Shop`