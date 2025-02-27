--main query
SELECT 
    SUM(CAST(time_spent AS INT64)) AS `Total time spent`,
    COUNT(DISTINCT user_id) AS `Total Users`,
    SUM(CAST(time_spent AS INT64)) / COUNT(DISTINCT user_id) AS time_result,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Average Time per Users`
FROM 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
JOIN 
    `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country = t.country
WHERE 1=1
    AND CAST(time_spent AS INT64) >= 0
    AND CAST(time_spent AS INT64) < 86400
    AND version >= (SELECT MIN(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{from_version}}]]) 
    AND version <= (SELECT MAX(version) FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{to_version}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND server_time >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]) 
    AND server_time < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country IN (SELECT country FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]])
    [[AND {{ishopfilter}}]]