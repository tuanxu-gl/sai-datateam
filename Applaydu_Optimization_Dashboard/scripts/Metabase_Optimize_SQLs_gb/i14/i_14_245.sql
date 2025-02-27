--main query
WITH t_users AS (
    SELECT 
        user_id,
        SUM(total_time_spent) AS total_time_spent,
        SUM(toy_unlocked_by_scan_count) AS toy_unlocked_by_scan_count,
        SUM(scan_mode_finished_count) AS scan_mode_finished_count
    FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
    JOIN 
        `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name
    WHERE 1=1
        AND DATE(server_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]])
        AND DATE(server_date) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])   
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])    	
        [[AND {{ishopfilter}}]]
    GROUP BY 
        user_id
)
SELECT `Time spent`
FROM (
    SELECT 
        COUNT(DISTINCT user_id) AS users,
        SUM(total_time_spent) AS sum_total_time_spent,
        SUM(total_time_spent) / COUNT(DISTINCT user_id) AS time_result,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent`
    FROM 
        t_users
    WHERE 
        toy_unlocked_by_scan_count > 0 OR scan_mode_finished_count > 0 
)
WHERE `Time spent` IS NOT NULL