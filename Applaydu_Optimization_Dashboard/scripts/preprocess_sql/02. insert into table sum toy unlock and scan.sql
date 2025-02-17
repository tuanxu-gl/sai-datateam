INSERT INTO `gcp-gfb-sai-tracking-gold.applaydu.tbl_sum_scan_unlock` (
    game_id,
    version,
    server_date,
    country,
    scan_type,
    toy_reference,
    leftover_type,
    category,
    toy_name,
    total_scan
)
(
SELECT 
    game_id,
    version,
    server_date,
    country,
    scan_type,
    LEFT(toy_reference, 255) as toy_reference,
    leftover_type,
    category,
    toy_name,
    COUNT(*) as total_scan
FROM (
    select *,
        CASE WHEN bug_unlock = 1 THEN 'Deep_Link' ELSE unlock_cause END as scan_type
    from
    (
    SELECT 
        game_id,
        version,
        timestamp(client_time) as server_date,
        country,
        CASE WHEN reference = 'N/A' THEN 'N/A' ELSE REGEXP_EXTRACT(reference, r'[^\/]*$') END as toy_reference,
        CASE WHEN CONTAINS_SUBSTR(reference, 'mkqr.biz') = FALSE THEN 'New toys' ELSE 'Leftover toys' END as leftover_type,
        gdd_toy_list.game_element_internal_maincategory as category,
        unlock_cause,
        CASE 
            WHEN (
                (gdd_toy_list.game_element_internal_subcategory = 'DisneyPrincess' AND country NOT IN ('DE','AT','CH','CZ','BE','NL','LU','RO','HU','IT','FR','ES','PT','UA','IN')) OR 
                (gdd_toy_list.game_element_internal_subcategory = 'Marvel' AND country NOT IN ('DE','AT','CH','PL','BE','NL','LU','RO','HU','IT','FR','ES','PT','UA','RU')) OR 
                (gdd_toy_list.game_element_internal_subcategory = 'Miraculous' AND country NOT IN ('DE','AT','CH','BE','NL','LU','RO','HU','IT','FR','ES','PT','ZA','IL','TW')) OR 
                (gdd_toy_list.game_element_internal_subcategory = 'Minions' AND country NOT IN ('DE','AT','CH','PL','CZ','BE','NL','LU','RO','HU','IT','FR','GR','UA','IL','BH','KW','OM','QA','SA','AE','YE','DZ','EG','LY','MA','TN','IQ','JO','LB','PS','SY','HK','KR','TH','VN','MY','KH','IN')) OR 
                (gdd_toy_list.game_element_internal_subcategory = 'JusticeLeague' AND country NOT IN ('UA','IN')) OR 
                (gdd_toy_list.game_element_internal_subcategory = 'Jurassic' AND country NOT IN ('BH','KW','OM','QA','SA','AE','YE','DZ','EG','LY','MA','TN','IQ','JO','LB','PS','SY','SG','MY','PH','TH','ID','BN','VN','HK','KR','ZA','IL','CN'))
            ) AND unlock_cause = 'Experience' AND toy_amount = 0 AND gdd_toy_list.game_element_internal_maincategory = 'Licensing'
            THEN 1 ELSE 0 END as bug_unlock,
        toy_name
    FROM `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    LEFT JOIN `gcp-gfb-sai-tracking-gold.applaydu.gdd_toy_list` gdd_toy_list
    ON gdd_toy_list.old_odd_name = `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`.toy_name
    WHERE (
        (
            unlock_cause IN ('Alternative Vignette', 'QR Code', 'Toy Scan', 'Vignette') AND 
            version IN (SELECT ivalue FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` WHERE ikey = 'versions_use_toy_unlock')
        ) OR unlock_cause IN ('Deep_Link', 'Experience')
    ) AND DATE(client_time) >= '2024-11-21'
    )
)
GROUP BY scan_type, leftover_type, category, server_date, country, game_id, version, toy_name, toy_reference
)
UNION all
(
SELECT 
    game_id,
    version,
    timestamp(client_time) as server_date,
    country,
    CASE 
        WHEN t.toy_detected LIKE '%_leftover' AND t.reference NOT LIKE 'http%' AND t.version IN ('3.1.0', '3.1.2', '3.2.0', '3.2.1') 
        THEN 'Scan_Toy' ELSE scan_type END as scan_type,
    CASE 
        WHEN t.scan_type = 'Scan_Toy' AND version IN ('2.0.1', '2.0.2', '2.0.4', '2.0.7', '2.0.8', '2.0.9', '2.2.0', '2.2.1', '2.2.2', '2.2.3', '2.3.0', '2.3.1', '2.4.3', '2.5.0', '2.6.0', '2.6.1', '2.6.2', '2.6.3', '2.7.0', '2.7.1', '2.7.2', '2.7.3', '3.0.0', '3.0.1', '3.0.2', '3.0.3', '3.0.4', '3.0.5', '3.0.6', '3.0.7')
        THEN COALESCE(UPPER(t.toy_detected), 'Undefined') 
        ELSE 
            CASE WHEN t.reference IS NULL OR t.reference = 'N/A' THEN 'Undefined' ELSE COALESCE(UPPER(REGEXP_EXTRACT(t.reference, r'[^\/]*$')), 'Undefined') END
        END as toy_detected,
    CASE WHEN CONTAINS_SUBSTR(reference, 'mkqr.biz') = FALSE THEN 'New toys' ELSE 'Leftover toys' END as leftover_type,
    gdd_toy_list.game_element_internal_maincategory as category,
    toy_name,
    COUNT(*) as total_scan
FROM `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished` t
LEFT JOIN `gcp-gfb-sai-tracking-gold.applaydu.gdd_toy_list` gdd_toy_list
ON gdd_toy_list.old_odd_name = t.toy_name
WHERE scan_type IN ('Alternative_Vignette', 'Scan_Toy', 'Scan_QR', 'Scan_Vignette', 'Easter Card')
AND DATE(client_time) >= '2021-01-06'
AND DATE(client_time) >= '2024-11-21'
AND scan_result IN ('New_Toy', 'Old_Toy')
GROUP BY scan_type, leftover_type, category, server_date, country, game_id, version, toy_name, toy_detected, t.reference
)
UNION all
(
SELECT 
    game_id,
    version,
    timestamp(server_date) as server_date,
    country,
    'Scan_Toy' as scan_type,
    toy_detected,
    'New toys' as leftover_type,
    gdd_toy_list.game_element_internal_maincategory as category,
    gdd_toy_list.old_odd_name as toy_name,
    SUM(total_scan) as total_scan
FROM `gcp-gfb-sai-tracking-gold.applaydu.tbl_scan_mode_finished_24x`
LEFT JOIN `gcp-gfb-sai-tracking-gold.applaydu.gdd_toy_list` gdd_toy_list
ON gdd_toy_list.ingame_name = toy_detected
WHERE total_scan > 0 and server_date >= '2024-11-21'
GROUP BY scan_type, leftover_type, category, server_date, country, game_id, version, old_odd_name, toy_detected
)
