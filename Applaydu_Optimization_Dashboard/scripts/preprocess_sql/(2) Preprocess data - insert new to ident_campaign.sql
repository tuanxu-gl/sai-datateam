insert into gcp-gfb-sai-tracking-gold.applaydu.ident_campaign (game_id, kpi, utm_medium, fCountry, ident_id, event_count, utm_source, fDate, load_time) 

select game_id
	, kpi
	, left(utm_medium, 255) as utm_medium
	, replace(replace(replace(replace(replace(replace(replace(replace(replace(fCountry, 'Bosnia and Herzegowina', 'Bosnia and Herzegovina')
		, 'Congo', 'Democratic Republic of the Congo')
		, 'Curaçao', 'Curacao')
		, 'Macedonia, The Former Yugoslav Republic of', 'North Macedonia')
		, 'N/A', 'Undefined')
		, 'Slovakia (Slovak Republic)', 'Slovakia')
		, 'Swaziland', 'Switzerland')
		, 'Taiwan, Province of China', 'Taiwan')
		, 'Viet Nam', 'Vietnam') as fcountry

	, left(ident_id, 255) as ident_id
	, sum(event_count) as event_count
	, left(utm_source, 255) as utm_source
	, TIMESTAMP(fDate)
	, current_timestamp() as load_time
from
(
 
	Select game_id, KPI , iUTM_MEDIUM as UTM_MEDIUM
		, event_count
		, fDate
		, fCountry
		, ident_id
		, utm_source
	from 
	(SELECT 
    game_id,
    'store_referral_aws' as kpi,
    REPLACE(REPLACE(utm_medium, '(not%20set)', 'organic'), 'N/A', 'organic') as iutm_medium,
    SUM(event_count) as event_count,
    DATE(client_time) as fdate,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(country_name, 'Bosnia and Herzegovina', 'Bosnia and Herzegovina'), 'Congo', 'Democratic Republic of the Congo'), 'Curaçao', 'Curacao'), 'Macedonia, The Former Yugoslav Republic of', 'North Macedonia'), 'N/A', 'Undefined'), 'Slovakia (Slovak Republic)', 'Slovakia'), 'Swaziland', 'Switzerland'), 'Taiwan, Province of China', 'Taiwan'), 'Viet Nam', 'Vietnam') as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats_store_referral_aws`
WHERE 1=1
    AND event_id = 393584 
    AND kpi_name IN ('store_referral_aws')
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY game_id, iutm_medium, fdate, fcountry, ident_id, utm_source
	)
	union all
		(SELECT 
		game_id, 
		'Downloads from stores' as kpi, 
		utm_medium,
		SUM(event_count) as event_count,
		DATE(client_time) as fdate,
		country_name as fcountry,
		CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
		utm_source
	FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
	WHERE 1=1 
		 
		AND event_id = 393584 
		AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
		AND DATE(client_time) >=  '2025-02-01'
		AND version IN ('1.0.0')
	GROUP BY game_id, utm_medium, fdate, fcountry, ident_id, utm_source
	 
	)
	 union all
	 (SELECT 
    game_id,
    'Downloads from App' as kpi,
    utm_medium,
    COUNT(DISTINCT user_id) as event_count,
    DATE(client_time) as fdate,
    country_name as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM (
    SELECT t.*, e.name as country_name
    FROM (
        (SELECT user_id, game_id, client_time, country, utm_medium, utm_content, utm_campaign, utm_source, referral_type 
         FROM `gcp-bi-elephant-db-gold.applaydu.custom_install_referral` 
         WHERE 1=1 AND DATE(client_time) >=  '2025-02-01')
        UNION ALL
        (SELECT user_id, game_id, client_time, country, utm_medium, utm_content, utm_campaign, utm_source, 'N/A' as referral_type 
         FROM `gcp-bi-elephant-db-gold.applaydu.install_referral` 
         WHERE 1=1 AND DATE(client_time) >=  '2025-02-01')
    ) t
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.country` e 
    ON t.country = e.code
)
WHERE 1=1  AND DATE(client_time) >=  '2025-02-01'
GROUP BY game_id, utm_medium, fdate, fcountry, ident_id, utm_source
	 ) 
	 
	 union all
	 (SELECT
    game_id,
    'QR Impressions' as kpi,
    utm_medium,
    SUM(event_count) as event_count,
    DATE(client_time) as fdate,
    COALESCE(country_name, 'Undefined') as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats_qr_impressions`
WHERE 1=1 
     
    AND kpi_name = 'QR Impressions'
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY game_id, fdate, fcountry, ident_id, utm_source, utm_medium
	 
	 )
	 
	 
	 union all
		 (SELECT 
    81335 as game_id, 
    'InstallReferral' as kpi,
    utm_medium,
    SUM(event_count) as event_count,
    DATE(client_time) as fdate,
    country_name as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 1=1 
     
    AND event_id = 393584 
    AND game_id = 81335
    AND kpi_name IN ('UTM App Units', 'UTM First-Time Downloads') 
    AND country_name <> 'All Countries'
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY utm_medium, fdate, fcountry, ident_id, utm_source
	 )
	 union all
	(SELECT 
    81335 as game_id, 
    'InstallReferral' as kpi,
    utm_medium,
    SUM(CASE WHEN country_name = 'All Countries' THEN event_count ELSE 0 END) - SUM(CASE WHEN country_name <> 'All Countries' THEN event_count ELSE 0 END) as event_count,
    DATE(client_time) as fdate,
    'Undefined' as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 1=1 
     
    AND event_id = 393584 
    AND game_id = 81335
    AND kpi_name IN ('UTM First-Time Downloads')
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY utm_medium, fdate, fcountry, ident_id, utm_source
	 )
	 union all
	 (SELECT 
    81337 as game_id, 
    'InstallReferral' as kpi,
    utm_medium,
    SUM(event_count) as event_count,
    DATE(client_time) as fdate,
    country_name as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 1=1 
     
    AND event_id = 393584 
    AND game_id = 81337
    AND kpi_name IN ('UTM GP Installers')
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY utm_medium, fdate, fcountry, ident_id, utm_source
	 ) 
	union all
	(SELECT 
    game_id, 
    kpi_name as kpi,
    'N/A' as utm_medium,
    SUM(event_count) as event_count,
    DATE(client_time) as fdate,
    'Undefined' as fcountry,
    'N/A' as ident_id,
    'N/A' as utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 1=1 
     
    AND event_id = 393584 
    AND game_id IN (81335, 81337)
    AND kpi_name IN (
        'App Units by Source Type - App Referrer', 
        'App Units by Source Type - App Store Browse', 
        'App Units by Source Type - App Store Search', 
        'App Units by Source Type - Web Referrer', 
        'GP Store listing acquisitions - All traffic sources', 
        'GP Store listing acquisitions - Google Play search', 
        'GP Store listing acquisitions - Third-party referrals', 
        'GP Store listing acquisitions - Google Play explore'
    )
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY utm_medium, fdate, fcountry, ident_id, utm_source, kpi_name, game_id
	)
				
	union all
	(SELECT 
    82471 as game_id, 
    'InstallReferral' as kpi,
    utm_medium,
    SUM(event_count) as event_count,
    DATE(client_time) as fdate,
    country_name as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 1=1 
     
    AND event_id = 393584 
    AND game_id = 82471
    AND kpi_name IN ('AppInChina Clicks')
    AND DATE(client_time) >=  '2025-02-01'
    AND version IN ('1.0.0')
GROUP BY utm_medium, fdate, fcountry, ident_id, utm_source
	 ) 
				
	 union all
	(SELECT 
    game_id, 
    'InstallReferral in App' as kpi,
    utm_medium,
    COUNT(DISTINCT user_id) as event_count,
    DATE(client_time) as fdate,
    country_name as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM (
    SELECT t.*, e.name as country_name
    FROM (
        (SELECT user_id, game_id, client_time, country, utm_medium, utm_content, utm_campaign, utm_source, referral_type 
         FROM `gcp-bi-elephant-db-gold.applaydu.custom_install_referral` 
         WHERE 1=1 AND DATE(client_time) >=  '2025-02-01')
        UNION ALL
        (SELECT user_id, game_id, client_time, country, utm_medium, utm_content, utm_campaign, utm_source, 'N/A' as referral_type 
         FROM `gcp-bi-elephant-db-gold.applaydu.install_referral` 
         WHERE 1=1 AND DATE(client_time) >=  '2025-02-01')
    ) t
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.country` e 
    ON t.country = e.code
)
WHERE 1=1 
     
    AND (game_id IN (81337, 82471) AND referral_type IN ('STORE_NO_FINGERPRINT', 'MISMATCH_STORE_FINGERPRINT', 'MATCH_STORE_FINGERPRINT', 'FINGERPRINT_NO_STORE'))
    AND utm_medium NOT IN ('', 'NULL', 'N/A', '(not%20set)', 'organic')
    AND DATE(client_time) >=  '2025-02-01'
GROUP BY utm_medium, fdate, fcountry, ident_id, utm_source, game_id
	 ) 
	 union all
	 (SELECT 
    game_id, 
    'Matching safebox' as kpi,
    utm_medium,
    COUNT(DISTINCT user_id) as event_count,
    DATE(client_time) as fdate,
    country_name as fcountry,
    CASE WHEN utm_content NOT IN ('', 'NULL', 'N/A') THEN utm_content ELSE utm_campaign END as ident_id,
    utm_source
FROM (
    SELECT t.*, e.name as country_name
    FROM (
        (SELECT user_id, game_id, client_time, country, utm_medium, utm_content, utm_campaign, utm_source, referral_type 
         FROM `gcp-bi-elephant-db-gold.applaydu.custom_install_referral` 
         WHERE 1=1 AND DATE(client_time) >=  '2025-02-01')
        UNION ALL
        (SELECT user_id, game_id, client_time, country, utm_medium, utm_content, utm_campaign, utm_source, 'N/A' as referral_type 
         FROM `gcp-bi-elephant-db-gold.applaydu.install_referral` 
         WHERE 1=1 AND DATE(client_time) >=  '2025-02-01')
    ) t
    LEFT JOIN `gcp-bi-elephant-db-gold.dimensions.country` e 
    ON t.country = e.code
)
WHERE 1=1 
     
    AND referral_type IN ('MISMATCH_STORE_FINGERPRINT', 'MATCH_STORE_FINGERPRINT', 'FINGERPRINT_NO_STORE')
    AND utm_medium NOT IN ('', 'NULL', 'N/A', '(not%20set)', 'organic')
    AND DATE(client_time) >=  '2025-02-01'
GROUP BY game_id, utm_medium, fdate, fcountry, ident_id, utm_source
		
	 )
	 
	
)
where DATE(fdate) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by game_id
	, kpi
	, utm_medium
	, fcountry
	, ident_id
	, utm_source
	, fDate 
	