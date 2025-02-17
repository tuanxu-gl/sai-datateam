DECLARE r319_q4227 ARRAY<STRUCT<`Scan type` STRING,`total_scan` INT64>>;

DECLARE row_count FLOAT64;
SET row_count = (
  SELECT COUNT(0) 
  FROM `applaydu.apd_report_319`
  WHERE 1=1 
    AND DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
    AND dashboard_id = 319 
    AND query_id = 4227
);

IF row_count = 0 THEN
  SET r319_q4227 = (
    SELECT ARRAY(
      with gb4227 as (SELECT 0)
,v_scan_mode_finished_vr as
(
    (
    SELECT user_id
        ,game_id
        ,date(server_date)
        ,version
        ,t.country_name
        ,'New_Toy' as scan_result
        ,coalesce(toy_name,'Undefined') as toy_name
        ,coalesce(toy_detected ,'Undefined') as toy_detected
        ,'Scan_Toy' as scan_type
        ,count(0) as event_count
    from `gcp-gfb-sai-tracking-gold.applaydu.tbl_scan_mode_finished_24x` t
        join `applaydu.tbl_country_vr`using (country)
    where (DATE(server_date) >= DATE('2021-01-06') and DATE(server_date) < date_sub(current_date(), interval 3 day))
        and date(server_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(server_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{icountry}}]] [[and {{iregion}}]])
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{inotcountry}}]]) 
        and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1  [[and {{iversion}}]])
        and game_id in (SELECT game_id from `applaydu.tbl_shop_filter` where 1=1  [[and {{ishopfilter}}]])
        and server_date >= `applaydu.tbl_country_vr`.start_date
        and user_id is not null
        and total_scan > 0 and visenze_new_toy_count>0
    group by user_id
        ,game_id
        ,server_date
        ,version
        ,t.country_name
        ,scan_result
        ,toy_name
        ,toy_detected 
        ,scan_type
    )
    union all
    (
    SELECT user_id
        ,game_id
        ,date(server_date)
        ,version
        ,t.country_name
        ,'Old_Toy' as scan_result
        ,coalesce(toy_name,'Undefined') as toy_name
        ,coalesce(toy_detected ,'Undefined') as toy_detected
        ,'Scan_Toy' as scan_type
        ,sum(case when visenze_new_toy_count>0 then (total_scan-1) else total_scan end) as event_count
    from `gcp-gfb-sai-tracking-gold.applaydu.tbl_scan_mode_finished_24x` t
        join `applaydu.tbl_country_vr` on `applaydu.tbl_country_vr`.country = t.country
    where (date(server_date) >= date('2021-01-06') and date(server_date) < date_sub(current_date(), interval 3 day))
        and date(server_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(server_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{icountry}}]] [[and {{iregion}}]])
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{inotcountry}}]]) 
        and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1  [[and {{iversion}}]]) 
        and game_id in (SELECT game_id from `applaydu.tbl_shop_filter` where 1=1  [[and {{ishopfilter}}]])
        and server_date >= `applaydu.tbl_country_vr`.start_date
        and user_id is not null
        and total_scan > 1 
    group by user_id
        ,game_id
        ,server_date
        ,version
        ,t.country_name
        ,scan_result
        ,toy_name
        ,toy_detected 
        ,scan_type
    )
    union all
    (
    SELECT t.user_id
      ,t.game_id
      ,date(t.client_time) as server_date
      ,t.version
      ,`applaydu.tbl_country_vr`.country_name as country_name
      ,t.scan_result
      ,coalesce(t.toy_name,'Undefined') as toy_name
      ,case when t.scan_type = 'Scan_Toy' and version in ('2.0.1','2.0.2','2.0.4','2.0.7','2.0.8','2.0.9','2.2.0','2.2.1','2.2.2','2.2.3','2.3.0','2.3.1','2.4.3','2.5.0','2.6.0','2.6.1','2.6.2','2.6.3','2.7.0','2.7.1','2.7.2','2.7.3','3.0.0','3.0.1','3.0.2','3.0.3','3.0.4','3.0.5','3.0.6','3.0.7')
        then coalesce(upper(t.toy_detected),'Undefined') 
        else 
            (case when t.reference is null or  t.reference = 'N/A' then 'Undefined' else coalesce(upper(regexp_substr(t.reference, '[^/]*$')),'Undefined') end)
        end as toy_detected
      ,case when t.toy_detected like '%_leftover' and t.reference not like 'http%' and t.version in ('3.1.0','3.1.2','3.2.0','3.2.1') then 'Scan_Toy' else scan_type end as scan_type
      ,count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished` t
        join `applaydu.tbl_country_vr` on `applaydu.tbl_country_vr`.country = t.country
    where  (date(client_time) >= date('2021-01-06') and date(client_time) < date_sub(current_date(), interval 3 day))
        and date(client_time) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(client_time) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{icountry}}]] [[and {{iregion}}]])
        and t.country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{inotcountry}}]]) 
        and version in (SELECT version from `applaydu.tbl_version_filter` where 1=1  [[and {{iversion}}]]) 
        and game_id in (SELECT game_id from `applaydu.tbl_shop_filter` where 1=1  [[and {{ishopfilter}}]])
        and date(client_time) >= date(`applaydu.tbl_country_vr`.start_date)
        and t.user_id is not null
        and scan_result in ('New_Toy','Old_Toy')
    group by t.user_id
      ,t.game_id
      ,server_date
      ,t.version
      ,country_name
      ,t.scan_result
      ,t.reference
      ,t.toy_name
      ,t.toy_detected
      ,t.scan_type
    )
)
--main query
SELECT AS STRUCT * from
(
SELECT case when scan_type in ('Scan_Toy') then 'Scan Toy' else 'Scan Leaflet' end as `Scan type`,
    sum(event_count) as total_scan
from   v_scan_mode_finished_vr
    join (
        SELECT distinct user_id 
        from `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        where 1=1 
        and install_source in (SELECT ua_filter from `applaydu.tbl_ua_filter` where 1=1  [[and {{iinstallsource}}]])
        and date(active_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(active_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
    ) using (user_id)
where scan_type in ('Scan_Toy','Alternative_Vignette','Scan_QR','Scan_Vignette')
group by 1
union all
SELECT 'Deeplink' as `Scan type`
,count(0) as `Total scans`
from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    join (
        SELECT distinct user_id 
        from `gcp-bi-elephant-db-gold.applaydu.user_activity` 
        where 1=1 
        and install_source in (SELECT ua_filter from `applaydu.tbl_ua_filter` where 1=1  [[and {{iinstallsource}}]])
        and date(active_date) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(active_date) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
    ) using (user_id)
where unlock_cause in ('Deep_Link')
    and date(client_time) >= date('2020-08-10') and date(client_time) < date_sub(current_date(), interval 3 day)
        and date(client_time) >= (SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] )
        and date(client_time) < date_add((SELECT max(server_date) from `applaydu.tbl_date_filter` where 1=1 [[and {{idate}}]] ), interval 1 day)
        and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{icountry}}]] [[and {{iregion}}]])
        and country in (SELECT country from `applaydu.tbl_country_filter` where 1=1  [[and {{inotcountry}}]])
        and game_id in (SELECT game_id from `applaydu.tbl_shop_filter` where 1=1  [[and {{ishopfilter}}]])
)
    )
  );
  
ELSE
  SET r319_q4227 = (
    SELECT ARRAY_AGG(
      STRUCT(
         CAST(value1_str as STRING) as `Scan type`, CAST(value2 as INT64) as `total_scan`
      )
    )
    FROM 
      `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
    WHERE 
      DATE(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
      AND DATE(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND ((SELECT COUNT(0) FROM `applaydu.tbl_country_filter`) = (SELECT COUNT(0) FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]]))
      AND dashboard_id = 319 
      AND query_id = 4227 
  );
END IF;

SELECT * FROM UNNEST(r319_q4227);
