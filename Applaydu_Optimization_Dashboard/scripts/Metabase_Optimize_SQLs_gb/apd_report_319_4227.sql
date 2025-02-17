insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3_str,value4,value5,value6,value7_str,value8_str,value9_str)
with gb4227 as (select 0)
,v_scan_mode_finished_vr as
(
  (
  select user_id
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
  where (date(server_date)>=date('2021-01-06') and date(server_date)<date_sub(current_date(), interval 3 day))
    and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2  )
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2 ) 
    and version in (select version from `applaydu.tbl_version_filter` where 2=2 )
    and game_id in (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
    and server_date>=`applaydu.tbl_country_vr`.start_date
    and user_id is not null
    and total_scan>0 and visenze_new_toy_count>0
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
  select user_id
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
    join `applaydu.tbl_country_vr` on `applaydu.tbl_country_vr`.country=t.country
  where (date(server_date)>=date('2021-01-06') and date(server_date)<date_sub(current_date(), interval 3 day))
    and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2  )
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2 ) 
    and version in (select version from `applaydu.tbl_version_filter` where 2=2 ) 
    and game_id in (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
    and server_date>=`applaydu.tbl_country_vr`.start_date
    and user_id is not null
    and total_scan>1 
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
  select t.user_id
   ,t.game_id
   ,date(t.client_time) as server_date
   ,t.version
   ,`applaydu.tbl_country_vr`.country_name as country_name
   ,t.scan_result
   ,coalesce(t.toy_name,'Undefined') as toy_name
   ,case when t.scan_type='Scan_Toy' and version in ('2.0.1','2.0.2','2.0.4','2.0.7','2.0.8','2.0.9','2.2.0','2.2.1','2.2.2','2.2.3','2.3.0','2.3.1','2.4.3','2.5.0','2.6.0','2.6.1','2.6.2','2.6.3','2.7.0','2.7.1','2.7.2','2.7.3','3.0.0','3.0.1','3.0.2','3.0.3','3.0.4','3.0.5','3.0.6','3.0.7')
    then coalesce(upper(t.toy_detected),'Undefined') 
    else 
      (case when t.reference is null or t.reference='N/A' then 'Undefined' else coalesce(upper(regexp_substr(t.reference, '[^/]*$')),'Undefined') end)
    end as toy_detected
   ,case when t.toy_detected like '%_leftover' and t.reference not like 'http%' and t.version in ('3.1.0','3.1.2','3.2.0','3.2.1') then 'Scan_Toy' else scan_type end as scan_type
   ,count(0) as event_count
  from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished` t
    join `applaydu.tbl_country_vr` on `applaydu.tbl_country_vr`.country=t.country
  where (date(client_time)>=date('2021-01-06') and date(client_time)<date_sub(current_date(), interval 3 day))
    and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2  )
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2 ) 
    and version in (select version from `applaydu.tbl_version_filter` where 2=2 ) 
    and game_id in (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
    and date(client_time)>=date(`applaydu.tbl_country_vr`.start_date)
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

select 319 as dashboard_id
		,4227 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Number of mau, scans & ratio' as kpi_name
		,`year` as value1,`month` as value2,`Time` as value3_str,`Users` as value4,`Scanned Users` as value5,`Scan users ratio` as value6,`Average Time per Sessions` as value7_str,`Average Time per scanned user` as value8_str,`Average Time per NOT scanned user` as value9_str
	from
	(
	
select * from
(
select case when scan_type in ('Scan_Toy') then 'Scan Toy' else 'Scan Leaflet' end as `Scan type`,
  sum(event_count) as total_scan
from  v_scan_mode_finished_vr
  join (
    select distinct user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source in (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
    and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
  ) using (user_id)
where scan_type in ('Scan_Toy','Alternative_Vignette','Scan_QR','Scan_Vignette')
group by 1
union all
select 'Deeplink' as `Scan type`
,count(0) as `Total scans`
from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
  join (
    select distinct user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source in (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
    and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
  ) using (user_id)
where unlock_cause in ('Deep_Link')
  and date(client_time)>=date('2020-08-10') and date(client_time)<date_sub(current_date(), interval 3 day)
    and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
    and country in (select country from `applaydu.tbl_country_filter` where 2=2  )
    and country in (select country from `applaydu.tbl_country_filter` where 2=2 )
    and game_id in (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
)
)