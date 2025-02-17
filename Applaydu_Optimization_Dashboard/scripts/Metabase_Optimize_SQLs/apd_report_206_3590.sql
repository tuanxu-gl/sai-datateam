insert into APPLAYDU_NOT_CERTIFIED.apd_report_206 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2_str,value3_str,value4)
WITH 
tbl_ar_activity_finished as (
select *
FROM APPLAYDU.AR_ACTIVITY_FINISHED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') 
    and ar_experience_name = 'VisenzeGame' 
    and UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons') 
    and version >= '4.0.0' and version < '9.0.0' 
    and client_time < dateadd(day, -3, CURRENT_DATE())
    and country in (select country from tbl_country_filter where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')  [[AND {{icountry}}]] [[AND {{iregion}}]])  
)
-- === NATOONS CUBS toy USERS ===
,scan_toy_natoons_cub_users as (
select user_id, UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$')) as PHYSICAL_MPG_CODE
from applaydu.scan_mode_finished
where  1=1 
    and UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$'))  in (select PHYSICAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
    and scan_type in ('Scan_Toy'/*, 'Scan_QR', 'Deep Link'*/)
    and scan_result in ('New_Toy','Old_Toy')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
union 
select user_id user_id, UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) as PHYSICAL_MPG_CODE
from APPLAYDU.TOY_UNLOCKED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and unlock_cause in ('Toy Scan'/*, 'QR Code', 'Deep_Link'*/) and ISNEWTOY = 1
    and UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
)
-- === NATOONS CUBS total USERS ===
,scan_natoons_cub_users as (
select user_id, UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$')) as PHYSICAL_MPG_CODE
from applaydu.scan_mode_finished
where  1=1 
    and UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$'))  in (select PHYSICAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
    and scan_type in ('Scan_Toy', 'Scan_QR', 'Deep Link')
    and scan_result in ('New_Toy','Old_Toy')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
union 
select user_id user_id, UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) as PHYSICAL_MPG_CODE
from APPLAYDU.TOY_UNLOCKED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and unlock_cause in ('Toy Scan', 'QR Code', 'Deep_Link') and ISNEWTOY = 1
    and UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
)
-- === NATOONS toy USERS (NOT INCLUDED CUBS) ===
,scan_toy_natoons_users as (
select user_id, UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$')) as PHYSICAL_MPG_CODE
from applaydu.scan_mode_finished
where  1=1 
    and UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$'))  in (select PHYSICAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory <> 'Natoons_CUBS')
    and scan_type in ('Scan_Toy'/*, 'Scan_QR', 'Deep Link'*/)
    and scan_result in ('New_Toy','Old_Toy')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
union 
select user_id user_id, UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) as PHYSICAL_MPG_CODE
from APPLAYDU.TOY_UNLOCKED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and unlock_cause in ('Toy Scan'/*,'QR Code',  'Deep_Link'*/) and ISNEWTOY = 1
    and UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory <> 'Natoons_CUBS')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
)
-- === NATOONS total USERS (NOT INCLUDED CUBS) ===
,scan_natoons_users as (
select user_id, UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$')) as PHYSICAL_MPG_CODE
from applaydu.scan_mode_finished
where  1=1 
    and UPPER(REGEXP_SUBSTR(REFERENCE, '[^\/]*$'))  in (select PHYSICAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory <> 'Natoons_CUBS')
    and scan_type in ('Scan_Toy', 'Scan_QR', 'Deep Link')
    and scan_result in ('New_Toy','Old_Toy')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
union 
select user_id user_id, UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) as PHYSICAL_MPG_CODE
from APPLAYDU.TOY_UNLOCKED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and unlock_cause in ('Toy Scan','QR Code',  'Deep_Link') and ISNEWTOY = 1
    and UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory <> 'Natoons_CUBS')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
)
select 'Total in scope scanned users' as "#"
    ,'Natoon Animal' as "Natoon type" 
    ,count (distinct user_id) as users
from APPLAYDU.LAUNCH_RESUME
inner join scan_natoons_users using (user_id)
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
    and country in (select country from tbl_country_filter where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')  [[AND {{icountry}}]] [[AND {{iregion}}]])  
    and time_spent::float >= 0 and time_spent::float < 86400
union    
select 'Total in scope scanned users' as "#"
    ,'Natoon Cub' as "Natoon type" 
    ,count (distinct user_id) as users
from APPLAYDU.LAUNCH_RESUME
inner join scan_natoons_cub_users using (user_id)
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
    and country in (select country from tbl_country_filter where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')  [[AND {{icountry}}]] [[AND {{iregion}}]])  
    and time_spent::float >= 0 and time_spent::float < 86400
union all   
select 'Scanned toy users'  as "#"
    ,'Natoon Animal' as "Natoon type" 
    ,count (distinct user_id) as users
from APPLAYDU.LAUNCH_RESUME
inner join scan_toy_natoons_users using (user_id)
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
    and country in (select country from tbl_country_filter where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')  [[AND {{icountry}}]] [[AND {{iregion}}]])  
    and time_spent::float >= 0 and time_spent::float < 86400
union    
select 'Scanned toy users' as "#"
    ,'Natoon Cub' as "Natoon type" 
    ,count (distinct user_id) as users
from APPLAYDU.LAUNCH_RESUME
inner join scan_toy_natoons_cub_users using (user_id)
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and version >= '4.0.0' and version < '9.0.0' and client_time < dateadd(day, -3, CURRENT_DATE())
    and country in (select country from tbl_country_filter where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')  [[AND {{icountry}}]] [[AND {{iregion}}]])  
    and time_spent::float >= 0 and time_spent::float < 86400
union all   
select 'Total users start' as "#"
    ,case when UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
        then 'Natoon Cub' else 'Natoon Animal' end as "Natoon type",
    count (distinct USER_ID) as users 
from tbl_ar_activity_finished t
where activity_01 = 'AR - VisenzeGame - End Cause' 
group by 1,2     
  union all
select 
'Complete the set-up phase' as "#", case when UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
    then 'Natoon Cub' else 'Natoon Animal' end as "Natoon type",
count (distinct USER_ID) as users
from tbl_ar_activity_finished t
where activity_01 = 'AR - VisenzeGame - End Cause' 
and activity_01_value >= 3
group by 1,2 
union all
select 
'Complete Level 1' as "#",
    case when UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
        then 'Natoon Cub' else 'Natoon Animal' end as "Natoon type",
    count (distinct USER_ID) as users 
from tbl_ar_activity_finished t
where activity_05 = 'AR - VisenzeGame - Completed Level' 
    and activity_05_value >= 1
group by 1,2     
union all 
select 
'Complete Level 2' as "#",
    case when UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
        then 'Natoon Cub' else 'Natoon Animal' end as "Natoon type",
    count (distinct USER_ID) as users 
from tbl_ar_activity_finished t
where activity_05 = 'AR - VisenzeGame - Completed Level' 
    and activity_05_value >= 2
group by 1,2      
union all 
select 
'Complete Level 3' as "#",
    case when UPPER( RIGHT(TOY_NAME, CHARINDEX('_', reverse(toy_name)) -1 )) in (select DIGITAL_MPG_CODE from applaydu_not_certified.gdd_toy_list_august_24 where Eduland_Attached = 'Natoons' and eduland_subcategory = 'Natoons_CUBS')
        then 'Natoon Cub' else 'Natoon Animal' end as "Natoon type",
    count (distinct USER_ID) as users 
from tbl_ar_activity_finished t
where activity_05 = 'AR - VisenzeGame - Completed Level' 
    and activity_05_value >= 3
group by 1,2      
  order by 3 desc