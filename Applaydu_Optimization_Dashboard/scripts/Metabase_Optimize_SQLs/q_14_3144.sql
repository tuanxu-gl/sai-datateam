with r3144 as(
SELECT value1_str as "Environment",value2 as "Percentage"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 3144 
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
        WHERE 1=1
        AND start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
		 and dashboard_id=14 and query_id = 3144
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q3144 as (select 0),
result as (
SELECT  case when environment_id like 'Natoons v4%' then 'Natoons Experience'
            when environment_id like '%Travel%' then 'Travel Experience'
            when environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') then 'Fantasy Experience'
            when environment_id like '%Space%' and environment_id <> 'Space' then 'Space Experience'
            when environment_id = 'Experience - Dino Museum' then 'Dino Experience - since v4.7.0'
            when environment_id = 'Eduland Lets Story' then 'Lets Story'
            when environment_id = 'Kinderini' then 'Kinderini'
            end as Environment
    , USER_ID
from APPLAYDU.STORY_MODE_TRIGGERED
where  1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (select min(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{from_version}}]]) and version <= (select max(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{to_version}}]])
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
    and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
union 
SELECT  case when environment_id like 'Natoons v4%' then 'Natoons Experience'
            when environment_id like '%Travel%' then 'Travel Experience'
            when environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') then 'Fantasy Experience'
            when environment_id like '%Space%' and environment_id <> 'Space' then 'Space Experience'
            when environment_id = 'Experience - Dino Museum' then 'Dino Experience - since v4.7.0'
            when environment_id = 'Eduland Lets Story' then 'Lets Story'
            when environment_id = 'Kinderini' then 'Kinderini'
            end as Environment
    , USER_ID
from APPLAYDU.STORY_MODE_FINISHED
where  1=1
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and client_time < CURRENT_DATE()
    and version >= (select min(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{from_version}}]]) and version <= (select max(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{to_version}}]])
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
    and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
)
,
active_user as (
SELECT COUNT(DISTINCT USER_ID)AS Total_Users
from APPLAYDU.LAUNCH_RESUME t
    join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country = t.country 
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'
    and time_spent::float >= 0	and time_spent::float < 86400
    and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and CLIENT_TIME < CURRENT_DATE()
    and version >= (select min(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{from_version}}]]) and version <= (select max(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{to_version}}]])
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
    and CLIENT_TIME < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and t.country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
    and not(t.game_id = 82471 and client_time <'2020-12-14')
) 

select * from r3144
union
select * from
(

select  Environment as "Environment", count (distinct USER_ID) /(select Total_Users from active_user) as "Percentage"
from result
where Environment is not null
group by 1
order by 2 desc
)
where "Environment" > 0
