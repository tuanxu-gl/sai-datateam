with r4413 as(
SELECT value1_str as grass_month,value2_str as starting_persona,value3_str as persona,value4 as user_cnt
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 4413 
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
		 and dashboard_id=14 and query_id = 4413
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, dim_date as (
    select server_date as grass_date
    from APPLAYDU_NOT_CERTIFIED.tbl_date_filter
    where day(server_date) = 1
    and server_date >= '2023-01-01'
)
, dim_user as (
    select user_id, min(to_date(client_time)) first_date
    from ELEPHANT_DB.APPLAYDU.LAUNCH_RESUME l
    where l.user_id is not null
    and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='persona_starting_date') 
	  and country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]])
    group by 1
)
, dim_user_date as (
    select u.user_id, d.grass_date
    from dim_user u
    join dim_date d
        on d.grass_date >= u.first_date
)
, unlock as (
    SELECT to_date(client_time) client_date
    , user_id
    , count(1) as toy_unlock_cnt
    FROM "ELEPHANT_DB"."APPLAYDU"."TOY_UNLOCKED"
    WHERE (("APPLAYDU"."TOY_UNLOCKED"."UNLOCK_CAUSE" = 'QR Code'
         OR "APPLAYDU"."TOY_UNLOCKED"."UNLOCK_CAUSE" = 'Toy Scan' 
         OR "APPLAYDU"."TOY_UNLOCKED"."UNLOCK_CAUSE" = 'Deep_Link')
        AND "APPLAYDU"."TOY_UNLOCKED"."ISNEWTOY" = 1)
        and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='persona_starting_date') 
	  and country in (select country from tbl_country_filter where 2=2 [[AND {{icountry}}]])
    group by 1,2
)
, lifetime_unlock as (
    SELECT d.grass_date, d.user_id, sum(toy_unlock_cnt) toy_unlock_cnt
    from dim_user_date d
    left join unlock u
        on d.user_id = u.user_id
        and u.client_date < d.grass_date
    group by 1,2
)
,persona as (
    select grass_date as grass_month, user_id
        , case
                when toy_unlock_cnt is null or toy_unlock_cnt = 0 then 'P1'
                when toy_unlock_cnt in (1,2,3) then 'P2'
                when toy_unlock_cnt > 3 then 'P3'
            end as persona
        , toy_unlock_cnt
    from lifetime_unlock
)
, mau as (
    select distinct user_id
        , country
        , date_from_parts(year(to_date(client_time)),month(to_date(client_time)),1) grass_month
        , min(date_from_parts(year(to_date(client_time)),month(to_date(client_time)),1)) over(partition by user_id) first_month
    from ELEPHANT_DB.APPLAYDU.LAUNCH_RESUME t
    WHERE user_id is not null
      and not (game_id = 82471 and CLIENT_TIME <'2020-12-14')
      and t.country in (select country from applaydu_not_certified.tbl_country_filter)
	  and version in (select version from applaydu_not_certified.tbl_version_filter)
	  and country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]])
)
, final_agg as (
select m.grass_month
    , coalesce(p.persona,'P1') persona
    , count(distinct m.user_id) user_cnt
from mau m
left join persona p
    on m.user_id = p.user_id
    and m.grass_month = dateadd(month,-1,p.grass_month)
    -- and m.grass_month = p.grass_month
where m.grass_month >= '2023-01-01'
group by 1,2
)
, final_user as (
select m.grass_month
    , m.first_month
    , m.user_id
    , coalesce(p1.persona,'P1') month_end_persona
    , coalesce(p2.persona,'P1') month_start_persona
from mau m
left join persona p1
    on m.user_id = p1.user_id
    and m.grass_month = dateadd(month,-1,p1.grass_month)
    -- and m.grass_month = p.grass_month
left join persona p2
    on m.user_id = p2.user_id
    and m.grass_month = p2.grass_month
)

select * from r4413
union
select * from
(

    select grass_month
        , case
            when grass_month = first_month then 'New-in-month'
            when month_start_persona in ('P1','P2') then 'P1,P2 inflow'
            else 'Existing P3'
        end as starting_persona
        , month_end_persona persona
        , count(distinct user_id) user_cnt
    from final_user
    where month_end_persona = 'P3'
    group by 1,2,3
)
where grass_month > 0
