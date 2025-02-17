from APPLAYDU.PARENTAL_SECTION
    where 1=1
        and version >= '4.3.0' and client_time >= '2024-02-05' and version < '9.0.0' and server_time < dateadd(day, -3, CURRENT_DATE())
        and version >= (select min(version) from tbl_version_filter where 1=1  [[AND {{from_version}}]]) 
        and version <= (select max(version) from tbl_version_filter where 1=1  [[AND {{to_version}}]])
        and version in (select version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
        and server_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] ) 
        and server_time < dateadd(day, 1, (SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] ))
        and country in (select country from tbl_country_filter where 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])
        
)
select * from r3180
union
select * from (
select 'Dedicated Experience' as feature, (select count(distinct user_id) from dedicate) as Users, Users / (select count(distinct user_id) from launch_resume) as "% User Access the Feature"
union
select 'AR' as feature, (select count(distinct user_id) from ar_mode) as Users, Users / (select count(distinct user_id) from launch_resume) as "% User Access the Feature"
union
select 'Minigame' as feature, (select count(distinct user_id) from minigame) as Users, Users / (select count(distinct user_id) from launch_resume) as "% User Access the Feature"
union
select 'Toy Friendship' as feature, (select count(distinct user_id) from tbl_toy_friendship) as Users, Users / (select count(distinct user_id) from launch_resume) as "% User Access the Feature"
union
select 'Avatar House' as feature, (select count(distinct user_id) from tbl_avatar) as Users, Users / (select count(distinct user_id) from launch_resume) as "% User Access the Feature"
union
select 'Parental' as feature, (select count(distinct user_id) from parental_section) as Users, Users / (select count(distinct user_id) from launch_resume) as "% User Access the Feature"
order by 2 desc)
where Users > 0