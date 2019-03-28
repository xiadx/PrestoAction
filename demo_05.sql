--时长 https://mdp.mafengwo.cn/mql?sid=533e84bc-f268-4a08-ac45-1f46f29fae1e&engine=PRESTO
select
    dt
    ,round(avg(sum_duration),2) as avg_duration
from
(
    select
        inside.dt as dt
        ,inside.open_udid as open_udid
        ,case when out.duration is null then (duration_total) else (out.duration+duration_total) end as sum_duration
    from
    (
        select
            dt
            ,device_id as open_udid
            ,sum(duration) as duration_total
        from
            mfw_dwd.fact_flow_mobile_hpinfo_event_increment
        where
            dt between '20190317' and '20190320'
            and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
            and app_ver>='9.3.0'
            and event_code = 'home_article_time' 
            and channel_id=55
            and element_at(basic_abtest,'app_home_change')='a'
        group by
            dt 
            ,device_id
        having sum(duration)>=0.1 and sum(duration)<=10800
    ) inside
    left join 
    (
        select 
            dt
            ,device_id as open_udid
            ,sum(duration) as duration
        from
            mfw_dwd.fact_flow_mobile_page_event_increment
        where
            dt between '20190317' and '20190320'
            and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
            and app_ver>='9.3.0'
            and page_name<>'目的地详情页'
            and parent_name='首页'
            and trigger_point in ('信息流','首页信息流_55')
            and url_extract_parameter(parent_uri,'channel_id')='55'
        group by
            dt
            ,device_id
        having sum(duration)>=0.1 and sum(duration)<=10800
    ) out
    on 
        inside.dt=out.dt
        and inside.open_udid=out.open_udid
    group by 
        inside.dt
        ,inside.open_udid
        ,case when out.duration is null then (duration_total) else (out.duration+duration_total) end
)
group by
    dt
limit 100
