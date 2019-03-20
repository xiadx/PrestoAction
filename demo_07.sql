--总体指标统计 //mdp.mafengwo.cn/mql?sid=e2b82258-fa23-437f-8c9a-51e553fda4c4&engine=PRESTO
--首页曝光
with page_uv as
(
select
    dt
    ,count(distinct device_id) as uv
from
    mfw_dwd.fact_flow_mobile_page_event_increment
where
    dt between '20190317' and '20190320'
    and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
    and app_ver>='9.3.0'
    and element_at(abtest,'app_home_change')='a'
    and page_name='首页'
group by
    dt
),

--信息流曝光
show as(
select
    dt
    ,count(distinct device_id) as uv
    ,count(1) as pv
from
    mfw_dwd.fact_flow_mobile_hpinfo_event_increment
where
    dt between '20190317' and '20190320'
    and event_code = 'home_article_list_show'
    and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
    and app_ver>='9.3.0'
    and channel_id=55
    and element_at(basic_abtest,'app_home_change')='a'
group by 
    dt
),

--信息流点击
click as(
select 
    dt
    ,count(distinct device_id) as uv
    ,count(1) as pv
from
    mfw_dwd.fact_flow_mobile_hpinfo_event_increment
where
    dt between '20190317' and '20190320'
    and event_code = 'home_article_list_click'
    and app_ver>='9.3.0'
    and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
    and channel_id=55
    and element_at(basic_abtest,'app_home_change')='a'
group by  
    dt
),

--互动行为
act as
(
    select
        dt
        ,count(distinct device_id) as uv
        ,sum(pv) as pv
    from
    (
        (
            select
                dt
                ,device_id
                ,count(1) as pv
            from
                mfw_dwd.fact_flow_mobile_hpinfo_event_increment
            where
                dt between '20190317' and '20190320'
                and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
                and app_ver>='9.3.0'
                and event_code='weng_click'
                and page_name='首页'
                and url_extract_parameter(uri,'channel_id')='55'
                and module_name in ('share','favorite','comment','collect')
                and element_at(basic_abtest,'app_home_change')='a'
            group by 
                dt
                ,device_id
        ) 
        union all
        (
            select
                dt
                ,device_id
                ,count(1) as pv
            from
                mfw_dwd.fact_flow_mobile_sc_event_increment
            where
                dt between '20190317' and '20190320'
                and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
                and app_ver>='9.3.0'
                and event_code in ('click_weng_detail','click_video_detail')
                and url_extract_parameter(parent_uri,'channel_id')='55'
                and item_source in ('share','fav','collect','reply','reply_icon','reply_box')
                and element_at(abtest,'app_home_change')='a'
            group by 
                dt
                ,device_id
        )
    )
    group by
        dt
),

--时长
overalltime as(
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
),


--次日留存，回到应用
retentA as(
select
    date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt
    ,count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
    (
    select
        dt
            ,device_id
    from
        mfw_dwd.fact_flow_mobile_hpinfo_event_increment
    where
        dt between date_format(date_add('day', -1, date_parse('20190317','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -1, date_parse('20190320','%Y%m%d')),'%Y%m%d') 
        and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
        and app_ver>='9.3.0'
        and event_code='home_article_list_show'
        and channel_id=55
        and element_at(basic_abtest,'app_home_change')='a'
    ) t1
      left join
    (
    select 
        dt
        ,device_id 
    from
        mfw_dws.aggr_flow_mobile_dau_dd_day 
    where
        dt between '20190317' and '20190320'
    ) t2
      on    
            t1.device_id=t2.device_id
            and date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
    group by 
          date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
),

--次日留存，回到信息流
retentB as(
select
    date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt,
    count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
    (
    select
        dt
            ,device_id
    from
        mfw_dwd.fact_flow_mobile_hpinfo_event_increment
    where
        dt between date_format(date_add('day', -1, date_parse('20190317','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -1, date_parse('20190320','%Y%m%d')),'%Y%m%d') 
        and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
        and app_ver>='9.3.0'
        and event_code='home_article_list_show'
        and channel_id=55
        and element_at(basic_abtest,'app_home_change')='a'
    ) t1
      left join
    (
    select 
        dt
        ,device_id 
    from
        mfw_dwd.fact_flow_mobile_hpinfo_event_increment 
    where
        dt between '20190317' and '20190320'
    ) t2
      on
            t1.device_id=t2.device_id
            and date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
    group by 
          date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
),

--七日留存，回到应用
retentC as(
select
    date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt,
    count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
    (
    select
        dt
            ,device_id
    from
        mfw_dwd.fact_flow_mobile_hpinfo_event_increment
    where
        dt between date_format(date_add('day', -7, date_parse('20190317','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -7, date_parse('20190320','%Y%m%d')),'%Y%m%d') 
        and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
        and app_ver>='9.3.0'
        and event_code='home_article_list_show'
        and channel_id=55
        and element_at(basic_abtest,'app_home_change')='a'
    ) t1
      left join
    (
    select 
        dt
        ,device_id 
    from
        mfw_dws.aggr_flow_mobile_dau_dd_day 
    where
        dt between '20190317' and '20190320'
    ) t2
      on
            t1.device_id=t2.device_id
            and date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
    group by 
          date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
),

--七日留存，回到信息流
retentD as(
select
    date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt,
    count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
    (
    select
        dt
        ,device_id
    from
        mfw_dwd.fact_flow_mobile_hpinfo_event_increment
    where
        dt between date_format(date_add('day', -7, date_parse('20190317','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -7, date_parse('20190320','%Y%m%d')),'%Y%m%d') 
        and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
        and app_ver>='9.3.0'
        and event_code='home_article_list_show'
        and channel_id=55
        and element_at(basic_abtest,'app_home_change')='a'
    ) t1
      left join
    (
    select 
        dt
        ,device_id 
    from
        mfw_dwd.fact_flow_mobile_hpinfo_event_increment
    where
        dt between '20190317' and '20190320'
    ) t2
      on
            t1.device_id=t2.device_id
            and date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
    group by 
          date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
)

select
    show.dt as "日期"
    ,page_uv.uv as "首页曝光UV"
    ,round(show.uv * 100.0 / page_uv.uv,2) as "信息流进入率"
    ,show.uv as "信息流曝光UV"
    ,show.pv as "信息流曝光PV"
    ,click.uv as "信息流点击UV"
    ,click.pv as "信息流点击PV"
    ,act.uv  as "互动用户数"
    ,act.pv as "互动量"
    ,round(click.uv * 100.0 / show.uv,2) as "点击行为用户比例"
    ,round(click.pv * 100.0 / show.pv,2) as "PV点击率"
    ,round(show.pv * 1.0 / show.uv,2) as "人均曝光篇数"
    ,round(click.pv * 1.0 / click.uv,2) as "人均点击篇数"
    ,retentA.retention as "次日留存率,回到应用"
    ,retentB.retention as "次日留存率,回到信息流"
    ,retentC.retention as "7日留存率,回到应用"
    ,retentD.retention as "7日留存率,回到信息流"
    ,overalltime.avg_duration as "人均停留时长"
from
    show
left join
    page_uv
on
    show.dt=page_uv.dt
left join
    click
on 
    show.dt=click.dt
left join
    act
on 
    show.dt=act.dt
left join
    overalltime
on 
    show.dt=overalltime.dt
left join
    retentA
on 
    show.dt=retentA.dt
left join
        retentB
on show.dt=retentB.dt
left join
        retentC
on show.dt=retentC.dt
left join
        retentD
on show.dt=retentD.dt
limit 100
