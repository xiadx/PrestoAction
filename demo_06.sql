--次日留存 //mdp.mafengwo.cn/mql?sid=0302a812-689e-4f3d-9c3e-a2b661ded5f4&engine=PRESTO
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
limit 100
