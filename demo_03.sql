--信息流点击 //mdp.mafengwo.cn/mql?sid=0b2e71ff-e998-49b8-8f5f-bb5f89ef3c72&engine=PRESTO
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
limit 100
