--信息流曝光 //mdp.mafengwo.cn/mql?sid=cf64fb6a-f023-4ed3-a2cc-7921cde4f56c&engine=PRESTO
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
limit 100
