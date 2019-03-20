--首页UV https://mdp.mafengwo.cn/mql?sid=9ddaad35-011b-411d-ad77-74c1aea60559&engine=PRESTO
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
limit 100
