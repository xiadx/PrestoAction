--互动行为 http://mdp.mafengwo.cn/mql?sid=33bb6cf8-5514-434c-bc88-1efaaa10c098&engine=PRESTO  
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
limit 100
