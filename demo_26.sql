-- show dt, cid, pv, uv, pi https://mdp.mafengwo.cn/mql?sid=c19fd207-5572-4884-8a0e-c1141e89eb1c&engine=PRESTO
with show as(
select
  dt
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
  --,json_extract_scalar(show.attr, '$.item_type') as item_type
  ,count(1) as pv
  ,count(distinct open_udid) as uv
  ,count(distinct attr_item_business_id) as pi
from 
  mobile_event.home_article_list_show show
where
  dt between '20190326' and '20190327'
  and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and json_extract_scalar(show.attr, '$.channel_id') = '55'
  --and json_extract_scalar(show.attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
group by
  dt
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1)
  --,json_extract_scalar(show.attr, '$.item_type')
),

--click dt, cid, pv, uv, pi https://mdp.mafengwo.cn/mql?sid=4725c754-6c63-42b1-b5a0-6b598a134139&engine=PRESTO
click as(
select
  dt
  ,regexp_extract(json_extract_scalar(click.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
  --,json_extract_scalar(click.attr, '$.item_type') as item_type
  ,count(1) as pv
  ,count(distinct open_udid) as uv
  ,count(distinct attr_item_business_id) as pi
from 
  mobile_event.home_article_list_click click
where
  dt between '20190326' and '20190327'
  and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and json_extract_scalar(click.attr, '$.channel_id') = '55'
  --and json_extract_scalar(click.attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
group by
  dt
  ,regexp_extract(json_extract_scalar(click.attr,'$.abtest'), '-1cid-2(.{5})', 1)
  --,json_extract_scalar(click.attr, '$.item_type')
),

act as(
select
  dt
  ,open_udid
  ,count(1) as pv
from 
  default.mobile_event_parquet act
where 
  dt between '20190326' and '20190327'
  and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and event_code='weng_click'
  and json_extract_scalar(act.attr, '$.page_name')='首页'
  and url_extract_parameter(act.uri,'channel_id')='55'
  and json_extract_scalar(act.attr, '$.module_name') in ('share','favorite','comment','collect')
group by
  dt
  ,open_udid
)
,user as(
select
  dt
  ,open_udid
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
from 
  mobile_event.home_article_list_show show
where
  dt between '20190326' and '20190327'
  and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and json_extract_scalar(show.attr, '$.channel_id') = '55'
group by
  dt
  ,open_udid
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1)
),

act_user as(
select
  act.dt as dt
  ,user.cid as cid
  ,sum(act.pv) as pv
  ,count(distinct act.open_udid) as uv 
from
  act
left join
  user
on
  act.dt=user.dt
  and act.open_udid=user.open_udid
group by
  act.dt
  ,user.cid
),

--retention_day_app dt, cid, item_type, retention https://mdp.mafengwo.cn/mql?sid=e04e6008-f763-4a38-9108-7f68575dadd2&engine=PRESTO
retention_day_app as(
select
  date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt
  ,t1.cid as cid
  --,t1.item_type as item_type
  ,count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
  (
    select
      dt
      ,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      --,json_extract_scalar(attr, '$.item_type') as item_type
      ,open_udid as device_id
    from
      mobile_event.home_article_list_show
    where
      dt between date_format(date_add('day', -1, date_parse('20190326','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -1, date_parse('20190327','%Y%m%d')),'%Y%m%d') 
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
      --and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t1
  left join
  (
    select
      dt
      ,device_id
    from 
      mfw_dws.aggr_flow_mobile_dau_dd_day
    where
      dt between '20190326' and '20190327'
  ) t2
  on
    t1.device_id=t2.device_id
    and date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
group by
  date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
  ,t1.cid
  --,t1.item_type
),

--retention_day_hpinfo dt, cid, item_type, retention https://mdp.mafengwo.cn/mql?sid=378b8b49-4e92-4084-8d36-005f0aa7060a&engine=PRESTO
retention_day_hpinfo as(
select
  date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt
  ,t1.cid as cid
  --,t1.item_type as item_type
  ,count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
  (
    select
      dt
      ,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      --,json_extract_scalar(attr, '$.item_type') as item_type
      ,open_udid as device_id
    from
      mobile_event.home_article_list_show
    where
      dt between date_format(date_add('day', -1, date_parse('20190326','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -1, date_parse('20190327','%Y%m%d')),'%Y%m%d') 
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
      --and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t1
  left join
  (
    select
      dt
      ,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      --,json_extract_scalar(attr, '$.item_type') as item_type
      ,open_udid as device_id
    from
      mobile_event.home_article_list_show
    where
      dt between '20190326' and '20190327'
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
      --and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t2
  on
    t1.device_id=t2.device_id
    --and t1.item_type=t2.item_type
    and date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
group by
  date_format(date_add('day', +1, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
  ,t1.cid
  --,t1.item_type
),

--retention_week_app dt, cid, item_type, retention https://mdp.mafengwo.cn/mql?sid=885edafe-65ce-464c-835b-ba8126873493&engine=PRESTO
retention_week_app as(
select
  date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt
  ,t1.cid as cid
  --,t1.item_type as item_type
  ,count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
  (
    select
      dt
      ,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      --,json_extract_scalar(attr, '$.item_type') as item_type
      ,open_udid as device_id
    from
      mobile_event.home_article_list_show
    where
      dt between date_format(date_add('day', -7, date_parse('20190326','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -7, date_parse('20190327','%Y%m%d')),'%Y%m%d') 
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
      --and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t1
  left join
  (
    select
      dt
      ,device_id
    from 
      mfw_dws.aggr_flow_mobile_dau_dd_day
    where
      dt between '20190326' and '20190327'
  ) t2
  on
    t1.device_id=t2.device_id
    and date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
group by
  date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
  ,t1.cid
  --,t1.item_type
),

--retention_week_hpinfo dt, cid, item_type, retention https://mdp.mafengwo.cn/mql?sid=0587d179-ad11-40c2-96c2-923088b0b7c8&engine=PRESTO
retention_week_hpinfo as(
select
  date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt
  ,t1.cid as cid
  --,t1.item_type as item_type
  ,count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
  (
    select
      dt
      ,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      --,json_extract_scalar(attr, '$.item_type') as item_type
      ,open_udid as device_id
    from
      mobile_event.home_article_list_show
    where
      dt between date_format(date_add('day', -7, date_parse('20190326','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -7, date_parse('20190327','%Y%m%d')),'%Y%m%d') 
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
      --and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t1
  left join
  (
    select
      dt
      ,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      --,json_extract_scalar(attr, '$.item_type') as item_type
      ,open_udid as device_id
    from
      mobile_event.home_article_list_show
    where
      dt between '20190326' and '20190327'
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
      --and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t2
  on
    t1.device_id=t2.device_id
    --and t1.item_type=t2.item_type
    and date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
group by
  date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
  ,t1.cid
  --,t1.item_type
)

select
  show.dt as "日期"
  ,show.cid as "桶号"
  --,show.item_type as "内容类型"
  ,show.pv as "信息流曝光PV"
  ,show.uv as "信息流曝光UV"
  ,show.pi as "信息流曝光篇数"
  ,click.pv as "信息流点击PV"
  ,click.uv as "信息流点击UV"
  ,click.pi as "信息流点击篇数"
  ,act_user.pv as "互动量"
  ,act_user.uv as "互动用户数"
  ,round(click.pv * 1.0000 / show.pv, 4) as CTR
  ,round(click.uv * 1.0000 / show.uv, 4) as "点击行为用户比例"
  ,round(act_user.uv * 1.0000 / show.uv, 4) as "有互动行为用户比例"
  ,round(show.pv * 1.00 / show.uv, 2) as "人均曝光篇数"
  ,round(click.pv * 1.00 / click.uv, 2) as "人均点击篇数"
  ,retention_day_app.retention as "APP次日留存率"
  ,retention_day_hpinfo.retention as "信息流次日留存率"
  ,retention_week_app.retention as "APP七日留存率"
  ,retention_week_hpinfo.retention as "信息流七日留存率"
from
  show
left join
  click
on 
  show.dt=click.dt
  and show.cid=click.cid
left join
  act_user
on
  show.dt=act_user.dt
  and show.cid=act_user.cid
left join
  retention_day_app
on
  show.dt=retention_day_app.dt
  and show.cid=retention_day_app.cid
left join
  retention_day_hpinfo
on 
  show.dt=retention_day_hpinfo.dt
  and show.cid=retention_day_hpinfo.cid
left join
  retention_week_app
on
  show.dt=retention_week_app.dt
  and show.cid=retention_week_app.cid
left join
  retention_week_hpinfo
on 
  show.dt=retention_week_hpinfo.dt
  and show.cid=retention_week_hpinfo.cid
limit 1000