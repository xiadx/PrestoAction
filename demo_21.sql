--act dt, cid, pv, uv https://mdp.mafengwo.cn/mql?sid=2347be11-05a0-4d53-950c-e515921fc38b&engine=PRESTO
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
)

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
limit 1000