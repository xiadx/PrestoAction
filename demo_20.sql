--act dt, cid, open_udid, pv https://mdp.mafengwo.cn/mql?sid=3082ad96-df86-49e1-a6a6-d0727bece24f&engine=PRESTO
with act as(
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
  ,act.open_udid as open_udid
  ,act.pv as pv
  ,user.cid as cid
from
	act
left join
	user
on
	act.dt=user.dt
  and act.open_udid=user.open_udid
limit 1000
