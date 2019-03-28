--act dt, open_udid, pv https://mdp.mafengwo.cn/mql?sid=61899017-ee0a-4cd3-b014-81452cd65ab2&engine=PRESTO
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
select
	*
from
	act
limit 100