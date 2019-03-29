--click dt, cid, pv, uv, pi https://mdp.mafengwo.cn/mql?sid=4725c754-6c63-42b1-b5a0-6b598a134139&engine=PRESTO
with click as(
select
	dt
  ,regexp_extract(json_extract_scalar(click.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
  ,json_extract_scalar(click.attr, '$.item_type') as item_type
  ,count(1) as pv
  ,count(distinct open_udid) as uv
  ,count(distinct attr_item_business_id) as pi
from 
	mobile_event.home_article_list_click click
where
	dt between '20190326' and '20190327'
	and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and json_extract_scalar(click.attr, '$.channel_id') = '55'
  and json_extract_scalar(click.attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
group by
	dt
  ,regexp_extract(json_extract_scalar(click.attr,'$.abtest'), '-1cid-2(.{5})', 1)
  ,json_extract_scalar(click.attr, '$.item_type')
)
select
	*
from
	click
limit 100