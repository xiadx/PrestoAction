-- show dt, cid, pv, uv, pi https://mdp.mafengwo.cn/mql?sid=c19fd207-5572-4884-8a0e-c1141e89eb1c&engine=PRESTO
with show as(
select
	dt
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
  ,json_extract_scalar(show.attr, '$.item_type') as item_type
  ,count(1) as pv
  ,count(distinct open_udid) as uv
  ,count(distinct attr_item_business_id) as pi
from 
	mobile_event.home_article_list_show show
where
	dt between '20190326' and '20190327'
	and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and json_extract_scalar(show.attr, '$.channel_id') = '55'
  and json_extract_scalar(show.attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
group by
	dt
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1)
  ,json_extract_scalar(show.attr, '$.item_type')
)
select
	*
from
	show
limit 100