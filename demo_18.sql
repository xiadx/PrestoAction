-- act dt, cid, pv, uv, pi https://mdp.mafengwo.cn/mql?sid=4ae7a43a-93a4-41fe-8b36-466fd097e369&engine=PRESTO
with user as(
select
  dt
  ,open_udid
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
  ,json_extract_scalar(show.attr, '$.item_type') as item_type
from 
	mobile_event.home_article_list_show show
where
	dt between '20190326' and '20190327'
	and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
  and json_extract_scalar(show.attr, '$.channel_id') = '55'
  and json_extract_scalar(show.attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
group by
  dt
  ,open_udid
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1)
  ,json_extract_scalar(show.attr, '$.item_type')
)
select
	*
from
	user
limit 100