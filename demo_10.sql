--show dt,cid,item_type,open_udid,item_type,pv https://mdp.mafengwo.cn/mql?sid=0f4eae09-e17b-47a7-99ce-1644b672571f&engine=PRESTO
with show as
(
select
    dt
  ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
  ,json_extract_scalar(show.attr, '$.item_type') as item_type
  ,open_udid
  ,json_extract_scalar(attr,'$.item_business_id') as item_id
  ,count(1) as pv
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
  ,open_udid
  ,json_extract_scalar(attr,'$.item_business_id')
 )
select 
    *
from
    show
limit 100
