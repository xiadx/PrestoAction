--retention_week_hpinfo dt, cid, item_type, retention https://mdp.mafengwo.cn/mql?sid=0587d179-ad11-40c2-96c2-923088b0b7c8&engine=PRESTO
with retention_week_hpinfo as(
select
  date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d') as dt
  ,t1.cid as cid
  ,t1.item_type as item_type
  ,count(distinct t2.device_id) * 1.0000 / count(distinct t1.device_id) as retention
from
  (
    select
    	dt
    	,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      ,json_extract_scalar(attr, '$.item_type') as item_type
    	,open_udid as device_id
    from
    	mobile_event.home_article_list_show
    where
      dt between date_format(date_add('day', -7, date_parse('20190326','%Y%m%d')),'%Y%m%d') and date_format(date_add('day', -7, date_parse('20190327','%Y%m%d')),'%Y%m%d') 
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
    	and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t1
  left join
  (
    select
    	dt
    	,regexp_extract(json_extract_scalar(attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
      ,json_extract_scalar(attr, '$.item_type') as item_type
    	,open_udid as device_id
    from
    	mobile_event.home_article_list_show
    where
      dt between '20190326' and '20190327'
      and app_code in ('com.mfw.roadbook','cn.mafengwo.www')
      and json_extract_scalar(attr, '$.channel_id') = '55'
    	and json_extract_scalar(attr, '$.item_type') in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
  ) t2
  on
  	t1.device_id=t2.device_id
  	and t1.item_type=t2.item_type
    and date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')=t2.dt
group by
  date_format(date_add('day', +7, date_parse(t1.dt,'%Y%m%d')),'%Y%m%d')
  ,t1.cid
  ,t1.item_type
)
select
	*
from
	retention_week_hpinfo
limit 100