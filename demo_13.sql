--cid dt, houur, open_udid, cid_numbers https://mdp.mafengwo.cn/mql?sid=166c8afc-2132-4f7f-bd06-4d4e7824c24e&engine=PRESTO
with cid as(
select
 dt
 ,hour
 ,regexp_extract(json_extract_scalar(show.attr,'$.abtest'), '-1cid-2(.{5})', 1) as cid
 ,open_udid
 ,count(1) as pv
from 
 mobile_event.home_article_list_show show
where
    dt between '20190326' and '20190327'
group by
  1
  ,2
  ,3
  ,4
)
select 
    dt
  ,hour
  ,open_udid
  ,count(1) as cid_numbers
from
    cid
group by
    1
  ,2
  ,3
order by
    cid_numbers desc
limit 1000
