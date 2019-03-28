--cid dt, open_udid, cid_numbers https://mdp.mafengwo.cn/mql?sid=85319762-6728-4770-a731-c67da49f385a&engine=PRESTO
with cid as(
select
 dt
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
)
select 
    dt
  ,open_udid
  ,count(1) as cid_numbers
from
    cid
group by
    1
  ,2
order by
    cid_numbers desc
limit 1000
