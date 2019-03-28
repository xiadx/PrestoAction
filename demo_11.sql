--cid dt, cid, open_udid, pv https://mdp.mafengwo.cn/mql?sid=cf9adc04-4a0a-4ef5-bce0-ebaca590e746&engine=PRESTO
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
    *
from
    cid
limit 1000
