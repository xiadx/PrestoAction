--cid dt, datetime, pen_udid, cid_arrays, cid_numbers https://mdp.mafengwo.cn/mql?sid=d49cc08b-b17b-4f03-b696-1f24e7061c05&engine=PRESTO
select
 datetime
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
  datetime
  ,open_udid
  ,array_join(array_agg(cid), ',', 'null') as cid_arrays
  ,count(1) as cid_numbers
from
    cid
group by
    1
  ,2
order by
    cid_numbers desc
limit 1000
