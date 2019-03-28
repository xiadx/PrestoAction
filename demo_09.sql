--union hpinfo，sc表的pv，uv对比 https://mdp.mafengwo.cn/mql?sid=f4ea185d-c323-40a9-befa-f2e974ceba15&engine=PRESTO
select 
    dt
  ,count(1) as pv
  ,count(distinct device_id) as uv
  ,'hpinfo' as type
from
    mfw_dwd.fact_flow_mobile_hpinfo_event_increment
where
    dt between '20190326' and '20190327'
group by
    dt

union

select
    dt
  ,count(1) as pv
  ,count(distinct device_id) as uv
  ,'sc' as type
from 
    mfw_dwd.fact_flow_mobile_sc_event_increment
where
    dt between '20190326' and '20190327'
group by
    dt

limit 100
