--发现池，候选池，推荐池总量 https://mdp.mafengwo.cn/mql?sid=24518440-5d28-46f7-ac32-40f2cbc46ed6&engine=PRESTO
select
	dt
  ,sum(flag) as "发现池总量"
  ,sum(pool_status) as "候选池总量"
  ,sum(status) as "推荐池总量"
from
	recommend.recommend_pool
where
	dt between '20190324' and '20190325'
group by
	dt
limit 50000