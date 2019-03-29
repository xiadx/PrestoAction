--新发池，候选池，推荐池变化情况 https://mdp.mafengwo.cn/mql?sid=6e66cfb9-0aa1-4d27-af8e-a696a8c6fbfc
with total as(
select
	dt
  ,sum(flag) as flag_total
  ,sum(pool_status) as ps_total
  ,sum(status) as status_total
from
	recommend.recommend_pool
where
	dt between '20190324' and '20190325'
group by
	dt
),

--可发
flag as(
select
	flagupdatetime
  ,sum(flag) as flag_add
  ,count(1)-sum(flag) as flag_cut
	,count(1) as flag_vary
from
	recommend.recommend_pool
where 
	flagupdatetime between '20190324' and '20190325'
group by
	flagupdatetime
),

--候选
ps as(
select
	psupdatetime
  ,count(1) as ps_add
from 
	recommend.recommend_pool
where
	psupdatetime between '20190324' and '20190325'
group by
	psupdatetime
),

--推荐
status as(
  select
	statusupdatetime
  ,sum(status) as status_add
  ,count(1)-sum(status) as status_cut
  ,count(1) as status_vary
from 
	recommend.recommend_pool
where
	statusupdatetime between '20190324' and '20190325'
group by
	statusupdatetime
)

select
	flag.flagupdatetime as "日期"
  ,total.flag_total as "可发总量"
  ,total.ps_total as "候选总量"
  ,total.status_total as "推荐总量"
	,flag.flag_add as "可发新增"
  ,flag.flag_cut as "可发减少"
  ,flag.flag_vary as "可发变化"
  ,ps.ps_add as "候选新增"
  ,status.status_add as "推荐新增"
  ,status.status_cut as "推荐减少"
  ,status.status_vary as "推荐变化"
from
	total
left join
  flag
on
	total.dt=flag.flagupdatetime
left join
	ps
on
	flag.flagupdatetime=ps.psupdatetime
left join
	status
on
	flag.flagupdatetime=status.statusupdatetime
limit 50000