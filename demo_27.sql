--目的地搜索次数占比 https://mdp.mafengwo.cn/mql?sid=925ac243-e0ce-4c96-825e-01e3e81da79b
with total as(
select 
	dt
  ,sum(searchCount) as totalCount
from 
  recommend.rec_search_mdd
where
	dt between '20190307' and '20190308'
group by
  dt
),
search as(
select
	dt
  ,search_mdd_name
  ,searchCount
from
  recommend.rec_search_mdd
where
  dt between '20190307' and '20190308'
)

select
	search.dt as "日期"
  ,search.search_mdd_name as "目的地"
  ,search.searchCount as "目的地搜索次数"
  ,round(search.searchCount * 1.000000 / total.totalCount, 6) as "目的地搜索占比"
from
	search
left join
	total
on 
	search.dt=total.dt
order by
	search.dt desc
	,search.searchCount desc
limit 50000