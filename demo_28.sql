--目的地相关指标统计 https://mdp.mafengwo.cn/mql?sid=b19efb30-3999-423d-b281-41e84d10c3ca
with total as(
select
  dt
	,count(1) as item_total
  ,sum(oldView) as view_total
  ,sum(oldClick) as click_total
from recommend.recommend_pool
where
	dt between '20190324' and '20190325'
  and flag=1
  and status=1
group by
	dt
),

mdd as(
  select 
	dt
  ,search_mdd_name
  ,count(1) as item_number
  ,sum(oldView) as mdd_view
  ,sum(oldClick) as mdd_click
from 
	recommend.recommend_pool 
where 
	dt between '20190324' and '20190325'
	and flag=1
  and status=1
group by
  dt
  ,search_mdd_name
),

total_search as(
select 
	dt
  ,sum(searchCount) as totalCount
from 
  recommend.rec_search_mdd
where
	dt between '20190324' and '20190325'
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
  dt between '20190324' and '20190325'
)

select
	total.dt as "日期"
  ,mdd.search_mdd_name as "目的地"
  ,round(mdd.mdd_view * 1.00000000 / total.view_total, 8) as "目的地曝光数占比"
  ,round(mdd.mdd_click * 1.00000000 / total.click_total, 8) as "目的地点击数占比"
  ,round(mdd.item_number * 1.00000000 / total.item_total, 8) as "目的地包含item数占比"
  ,round(search.searchCount * 1.00000000 / total_search.totalCount, 8) as "目的地搜索次数占比"
  ,round((round(mdd.item_number * 1.00000000 / total.item_total, 8) - round(search.searchCount * 1.00000000 / total_search.totalCount, 8)) / (round(search.searchCount * 1.00000000 / total_search.totalCount, 8) + 0.00000001), 4) as "item搜索分布差"  
  ,round(mdd.mdd_click * 1.0000 / mdd.mdd_view, 4) as "目的地CTR"
  ,round(mdd.mdd_view * 1.00 / mdd.item_number, 2) as "目的地平均每个item曝光数"
  ,round(mdd.mdd_click * 1.00 / mdd.item_number, 2) as "目的地平均每个item点击数"
  ,mdd.mdd_view as "目的地曝光数"
  ,total.view_total as "曝光总数"
  ,mdd.mdd_click as "目的地点击数"
  ,total.click_total as "点击总数"
  ,mdd.item_number as "目的地包含item数"
  ,total.item_total as "item总数"
  ,search.searchCount as "目的地搜索次数"
  ,total_search.totalCount as "搜索总次数"
from
	total
left join
	mdd
on
	total.dt=mdd.dt
left join
	total_search
on 
	total.dt=total_search.dt
inner join
	search
on 
	mdd.dt=search.dt
  and mdd.search_mdd_name=search.search_mdd_name
where
	search.searchCount > 1000
order by
	total.dt desc
  ,mdd.item_number desc
limit 50000