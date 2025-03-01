{{
    config(
        materialized='table'
    )
}}

with quarterly_revenues as (
  select
    pickup_year as year,
    pickup_quarter as quarter,
    service_type,
    sum(total_amount) as quarterly_revenue
  from {{ ref('fact_taxi_trips') }} 
  group by 1, 2,3
),
yoy_quarterly_growth as (
  select
    year,
    quarter,
    service_type,
    quarterly_revenue,
    lag(quarterly_revenue,1) over (
      partition by quarter, service_type 
      order by year
    ) as prev_year_quarterly_revenue
  from quarterly_revenues
)
select
  year,
  quarter,
  service_type,
  quarterly_revenue,
  prev_year_quarterly_revenue,
  (quarterly_revenue - prev_year_quarterly_revenue) as quarterly_revenue_growth,
  round(
    (quarterly_revenue - prev_year_quarterly_revenue) / nullif(prev_year_quarterly_revenue, 0) * 100, 
    2
  ) as revenue_growth_pct
from yoy_quarterly_growth
where year=2020
order by service_type,revenue_growth_pct