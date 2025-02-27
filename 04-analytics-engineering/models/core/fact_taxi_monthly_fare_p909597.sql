{{
    config(
        materialized='table'
    )
}}

with fare_quantile as (
    SELECT
        service_type,
        pickup_year as year,
        pickup_month as month,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97
    from {{ ref('fact_taxi_trips') }} 
    WHERE
        fare_amount > 0 AND trip_distance > 0 AND payment_type_description in ('Cash', 'Credit Card')
    group by
        1,2,3
)
select 
    service_type,
    year,
    month,
    p97,
    p95,
    p90
from fare_quantile
where year = 2020 and month = 4
    