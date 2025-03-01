{{
    config(
        materialized='table'
    )
}}

with travel_time as (
    select 
        pickup_year,
        pickup_month,
        pickup_locationid,
        dropoff_locationid,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    from {{ ref('fact_fhv_trips') }}
    where pickup_year = 2019 
        and pickup_month = 11 
        and  pick_up_zone in ('Newark Airport','SoHo','Yorkville East')
),

P90_travel_time as ( 
    select 
        pickup_year,
        pickup_month,
        pickup_locationid,
        dropoff_locationid,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90_trip_duration
    from travel_time
    where trip_duration > 0
    group by 1,2,3,4
),

dim_zones as (
    select * from `dezoomcamp2025-448509.dezoomamp_ny_taxi.dim_zones`
    where borough != 'Unknown'
),

zone_p90 as (
    select 
        pickup_zone.zone as pu_zone,
        dropoff_zone.zone as do_zone,
        p90_trip_duration,
    from P90_travel_time
    inner join dim_zones as pickup_zone
    on P90_travel_time.pickup_locationid = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on P90_travel_time.dropoff_locationid = dropoff_zone.locationid
),

ranked_zone_p90 as ( 
    select 
        pu_zone,
        do_zone,
        p90_trip_duration,
        rank() over (partition by pu_zone order by p90_trip_duration DESC) as p90_duration_rank
    from zone_p90
)  

select
    pu_zone,
    do_zone,
    p90_trip_duration,
    p90_duration_rank
from ranked_zone_p90
where p90_duration_rank=2
order by 1