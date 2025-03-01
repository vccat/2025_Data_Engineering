{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
        EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
        EXTRACT(MONTH FROM pickup_datetime) AS pickup_month
    from {{ ref('stg_fhv_tripdata') }}
    where EXTRACT(YEAR FROM pickup_datetime) = 2019
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    dispatching_base_num,
    pickup_locationid,
    pickup_zone.borough as pick_up_borough, 
    pickup_zone.zone as pick_up_zone,
    dropoff_locationid,
    dropoff_zone.borough as drop_off_borough, 
    dropoff_zone.zone as drop_off_zone,
    pickup_datetime,
    pickup_year,
    pickup_month,
    dropoff_datetime,
    sr_flag,
    affiliated_base_number
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid

