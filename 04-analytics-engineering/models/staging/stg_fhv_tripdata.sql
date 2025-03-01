{{
    config(
        materialized='view'
    )
}}

with fhv_trips as (

    select * from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null

)

select
    dispatching_base_num,

    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,
    affiliated_base_number
    
from fhv_trips




