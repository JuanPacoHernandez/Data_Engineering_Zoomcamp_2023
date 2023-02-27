{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
)

select 
    tripid,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationID,
    dropoff_locationID,
    SR_Flag,
    affiliated_base_number

from fhv_data