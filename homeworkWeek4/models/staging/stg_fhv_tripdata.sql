{{ config(materialized='view') }}

select 
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    -- trip info
    dispatching_base_num,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    cast(PUlocationID as integer) as  pickup_locationID,
    cast(DOlocationID as integer) as dropoff_locationID,
    cast(SR_Flag as numeric) as SR_Flag,
    Affiliated_base_number as affiliated_base_number

from {{ source('staging', 'fhvTaxiParquet') }}