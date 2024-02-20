{{
    config(
        materialized='table'
    )
}}

with taxi_factdata as (
    select 
        tripid
        pickup_locationid, 
        dropoff_locationid,
        pickup_datetime, 
        dropoff_datetime, 
        service_type
    from {{ ref('fact_taxi_trips') }}
),
fhv_factdata as (
    select 
        tripid
        pickup_locationid, 
        dropoff_locationid,
        pickup_datetime, 
        dropoff_datetime, 
        service_type
    from {{ ref('fact_fhv_trips') }}
)

select *
from fhv_factdata
union all
select *
from taxi_factdata