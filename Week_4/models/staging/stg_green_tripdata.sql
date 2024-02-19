{{
    config(
        materialized='view'
    )
}}

with tripdata as (
    select *,
        row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
    from {{ source('staging', 'green_tripdata') }}
    where vendorid is not null 
)

select 
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(VendorID as integer) as vendorid,
    cast(RatecodeID as integer) as ratecodeid,
    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,

    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    1 as trip_type,

    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    0 as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as numeric) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge

from tripdata
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}