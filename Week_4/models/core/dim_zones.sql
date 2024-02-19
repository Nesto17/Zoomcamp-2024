-- materialized as table for efficiency in integration to BI tools
{{
    config(
        materialized='table' 
    )
}}

select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('taxi_zone_lookup') }}