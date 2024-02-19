{{ config(materialized='view') }}


select 
    --indentifiers
/*    {{ dbt_utils.surrogate_key('vendorid', 'lpep_pickup_datetime')}} as tripid, 
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,*/
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    --timestamps
    cast(pickup_datetime as timestamp) as  pickup_datetime,
    cast(dropOff_datetime as timestamp) as  dropoff_datetime,

    --trip info
    dispatching_base_num,
    affiliated_base_number,
    sr_flag
from {{ source('staging', 'fhv_tripdata')}}


{% if var('is_test_run', default=true) %}
    
    limit 100

{% endif %}
