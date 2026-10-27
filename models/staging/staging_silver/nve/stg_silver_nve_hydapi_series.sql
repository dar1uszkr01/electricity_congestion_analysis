with source as (
    select * from {{ source('nve', 'nve_hydapi_series') }}
)

select * from source