with source as (
    select * from {{ source('nve', 'nve_hydapi_parameters') }}
)

select * from source