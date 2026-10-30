with source as (
    select * from {{ source('nve', 'nve_hydapi_observations') }}
)

select * from source