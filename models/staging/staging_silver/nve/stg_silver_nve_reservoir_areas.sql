with source as (
    select * from {{ source('nve', 'nve_reservoir_areas') }}
)

select * from source