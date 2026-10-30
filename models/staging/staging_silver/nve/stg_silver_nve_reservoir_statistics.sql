with source as (
    select * from {{ source('nve', 'nve_reservoir_statistics') }}
)

select * from source