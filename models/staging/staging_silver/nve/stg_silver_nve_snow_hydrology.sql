with source as (
    select *
    from {{ source('nve', 'nve_snow_hydrology') }}
)

select * from source