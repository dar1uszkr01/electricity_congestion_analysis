with source as (
    select *
    from {{ source('nve', 'nve_snow_hydrology_raw') }}
)

select * from source