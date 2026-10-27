with source as (
    select *
    from {{ source('met_norway', 'met_norway_frost_observations') }}
)

select * from source