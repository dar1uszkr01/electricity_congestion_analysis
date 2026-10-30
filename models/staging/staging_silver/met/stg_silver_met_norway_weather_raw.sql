with source as (
    select *
    from {{ source('met_norway', 'met_norway_weather_raw') }}
)

select * from source