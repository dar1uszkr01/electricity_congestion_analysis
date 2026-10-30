with source as (
    select *
    from {{ source('met_norway', 'met_norway_weather_forecasts') }}
)

select * from source