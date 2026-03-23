with source as (
    select * from {{ source('steam_raw', 'exchange_rates') }}
),

renamed as (
    select
        from_currency,
        to_currency,
        cast(rate as numeric)        as rate,
        source                       as rate_source,
        cast(timestamp as timestamp) as fetched_at
    from source
)

select * from renamed