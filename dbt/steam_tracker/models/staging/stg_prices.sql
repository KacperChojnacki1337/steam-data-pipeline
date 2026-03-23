with source as (
    select * from {{ source('steam_raw', 'prices_history') }}
),

renamed as (
    select
        item_id,
        cast(price_usd as numeric)   as price_usd,
        cast(timestamp as timestamp) as fetched_at
    from source
)

select * from renamed