with source as (
    select * from {{ source('steam_raw', 'assets_history') }}
),

renamed as (
    select
        asset_id,
        item_id,
        cast(buy_date as date)          as buy_date,
        cast(buy_price as numeric)      as buy_price,
        upper(buy_currency)             as buy_currency,
        cast(quantity as integer)       as quantity,
        initcap(category)               as category,
        purchase_channel,
        cast(last_updated as timestamp) as last_updated
    from source
)

select * from renamed