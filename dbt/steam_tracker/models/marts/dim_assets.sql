with assets as (
    select * from {{ ref('stg_assets') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['asset_id']) }} as asset_sk,
        asset_id,
        item_id,
        buy_date,
        buy_price,
        buy_currency,
        quantity,
        category,
        purchase_channel,
        last_updated
    from assets
)

select * from final