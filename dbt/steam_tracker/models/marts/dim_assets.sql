with assets as (
    select * from {{ ref('stg_assets') }}
),

-- Deduplicate: keep the latest record per asset_id
deduped as (
    select
        *,
        row_number() over (
            partition by asset_id
            order by last_updated desc
        ) as rn
    from assets
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
    from deduped
    where rn = 1
)

select * from final

