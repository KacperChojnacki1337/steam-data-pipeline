with assets as (
    select * from {{ ref('dim_assets') }}
),

prices as (
    select * from {{ ref('int_latest_prices') }}
),

exchange_rate as (
    select * from {{ ref('int_latest_exchange_rate') }}
    where from_currency = 'USD'
    and to_currency = 'PLN'
),

final as (
    select
        a.asset_sk,
        a.asset_id,
        a.item_id,
        a.buy_date,
        a.buy_price                                                         as buy_price_pln,
        a.buy_currency,
        a.quantity,
        a.category,
        a.purchase_channel,

        -- Current prices
        p.price_usd                                                         as current_price_usd,
        round(p.price_usd * r.rate, 2)                                      as current_price_pln,
        p.price_fetched_at,
        r.rate                                                              as usd_pln_rate,
        r.rate_fetched_at,

        -- Portfolio value
        round(p.price_usd * a.quantity, 2)                                  as current_value_usd,
        round(p.price_usd * r.rate * a.quantity, 2)                         as current_value_pln,

        -- Unrealized PnL in PLN (buy and current both in PLN)
        round((p.price_usd * r.rate) - a.buy_price, 2)                     as pnl_per_unit_pln,
        round(((p.price_usd * r.rate) - a.buy_price) * a.quantity, 2)      as pnl_total_pln,

        -- Unrealized PnL %
        round(
            (((p.price_usd * r.rate) - a.buy_price) / nullif(a.buy_price, 0)) * 100
        , 2)                                                                as pnl_pct

    from assets a
    left join prices p on a.item_id = p.item_id
    left join exchange_rate r on 1 = 1
)

select * from final