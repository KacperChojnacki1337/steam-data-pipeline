with prices as (
    select * from {{ ref('stg_prices') }}
),

latest as (
    select
        item_id,
        price_usd,
        fetched_at,
        row_number() over (
            partition by item_id
            order by fetched_at desc
        ) as rn
    from prices
)

select
    item_id,
    price_usd,
    fetched_at as price_fetched_at
from latest
where rn = 1