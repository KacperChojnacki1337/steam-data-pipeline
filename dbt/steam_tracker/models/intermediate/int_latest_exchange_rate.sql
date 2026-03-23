with rates as (
    select * from {{ ref('stg_exchange_rates') }}
),

latest as (
    select
        from_currency,
        to_currency,
        rate,
        fetched_at,
        row_number() over (
            partition by from_currency, to_currency
            order by fetched_at desc
        ) as rn
    from rates
)

select
    from_currency,
    to_currency,
    rate,
    fetched_at as rate_fetched_at
from latest
where rn = 1