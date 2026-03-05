{{
    config(
        materialized="incremental",
        unique_key="customer_id",
        cluster_by=["customer_id"],
        tags=["baseline", "daily"]
    )
}}

/*
    90-day rolling behavioral baseline per customer.

    This is the reference point for everything - we compare every incoming
    transaction against what's "normal" for that specific customer.
    Running it incrementally so we only recompute customers who actually
    had new transactions, not the entire table every 5 minutes.
*/

with txns as (

    select *
    from {{ ref("stg_transactions") }}
    where
        transaction_date >= dateadd(day, -{{ var("baseline_lookback_days") }}, current_date())
        and transaction_date < current_date()   -- exclude today, it's incomplete

    {% if is_incremental() %}
        -- only touch customers active since our last run
        and customer_id in (
            select distinct customer_id
            from {{ ref("stg_transactions") }}
            where transaction_date >= (select max(baseline_date) from {{ this }})
        )
    {% endif %}

),

spending as (

    select
        customer_id,

        avg(amount)                                                     as avg_amount,
        stddev(amount)                                                  as stddev_amount,
        median(amount)                                                  as median_amount,

        -- percentiles give a more robust picture than just mean/stddev
        percentile_cont(0.25) within group (order by amount)           as p25_amount,
        percentile_cont(0.75) within group (order by amount)           as p75_amount,
        percentile_cont(0.95) within group (order by amount)           as p95_amount,
        percentile_cont(0.99) within group (order by amount)           as p99_amount,

        count(*)                                                        as txn_count_90d,
        sum(amount)                                                     as total_spend_90d,
        count(*) / 90.0                                                 as avg_daily_txns,

        max(transaction_date)                                           as last_txn_date

    from txns
    group by 1

),

geography as (

    select
        customer_id,
        mode(country)                                                   as home_country,
        mode(city)                                                      as home_city,
        count(distinct country)                                         as countries_visited,

        -- what fraction of their transactions are cross-border
        sum(
            case when country != mode(country) over (partition by customer_id)
            then 1 else 0 end
        ) / nullif(count(*), 0)                                         as cross_border_rate

    from txns
    group by 1

),

timing as (

    select
        customer_id,
        mode(hour(transaction_timestamp))                               as typical_hour,

        -- customers who never transact at night should be flagged when they do
        sum(
            case when hour(transaction_timestamp) between 0 and 5
            then 1 else 0 end
        ) / nullif(count(*), 0)                                         as late_night_rate

    from txns
    group by 1

),

velocity as (

    -- what's the most transactions this customer has ever done in a 5-min burst?
    -- we use this later to detect when they're way above their own personal max
    select
        customer_id,
        max(burst_5min)     as historical_max_5min,
        max(burst_15min)    as historical_max_15min,
        max(burst_1hr)      as historical_max_1hr

    from (
        select
            customer_id,
            transaction_timestamp,

            count(*) over (
                partition by customer_id
                order by transaction_timestamp
                range between interval '5 minutes' preceding and current row
            ) - 1 as burst_5min,

            count(*) over (
                partition by customer_id
                order by transaction_timestamp
                range between interval '15 minutes' preceding and current row
            ) - 1 as burst_15min,

            count(*) over (
                partition by customer_id
                order by transaction_timestamp
                range between interval '1 hour' preceding and current row
            ) - 1 as burst_1hr

        from txns
    )
    group by 1

),

devices as (

    select
        customer_id,
        count(distinct device_id)   as device_count_90d,
        mode(device_id)             as primary_device

    from txns
    group by 1

)

select
    s.customer_id,

    s.avg_amount,
    s.stddev_amount,
    s.median_amount,
    s.p25_amount,
    s.p75_amount,
    s.p95_amount,
    s.p99_amount,
    s.txn_count_90d,
    s.total_spend_90d,
    s.avg_daily_txns,
    s.last_txn_date,

    g.home_country,
    g.home_city,
    g.countries_visited,
    g.cross_border_rate,

    t.typical_hour,
    t.late_night_rate,

    v.historical_max_5min,
    v.historical_max_15min,
    v.historical_max_1hr,

    d.device_count_90d,
    d.primary_device,

    current_date()      as baseline_date,
    current_timestamp() as computed_at

from spending s
left join geography g using (customer_id)
left join timing    t using (customer_id)
left join velocity  v using (customer_id)
left join devices   d using (customer_id)
