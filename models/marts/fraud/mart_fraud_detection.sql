{{
    config(
        materialized="incremental",
        unique_key="transaction_id",
        cluster_by=["transaction_date", "risk_level"],
        tags=["fraud", "scoring"]
    )
}}

/*
    Multi-factor fraud scoring across 5 dimensions (max 100 points):

        Amount anomaly          30 pts  — z-score vs 90-day baseline
        Geographic impossibility 30 pts  — haversine distance / time > 800 km/h
        Velocity burst          25 pts  — transactions in 5-min window vs historical max
        Device anomaly          10 pts  — new device for a creature-of-habit customer
        Temporal anomaly         5 pts  — transacting at hours they never use

    >= 60  →  HIGH   →  BLOCK
    40-59  →  MEDIUM →  STEP_UP_AUTH
    < 40   →  LOW    →  APPROVE

    Keeping all logic in SQL (not Spark) so analysts can read, test, and modify
    scoring rules without touching any infrastructure.
*/

with new_txns as (

    select * from {{ ref("stg_transactions") }}

    {% if is_incremental() %}
        where transaction_timestamp > (
            select max(transaction_timestamp) from {{ this }}
        )
    {% endif %}

),

baseline as (
    select * from {{ ref("int_customer_baseline") }}
),


-- ── 1. Amount anomaly ─────────────────────────────────────────────────────────
-- z-score measures how many standard deviations above normal this amount is.
-- beyond 3σ is statistically unusual for ~99.7% of normal distributions.

amount_scored as (

    select
        t.*,
        b.avg_amount        as baseline_avg,
        b.stddev_amount     as baseline_std,

        case
            when b.stddev_amount > 0
            then (t.amount - b.avg_amount) / b.stddev_amount
            else 0
        end as amount_zscore,

        case
            when b.stddev_amount > 0
             and (t.amount - b.avg_amount) / b.stddev_amount > 3   then 30
            when b.stddev_amount > 0
             and (t.amount - b.avg_amount) / b.stddev_amount > 2   then 20
            when t.amount > b.p99_amount                            then 15
            else 0
        end as amount_score

    from new_txns t
    left join baseline b using (customer_id)

),


-- ── 2. Velocity burst ─────────────────────────────────────────────────────────
-- card-testing attacks flood 10-20 transactions in under 2 minutes.
-- compare the current burst against what this customer has ever done historically.

velocity_scored as (

    select
        a.*,
        b.historical_max_5min,

        count(*) over (
            partition by a.customer_id
            order by a.transaction_timestamp
            range between interval '5 minutes' preceding and current row
        ) - 1 as current_burst_5min,

        case
            when count(*) over (
                partition by a.customer_id
                order by a.transaction_timestamp
                range between interval '5 minutes' preceding and current row
            ) - 1 > greatest(coalesce(b.historical_max_5min, 0), 3)    then 25
            when count(*) over (
                partition by a.customer_id
                order by a.transaction_timestamp
                range between interval '5 minutes' preceding and current row
            ) - 1 > 3                                                    then 15
            else 0
        end as velocity_score

    from amount_scored a
    left join baseline b using (customer_id)

),


-- ── 3. Geographic impossibility ───────────────────────────────────────────────
-- haversine distance between this txn and the previous one.
-- if the implied travel speed is faster than a commercial flight, flag it.
-- note: RADIANS() is critical here - SQL trig functions need radians not degrees.
-- (got burned by this - raw lat/lon in degrees gives wildly wrong distances)

geo_lagged as (

    select
        v.*,
        lag(latitude)             over (partition by customer_id order by transaction_timestamp) as prev_lat,
        lag(longitude)            over (partition by customer_id order by transaction_timestamp) as prev_lon,
        lag(country)              over (partition by customer_id order by transaction_timestamp) as prev_country,
        lag(transaction_timestamp) over (partition by customer_id order by transaction_timestamp) as prev_ts
    from velocity_scored v

),

geo_scored as (

    select
        g.*,

        -- haversine great-circle distance in km
        2 * 6371 * asin(sqrt(
            power(sin(radians((latitude  - prev_lat) / 2)), 2)
            + cos(radians(prev_lat))
            * cos(radians(latitude))
            * power(sin(radians((longitude - prev_lon) / 2)), 2)
        )) as distance_km,

        datediff(minute, prev_ts, transaction_timestamp) / 60.0 as hours_elapsed,

        case
            -- physically impossible: faster than any aircraft
            when 2 * 6371 * asin(sqrt(
                    power(sin(radians((latitude  - prev_lat) / 2)), 2)
                    + cos(radians(prev_lat))
                    * cos(radians(latitude))
                    * power(sin(radians((longitude - prev_lon) / 2)), 2)
                )) / nullif(datediff(minute, prev_ts, transaction_timestamp) / 60.0, 0)
                > {{ var("impossible_speed_kmh") }}                     then 30
            -- different country in under an hour is still suspicious
            when country != prev_country
             and datediff(minute, prev_ts, transaction_timestamp) < 60  then 20
            when country != prev_country                                 then 10
            else 0
        end as geo_score,

        case
            when 2 * 6371 * asin(sqrt(
                    power(sin(radians((latitude  - prev_lat) / 2)), 2)
                    + cos(radians(prev_lat))
                    * cos(radians(latitude))
                    * power(sin(radians((longitude - prev_lon) / 2)), 2)
                )) / nullif(datediff(minute, prev_ts, transaction_timestamp) / 60.0, 0)
                > 0
            then 2 * 6371 * asin(sqrt(
                    power(sin(radians((latitude  - prev_lat) / 2)), 2)
                    + cos(radians(prev_lat))
                    * cos(radians(latitude))
                    * power(sin(radians((longitude - prev_lon) / 2)), 2)
                )) / (datediff(minute, prev_ts, transaction_timestamp) / 60.0)
            else 0
        end as travel_speed_kmh

    from geo_lagged g

),


-- ── 4. Device anomaly ─────────────────────────────────────────────────────────
-- someone who always uses the same iPhone and suddenly shows up on a new Android
-- is more suspicious than a customer who rotates 5 devices regularly.

device_scored as (

    select
        g.*,
        b.primary_device,
        b.device_count_90d,

        case
            when g.device_id != b.primary_device
             and b.device_count_90d < 3     then 10   -- creature of habit, new device
            when g.device_id != b.primary_device then 5
            else 0
        end as device_score

    from geo_scored g
    left join baseline b using (customer_id)

),


-- ── 5. Temporal anomaly ───────────────────────────────────────────────────────
-- low weight (5 pts) because plenty of legit reasons to transact at odd hours.
-- only fires if the customer genuinely never does this.

temporal_scored as (

    select
        d.*,
        b.late_night_rate,
        b.typical_hour,

        case
            when hour(d.transaction_timestamp) between 0 and 5
             and b.late_night_rate < 0.05   then 5
            else 0
        end as temporal_score

    from device_scored d
    left join baseline b using (customer_id)

)


-- ── Final output ──────────────────────────────────────────────────────────────

select
    transaction_id,
    customer_id,
    transaction_date,
    transaction_timestamp,

    amount,
    currency,
    merchant_name,
    merchant_category,
    country,
    city,
    device_id,

    -- individual scores for explainability / analyst review
    amount_score,
    velocity_score,
    geo_score,
    device_score,
    temporal_score,

    amount_score + velocity_score + geo_score + device_score + temporal_score
        as total_score,

    case
        when amount_score + velocity_score + geo_score + device_score + temporal_score
             >= {{ var("high_risk_threshold") }}    then "HIGH"
        when amount_score + velocity_score + geo_score + device_score + temporal_score
             >= {{ var("medium_risk_threshold") }}  then "MEDIUM"
        else "LOW"
    end as risk_level,

    case
        when amount_score + velocity_score + geo_score + device_score + temporal_score
             >= {{ var("high_risk_threshold") }}    then "BLOCK"
        when amount_score + velocity_score + geo_score + device_score + temporal_score
             >= {{ var("medium_risk_threshold") }}  then "STEP_UP_AUTH"
        else "APPROVE"
    end as recommended_action,

    -- supporting context so analysts understand why it was flagged
    amount_zscore,
    baseline_avg,
    baseline_std,
    distance_km,
    travel_speed_kmh,
    prev_country,
    current_burst_5min,
    historical_max_5min,

    -- ground truth (only available in test/synthetic data)
    actual_fraud_flag,
    actual_fraud_type,

    current_timestamp() as scored_at

from temporal_scored
