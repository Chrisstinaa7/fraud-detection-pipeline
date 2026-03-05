{{ config(materialized="view") }}

-- thin staging layer, just casting and light cleaning
-- no business logic here - that all lives in intermediate/marts

with source as (
    select * from {{ source("raw", "transactions") }}
),

cleaned as (
    select
        transaction_id,
        customer_id,

        cast(amount as decimal(18, 2))          as amount,
        upper(trim(currency))                   as currency,

        trim(merchant_name)                     as merchant_name,
        lower(trim(merchant_category))          as merchant_category,

        cast(transaction_timestamp as timestamp) as transaction_timestamp,
        cast(transaction_timestamp as date)      as transaction_date,

        upper(trim(country))                    as country,
        trim(city)                              as city,
        cast(latitude as float)                 as latitude,
        cast(longitude as float)                as longitude,

        device_id,

        -- only exists in synthetic test data, won't be present in real pipeline
        is_fraud        as actual_fraud_flag,
        fraud_type      as actual_fraud_type,

        ingested_at     as _ingested_at

    from source
    where transaction_id is not null
)

select * from cleaned
