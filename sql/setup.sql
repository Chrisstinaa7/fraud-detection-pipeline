-- run once to set up Snowflake infrastructure
-- replace FRAUD_DB with whatever you put in .env

create warehouse if not exists FRAUD_WH
    warehouse_size = 'x-small'
    auto_suspend   = 60
    auto_resume    = true
    initially_suspended = true;

create database if not exists FRAUD_DB;

create schema if not exists FRAUD_DB.raw;
create schema if not exists FRAUD_DB.staging;
create schema if not exists FRAUD_DB.intermediate;
create schema if not exists FRAUD_DB.fraud;

-- raw table — Spark writes here, nothing else touches it
create table if not exists FRAUD_DB.raw.transactions (
    transaction_id        varchar(50)   not null,
    customer_id           varchar(50)   not null,
    amount                float         not null,
    currency              varchar(3),
    merchant_name         varchar(200),
    merchant_category     varchar(100),
    transaction_timestamp varchar(50)   not null,
    country               varchar(10),
    city                  varchar(100),
    latitude              float,
    longitude             float,
    device_id             varchar(100),
    is_fraud              boolean,
    fraud_type            varchar(50),
    kafka_timestamp       timestamp_ntz,
    ingested_at           timestamp_ntz default current_timestamp()
)
cluster by (to_date(transaction_timestamp));
