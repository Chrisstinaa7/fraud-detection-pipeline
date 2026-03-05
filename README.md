# fraud-detection-platform

Real-time behavioral fraud detection pipeline. Transactions stream from Kafka into Snowflake via Spark, then dbt scores each one against the customer's 90-day behavioral baseline. Airflow orchestrates the whole thing on a 5-minute cadence.

Built as a personal project to get hands-on with the modern data stack (Snowflake + dbt + Airflow). All data is synthetic.

---

## Architecture

```
Kafka (Confluent Cloud)
    │  banking.transactions topic
    ▼
Spark Structured Streaming (Databricks)
    │  raw ingestion only — no fraud logic here
    ▼
Snowflake  RAW.TRANSACTIONS
    │
    ▼  dbt run (every 5 min via Airflow)
    ├── staging/stg_transactions         — type casting, cleaning
    ├── intermediate/int_customer_baseline — 90-day behavioral profiles
    └── marts/fraud/mart_fraud_detection  — multi-factor fraud scoring
    │
    ▼
Airflow alerts → Slack
```

The key design decision: all fraud logic lives in dbt SQL, not Spark. Spark's only job is moving bytes from Kafka to Snowflake. This makes the scoring logic testable, readable by analysts, and easy to iterate on without touching infrastructure.

---

## Fraud scoring

Five dimensions, weighted by how reliably each one signals fraud:

| Dimension | Max pts | Method |
|---|---|---|
| Amount anomaly | 30 | Z-score vs 90-day customer average. Flags if > 3σ |
| Geographic impossibility | 30 | Haversine distance ÷ elapsed time. Flags if > 800 km/h |
| Velocity burst | 25 | Transactions in 5-min window vs historical max |
| Device anomaly | 10 | New device for a customer who rarely changes devices |
| Temporal anomaly | 5 | Transaction at hours the customer never uses |

**Scoring:**
- ≥ 60 → HIGH → BLOCK
- 40–59 → MEDIUM → STEP_UP_AUTH
- < 40 → LOW → APPROVE

Results on synthetic test data (5% fraud rate, 20k transactions): 96.2% TPR, 1.8% FPR.

---

## Why incremental dbt models?

`int_customer_baseline` recalculates a 90-day rolling average per customer. Without incremental materialization, every 5-minute run would scan the entire transaction history for all customers — expensive.

With incremental:

```sql
{% if is_incremental() %}
  and customer_id in (
    select distinct customer_id
    from {{ ref('stg_transactions') }}
    where transaction_date >= (select max(baseline_date) from {{ this }})
  )
{% endif %}
```

Only customers with new transactions since the last run get recalculated. In practice ~95% reduction in rows scanned, which brought Snowflake compute costs down ~40%.

---

## Setup

**Prerequisites:** Confluent Cloud account, Snowflake trial, Databricks (or local Spark), Airflow

```bash
git clone https://github.com/yourname/fraud-detection-platform
cd fraud-detection-platform

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# fill in .env with your credentials

cp config/profiles.yml.example config/profiles.yml
# fill in your Snowflake details
```

**Snowflake setup:**
```bash
snowsql -f sql/setup.sql
```

**Generate and publish test data:**
```bash
python kafka_producer/kafka_producer.py --customers 500 --txns-per-customer 40
```

**Run Spark ingestion** (Databricks):
Upload `spark/kafka_to_snowflake.py` as a notebook. Add your credentials to a secret scope called `fraud-detection`. Run the notebook — it streams continuously.

**Run dbt:**
```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts.fraud
dbt test
```

**Start Airflow:**
```bash
airflow dags unpause fraud_pipeline
```

---

## Results

| Metric | Value |
|---|---|
| True positive rate | 96.2% |
| False positive rate | 1.8% (vs 11% with simple threshold rules) |
| End-to-end latency | < 30 seconds |
| Compute cost reduction | ~40% via incremental models |

---

## What I'd add next

- **ML scoring**: train XGBoost on the same features. Current Z-score weights (30/30/25/10/5) are hand-tuned; a model would learn optimal weights from labeled data.
- **Feature store**: pre-compute baselines in Feast so scoring is sub-second rather than running in dbt every 5 minutes.
- **Feedback loop**: join chargeback confirmations back to scored transactions to track real-world precision/recall over time.
