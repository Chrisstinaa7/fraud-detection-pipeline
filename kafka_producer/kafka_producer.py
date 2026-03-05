"""
kafka_producer.py

Generates synthetic banking transactions and publishes them to Confluent Cloud.
Runs once to seed the pipeline with test data - not meant to be a long-running process.

Usage:
    python kafka_producer/kafka_producer.py
    python kafka_producer/kafka_producer.py --customers 500 --txns-per-customer 50
"""

import json
import os
import random
import argparse
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "banking.transactions"

CITIES = {
    "US": [("New York", 40.7128, -74.0060), ("Chicago", 41.8781, -87.6298),
           ("Los Angeles", 34.0522, -118.2437), ("Houston", 29.7604, -95.3698)],
    "GB": [("London", 51.5074, -0.1278), ("Manchester", 53.4808, -2.2426)],
    "JP": [("Tokyo", 35.6762, 139.6503), ("Osaka", 34.6937, 135.5023)],
    "AU": [("Sydney", -33.8688, 151.2093), ("Melbourne", -37.8136, 144.9631)],
    "DE": [("Berlin", 52.5200, 13.4050), ("Munich", 48.1351, 11.5820)],
}

MERCHANTS = {
    "retail":      ["Amazon", "Walmart", "Target", "IKEA"],
    "grocery":     ["Whole Foods", "Kroger", "Trader Joe's"],
    "dining":      ["McDonald's", "Chipotle", "Shake Shack"],
    "fuel":        ["Shell", "BP", "Chevron"],
    "electronics": ["Best Buy", "Apple Store", "Samsung"],
}


@dataclass
class Customer:
    id: str
    avg_spend: float
    std_spend: float
    country: str
    city: str
    lat: float
    lon: float
    device_id: str
    active_hours: tuple[int, int]  # (start_hour, end_hour)


def make_customers(n: int) -> list[Customer]:
    customers = []
    country_weights = {"US": 0.60, "GB": 0.15, "JP": 0.12, "AU": 0.08, "DE": 0.05}
    countries = list(country_weights.keys())
    weights = list(country_weights.values())

    for i in range(n):
        country = random.choices(countries, weights=weights)[0]
        city, lat, lon = random.choice(CITIES[country])
        high_value = (i % 8 == 0)

        customers.append(Customer(
            id=f"C{i:06d}",
            avg_spend=random.uniform(800, 3000) if high_value else random.uniform(40, 250),
            std_spend=random.uniform(150, 400)  if high_value else random.uniform(15, 60),
            country=country,
            city=city,
            lat=lat,
            lon=lon,
            device_id=f"dev_{i:08x}",
            active_hours=(random.randint(7, 10), random.randint(18, 22)),
        ))
    return customers


_txn_seq = 0

def _next_id() -> str:
    global _txn_seq
    _txn_seq += 1
    return f"T{_txn_seq:010d}"


def normal_txn(c: Customer, ts: datetime) -> dict:
    hour = random.randint(*c.active_hours)
    ts = ts.replace(hour=hour, minute=random.randint(0, 59))
    category = random.choice(list(MERCHANTS))
    return {
        "transaction_id":        _next_id(),
        "customer_id":           c.id,
        "amount":                round(max(1.0, random.gauss(c.avg_spend, c.std_spend)), 2),
        "currency":              "USD",
        "merchant_name":         random.choice(MERCHANTS[category]),
        "merchant_category":     category,
        "transaction_timestamp": ts.isoformat(),
        "country":               c.country,
        "city":                  c.city,
        "latitude":              round(c.lat + random.uniform(-0.05, 0.05), 6),
        "longitude":             round(c.lon + random.uniform(-0.05, 0.05), 6),
        "device_id":             c.device_id,
        "is_fraud":              False,
        "fraud_type":            None,
    }


def fraud_txn(c: Customer, ts: datetime, fraud_type: str) -> dict:
    base = normal_txn(c, ts)
    base["is_fraud"]   = True
    base["fraud_type"] = fraud_type

    if fraud_type == "amount_anomaly":
        base["amount"] = round(c.avg_spend * random.uniform(6, 12), 2)

    elif fraud_type == "geo_anomaly":
        # pick a country that isn't theirs
        other = random.choice([co for co in CITIES if co != c.country])
        city, lat, lon = random.choice(CITIES[other])
        base["country"]   = other
        base["city"]      = city
        base["latitude"]  = round(lat, 6)
        base["longitude"] = round(lon, 6)

    elif fraud_type == "device_anomaly":
        base["device_id"] = f"dev_unknown_{random.randint(10000, 99999):x}"

    elif fraud_type == "velocity_burst":
        # caller is responsible for sending multiple in quick succession
        pass

    return base


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":   os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
        "security.protocol":   "SASL_SSL",
        "sasl.mechanisms":     "PLAIN",
        "sasl.username":       os.environ["CONFLUENT_API_KEY"],
        "sasl.password":       os.environ["CONFLUENT_API_SECRET"],
        "acks":                "all",
        "compression.type":    "lz4",
        "linger.ms":           10,
    })


def on_delivery(err, msg):
    if err:
        log.warning("delivery failed: %s", err)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers",          type=int, default=200)
    parser.add_argument("--txns-per-customer",  type=int, default=30)
    parser.add_argument("--fraud-rate",         type=float, default=0.05)
    args = parser.parse_args()

    log.info("building %d customer profiles", args.customers)
    customers = make_customers(args.customers)
    producer  = build_producer()

    fraud_types = ["amount_anomaly", "geo_anomaly", "device_anomaly"]
    base_time   = datetime.now() - timedelta(days=7)

    sent = 0
    fraud_sent = 0

    for customer in customers:
        for i in range(args.txns_per_customer):
            ts = base_time + timedelta(minutes=random.randint(0, 7 * 1440))

            if random.random() < args.fraud_rate:
                txn = fraud_txn(customer, ts, random.choice(fraud_types))
                fraud_sent += 1
            else:
                txn = normal_txn(customer, ts)

            producer.produce(
                topic=TOPIC,
                key=customer.id,
                value=json.dumps(txn),
                callback=on_delivery,
            )
            sent += 1

            if sent % 500 == 0:
                producer.poll(0)
                log.info("sent %d  (fraud: %d)", sent, fraud_sent)

    producer.flush()
    log.info("done — %d total, %d fraud (%.1f%%)", sent, fraud_sent, fraud_sent / sent * 100)


if __name__ == "__main__":
    main()
