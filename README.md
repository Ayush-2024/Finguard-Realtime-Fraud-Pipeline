# 🛡️ FinGuard — Real-Time Fraud Detection & Transaction Monitoring Pipeline using PySpark, Databricks & Delta Lake

## 📌 Project Objective

To design and implement a real-time, end-to-end data engineering pipeline for credit card fraud detection using **PySpark Structured Streaming**, **Databricks Lakeflow Declarative Pipelines**, and **Delta Lake**. The project focuses on streaming ingestion, fraud watchlist matching, high-value transaction detection, automated alerting, and self-serve analytics — built the way a real fraud monitoring system at a bank would be structured.

---

## 📁 Data Source

All data in this project is synthetically generated to simulate a live banking environment:

- **Transactions** — simulated credit card transaction events streamed continuously into a **Confluent Kafka** topic (`credit_card_transactions`).
- **Fraud watchlist** — a Databricks notebook (`fraud_watchlist_file_generator`) reads a seed `fraud_watchlist.csv` and writes it out **one JSON file per row**, with a delay between writes, into a Unity Catalog volume — simulating a live, incrementally-arriving watchlist feed for Auto Loader to pick up.
- **Customers** — customer master data (profile, risk score, spending preferences, trusted device, transaction limits) sourced from a **PostgreSQL** database and ingested through a dedicated `finguard_customers_ingestion` pipeline.

---

## 🏗️ Architecture

![FinGuard Architecture](https://github.com/Ayush-2024/Finguard-Realtime-Fraud-Pipeline/blob/main/Finguard_Project_Architecture.jpg?raw=true)

## 🔗 Pipeline Lineage (Databricks Lakeflow DAG)

The graph below is the actual table-level lineage produced by the Lakeflow Declarative Pipeline — showing how each source flows through bronze → silver → gold, and where the alert tables fan out to the email notifiers.

![FinGuard Pipeline Lineage](https://github.com/Ayush-2024/Finguard-Realtime-Fraud-Pipeline/blob/main/Pipeline%20Lineage%20(Databricks%20Lakeflow%20DAG).jpg?raw=true)

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Streaming Source | Confluent Kafka (SASL_SSL) |
| Ingestion | Databricks Auto Loader (`cloudFiles`), Structured Streaming |
| Processing Engine | PySpark / Databricks Lakeflow Declarative Pipelines |
| Storage | Delta Lake (Medallion: Bronze / Silver / Gold) |
| Stream Joins | Watermarked stream-stream & stream-static joins |
| Alerting | Python `smtplib` (Gmail SMTP) — HTML email templates |
| Analytics | Databricks AI/BI Dashboard, Databricks Genie Space |

---

## 🔄 Pipeline Breakdown

### 🥉 Bronze Layer
- **`finguard_bronze_transactions`** — streams raw credit card transactions from a Confluent Kafka topic with full Kafka metadata (offset, partition, timestamp) preserved.
- **`finguard.bronze.fraud_watchlist`** — ingests fraud watchlist JSON files via Auto Loader with schema inference and rescued-data handling.
- **`finguard.bronze.customers`** — ingests customer master data via the `finguard_customers_ingestion` pipeline.

### 🥈 Silver Layer
- **`finguard_silver_transactions`** — parses the Kafka `value` payload against a strict JSON schema (transaction ID, card number, merchant, amount, channel, geo, etc.) and standardizes types.
- **`finguard.silver.fraud_watchlist`** — normalizes watchlist entries (uppercased IDs/risk levels/actions, typed timestamps).
- **`finguard.silver.customers`** — cleaned customer profiles including risk score, preferred spending range, trusted device, and transaction limits, with `@dp.expect_or_drop` data quality enforcement.

### 🥇 Gold Layer
- **`fraud_card_alert`** — watermarked stream-stream join between live transactions and the fraud watchlist (matched on card number), enriched with customer identity — flags a fraud hit in real time.
- **`high_value_transactions_alert`** — flags transactions that exceed a customer's personal transaction limit.
- **`transaction_count_by_minute`** — tumbling 1-minute windowed transaction volume.
- **`transaction_count_by_minute_sliding_window`** — 5-minute sliding window (1-minute slide) for smoother trend analysis.

---

## 📧 Real-Time Alerting

Two `foreach_batch_sink` streaming sinks read off the Gold layer and dispatch rich HTML emails via Gmail SMTP the instant a row lands:

- 🚨 **Fraud Alert Notifier** — urgent fraud-watchlist-match emails with full transaction + watchlist context and required customer actions.
- ⚠️ **High-Value Alert Notifier** — notifies customers when a transaction exceeds their configured spending limit.

---

## 📊 Analytics Layer

- **Fraud Monitoring AI/BI Dashboard** — real-time counters (alerts today, transactions today, avg. transaction amount, high-risk customers) plus charts for alert trends, top customers/merchants by alert count, alert type/status breakdown, transaction volume by minute, merchant category spend, domestic vs. international split, a country heatmap, and payment channel distribution.
- **Genie Space** — natural-language Q&A directly over the fraud and transaction gold tables.

---

## 🔐 Security Note

All credentials (Kafka API keys, SMTP app passwords) are managed via **Databricks Secrets** (`dbutils.secrets`) rather than hardcoded — never commit real credentials to source control. Card numbers are masked (last 4 digits only) in every outbound alert email.

---

## 🚀 Key Highlights

- ⚡ **Fully streaming, end-to-end** — from Kafka ingestion to customer email, with no batch step in the fraud-detection path.
- 🧩 **Medallion architecture** done right — clear separation of raw, cleaned, and business-ready layers.
- 🔗 **Watermarked stream joins** — correctly handles late-arriving data in a real-time fraud match.
- 📬 **Production-style alerting** — polished, templated HTML emails, not just log lines.
- 📈 **Self-serve analytics** — dashboard + Genie Space so non-technical stakeholders can explore the data themselves.

