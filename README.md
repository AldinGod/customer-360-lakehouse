
# Customer 360 Lakehouse on LocalStack

This project is an end-to-end **data platform** built locally using **LocalStack (AWS emulation)**, **Apache Spark**, **Airflow**, and **PostgreSQL**.

The goal is to build a **Customer 360 Lakehouse** that combines:
- Application events (logins, page views, add_to_cart, errors)
- Transactional data (orders, payments)
- Support data (tickets, CSAT scores)

### High-Level Architecture

- **Ingestion**
  - Python services generate fake events and send them to LocalStack (SNS → SQS).
  - Raw data lands in an S3-based *bronze* zone.

- **Data Lake (Bronze → Silver → Gold)**
  - Apache Spark reads bronze data from S3, cleans and normalizes it into the *silver* zone.
  - Aggregations and Customer 360 features are computed into the *gold* zone (Parquet, partitioned).

- **Orchestration & Data Quality**
  - Airflow DAGs orchestrate ingestion, Spark jobs, and data quality checks.
  - Great Expectations or custom checks validate schemas and data quality.

- **Serving & Analytics**
  - Gold data is loaded into PostgreSQL as a simple **warehouse layer**.
  - A dashboard (Streamlit or Metabase) visualizes key KPIs:
    - Revenue, retention, churn proxy, customer segments, support metrics.

### Main Tech Stack

- **Infrastructure:** Docker, Docker Compose, LocalStack (S3, SNS, SQS)
- **Processing:** Apache Spark (batch + micro-batch), Python
- **Orchestration:** Apache Airflow
- **Data Quality:** Great Expectations (or custom Spark checks)
- **Storage:** S3-style Data Lake (bronze/silver/gold), PostgreSQL
- **Analytics:** Streamlit or Metabase


