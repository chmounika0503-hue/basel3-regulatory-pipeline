# Basel III Regulatory Compliance Reporting Pipeline 📊

## Overview
This project implements an end-to-end Big Data pipeline designed for the financial sector. It automates the calculation of core Basel III Liquidity and Capital metrics (CAR, LCR, NPL) using Apache Spark, orchestrated by Airflow, and served via a Plotly Dash dashboard.

## 🏗️ Architecture
- **Data Layer:** Simulated bank balance sheets generated via Python/Faker
- **Ingestion:** External industry benchmarks fetched via the FDIC BankFind API
- **Processing:** PySpark cluster performing distributed aggregations, window functions, and broadcast joins
- **Orchestration:** Apache Airflow DAG managing task dependencies, retries, and SLAs
- **Serving Layer:** PostgreSQL for metric storage and Plotly Dash for the regulatory UI

## 🛠️ Tech Stack
- **Language:** Python 3.13
- **Big Data:** Apache Spark (PySpark)
- **Orchestration:** Apache Airflow
- **Database:** PostgreSQL
- **Infrastructure:** Docker & Docker Compose
- **Visualization:** Plotly Dash

## 🚀 How to Run
1. Clone the repository
2. Ensure Docker Desktop is running
3. Run `docker-compose up -d`
4. Access Airflow at `http://localhost:8082` to trigger the pipeline
5. Run `python dashboard/app.py` to view results at `http://localhost:8050`