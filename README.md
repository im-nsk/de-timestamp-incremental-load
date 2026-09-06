# Timestamp-Based Incremental Data Pipeline using PySpark & PostgreSQL

An end-to-end Data Engineering project demonstrating how to build a timestamp-based incremental data pipeline using **PySpark**, **PostgreSQL**, and **JDBC**.

This project simulates a production-style ETL pipeline that loads only new or updated records using a watermark while ensuring idempotent reruns.

---

## Tech Stack

- Python
- PySpark
- PostgreSQL
- JDBC
- SQL
- VS Code

---

## Project Architecture

```
           Source PostgreSQL
                  │
                  ▼
          PySpark Pipeline
                  │
                  ▼
          Staging Table
                  │
                  ▼
              MERGE
                  │
                  ▼
          Target PostgreSQL
                  │
                  ▼
        Update Watermark
```

---

## Features

- Full Load
- Incremental Load
- Watermark Management
- JDBC Integration
- MERGE-based Upsert
- Staging Table
- Idempotent Reruns
- Production-like Folder Structure

---

## Project Flow

```
Start
  │
  ▼
Read Watermark
  │
  ├── No → Full Load
  │
  └── Yes → Incremental Load
              │
              ▼
Read Changed Records
              │
              ▼
Load Staging Table
              │
              ▼
MERGE into Target
              │
              ▼
Update Watermark
              │
              ▼
Truncate Staging
              │
              ▼
End
```

---

## Project Structure

```
de-timestamp-incremental-load/
│
├── data/
├── jars/
├── sql/
├── screenshots/
├── incremental_load.py
├── requirements.txt
└── README.md
```

---

## Setup

### Clone Repository

```bash
git clone https://github.com/<your-username>/de-timestamp-incremental-load.git
cd de-timestamp-incremental-load
```

### Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Download PostgreSQL JDBC Driver

Download the PostgreSQL JDBC driver and place it inside:

```
jars/
```

---

## Database Setup

Execute the SQL scripts in order:

1. `source_setup.sql`
2. `target_setup.sql`

These scripts create:

- source_db
- target_db
- customers
- stg_customers

---

## Run

```bash
python incremental_load.py
```

---

## Project Phases

### Phase 1

- Initial Full Load

### Phase 2

- Watermark-Based Incremental Load

### Phase 3

- Idempotent Reruns

### Phase 4

- Demonstrate Delete Limitation

### Phase 5

- MERGE-based Upsert

---

## What I Learned

- Connecting Spark with PostgreSQL using JDBC
- Implementing timestamp-based incremental loading
- Watermark management
- MERGE-based upserts
- Idempotent ETL design
- Why timestamp-based loading cannot detect deletes

---

## Current Limitation

Timestamp-based incremental loading cannot detect deleted records because deletes do not update the timestamp column.

Production systems typically address this using:

- Trigger-Based CDC
- Log-Based CDC
- Soft Deletes
- Reconciliation Jobs

---

## Future Improvements

- Store watermark in a metadata table
- Add logging
- Add configuration management
- Dockerize the project
- Orchestrate with Airflow
- Replace timestamp loading with Debezium-based CDC

---

## Author

**Nishant Kumar**

Data Engineer | Big Data | PySpark | AWS | SQL

LinkedIn: https://linkedin.com/in/im-nsk