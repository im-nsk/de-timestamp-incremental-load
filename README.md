```markdown
# Timestamp-Based Incremental Data Pipeline using PySpark

An end-to-end Data Engineering project demonstrating how to build a **timestamp-based incremental data pipeline** using **PySpark**, **PostgreSQL**, **JDBC**, and **Snowflake**.

The project starts with a PostgreSQL-to-PostgreSQL incremental pipeline and gradually extends the same concepts to Snowflake.

It demonstrates practical Data Engineering concepts such as:

- Full Load
- Incremental Load
- Watermark Management
- JDBC Integration
- Staging Tables
- MERGE-Based Upserts
- Idempotent Reruns
- Delete Limitations
- Snowflake Integration
- RSA Key-Pair Authentication
- Spark and Connector Version Compatibility
- Production-Oriented Pipeline Design

---

# Architecture

The project currently contains two pipeline implementations.

## Pipeline 1: PostgreSQL → PostgreSQL

```text
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

## Pipeline 2: PostgreSQL → Snowflake

```text
                    Source PostgreSQL
                           │
                           │ JDBC
                           ▼
                    PySpark Pipeline
                           │
                           │ Snowflake Spark Connector
                           ▼
                       Snowflake
                           │
                           ▼
                DE_PROJECTS.BRONZE.CUSTOMERS
```

The PostgreSQL pipeline currently demonstrates the complete timestamp-based incremental-load flow.

The Snowflake pipeline currently demonstrates the initial PostgreSQL-to-Snowflake load with RSA key-pair authentication.

The next phase will extend the Snowflake pipeline with timestamp-based incremental loading.

---

# Tech Stack

- Python
- PySpark
- PostgreSQL
- Snowflake
- JDBC
- Snowflake Spark Connector
- SQL
- OpenSSL
- VS Code

---

# Repository Structure

```text
H1_Incremental_Load_TS_1/
│
├── data/
│   ├── watermark.txt
│   └── watermark_sf.txt
│
├── jars/
│   ├── postgresql-42.7.12.jar
│   ├── snowflake-jdbc-4.3.4.jar
│   └── spark-snowflake_2.13-3.2.2-spark_4.1.jar
│
├── sql/
│   ├── source_setup.sql
│   └── target_setup.sql
│
├── screenshots/
│
├── incremental_load.py
├── incremental_load_snowflake.py
├── config.py
├── requirements.txt
└── README.md
```

> **Security:** `config.py` should not contain production credentials in a public repository. Private Snowflake RSA keys are stored outside the project directory and are never committed to Git.

---

# PostgreSQL Pipeline

The first pipeline implements timestamp-based incremental loading from one PostgreSQL database to another PostgreSQL database.

```text
Source PostgreSQL
       │
       ▼
    PySpark
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
Watermark Update
```

---

# PostgreSQL Data Flow

The pipeline uses the `updated_at` column as the watermark.

For an incremental run, records are selected using the previously stored watermark:

```sql
WHERE updated_at > <last_watermark>
```

Only records that are newer than the previous watermark are processed.

The records are then:

```text
Source
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
```

---

# Incremental Load Logic

The pipeline follows this general flow:

```text
Start
  │
  ▼
Read Watermark
  │
  ├── Watermark does not exist
  │        │
  │        ▼
  │     Full Load
  │
  └── Watermark exists
           │
           ▼
    Read Changed Records
           │
           ▼
     Load Staging Table
           │
           ▼
       MERGE Target
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

# Full Load

During the first execution, no previous watermark exists.

The pipeline performs a full load:

```text
Source PostgreSQL
        │
        ▼
      PySpark
        │
        ▼
Staging
        │
        ▼
Target PostgreSQL
        │
        ▼
Create Watermark
```

The watermark is then stored for future incremental runs.

---

# Incremental Load

During subsequent executions, the pipeline reads the stored watermark.

For example:

```text
Previous Watermark:

2026-08-28 14:46:00
```

The pipeline reads records newer than this value:

```sql
WHERE updated_at > '2026-08-28 14:46:00'
```

This prevents the pipeline from processing the entire source table on every execution.

---

# Watermark Management

The watermark represents the latest successfully processed timestamp.

Example:

```text
Initial Run
    │
    ▼
Full Load
    │
    ▼
Watermark = 2026-08-28 14:46:00

Next Run
    │
    ▼
Read records where updated_at > watermark
    │
    ▼
Process changed records
    │
    ▼
Update watermark
```

The project currently uses a local watermark file for demonstration purposes.

A production implementation could store the watermark in a metadata table or another durable metadata store.

---

# Staging Table

Changed records are first loaded into a staging table.

```text
Source
  │
  ▼
PySpark
  │
  ▼
stg_customers
  │
  ▼
MERGE
  │
  ▼
customers
```

The staging table provides a temporary landing area before applying changes to the target.

This pattern is useful when the target operation requires transformations, validation, deduplication, or MERGE logic.

---

# MERGE-Based Upsert

The target table is updated using MERGE logic.

Conceptually:

```sql
MERGE INTO target AS t
USING staging AS s
ON t.customer_id = s.customer_id

WHEN MATCHED THEN
    UPDATE SET ...

WHEN NOT MATCHED THEN
    INSERT (...);
```

This allows the pipeline to handle:

```text
Existing customer
       │
       ▼
     UPDATE

New customer
       │
       ▼
     INSERT
```

---

# Idempotent Reruns

The pipeline is designed so that rerunning the same batch does not continuously create duplicate target records.

For example:

```text
Run 1
Source → Staging → MERGE → Target

Run 2
Same data → Staging → MERGE → Target
```

The MERGE operation ensures that existing records are updated rather than blindly inserted again.

This is an important property of reliable ETL pipelines.

---

# Delete Limitation

Timestamp-based incremental loading has an important limitation.

It can identify records whose timestamps changed, but a DELETE operation does not update the `updated_at` column.

Example:

```text
Source

ID   Name       Updated_At
1    Nishant    09:00
2    Abhay      10:00
3    Yasir      11:00
```

If ID `2` is deleted:

```text
Source

ID   Name       Updated_At
1    Nishant    09:00
3    Yasir      11:00
```

There is no timestamp event telling the incremental pipeline:

```text
"Customer 2 was deleted."
```

Therefore, the target may continue to contain ID `2`.

---

# Incremental Loading vs CDC

This project demonstrates an important distinction between **Incremental Loading** and **Change Data Capture (CDC)**.

Timestamp-based incremental loading generally detects:

```text
INSERT
UPDATE
```

when the timestamp column is correctly maintained.

It does not inherently detect:

```text
DELETE
```

True CDC mechanisms can capture:

```text
INSERT
UPDATE
DELETE
```

Examples of approaches that can address deletes include:

- Trigger-Based CDC
- Log-Based CDC
- Soft Deletes
- Reconciliation Jobs
- Debezium-based CDC

---

# Snowflake Pipeline

The project also extends the pipeline to Snowflake.

The current Snowflake architecture is:

```text
PostgreSQL
    │
    │ JDBC
    ▼
PySpark
    │
    │ Snowflake Spark Connector
    ▼
Snowflake
    │
    ▼
DE_PROJECTS.BRONZE.CUSTOMERS
```

---

# Snowflake Environment

The Snowflake environment contains:

```text
DE_PROJECTS
│
└── BRONZE
    │
    └── CUSTOMERS
```

The Snowflake warehouse used for the project is:

```text
DE_WH
```

The warehouse provides the compute resources used to execute Snowflake operations.

---

# Snowflake Authentication

The Snowflake pipeline uses **RSA key-pair authentication** instead of storing a Snowflake password in the Spark connection.

The authentication model is:

```text
Local Machine
│
├── Private RSA Key
│       │
│       ▼
│    PySpark
│       │
│       ▼
│    Snowflake
│
└── Public RSA Key
        │
        ▼
 Snowflake User
```

The public key is registered with the Snowflake user.

The private key remains on the local machine.

```text
rsa_key.p8
```

must never be committed to GitHub.

---

# RSA Key Storage

The private key is intentionally stored outside the project directory.

Example:

```text
~/snowflake_keys/rsa_key.p8
```

The Python application reads the key from the local machine when the Spark job starts.

The key is not stored directly inside the source code.

This reduces the risk of accidentally committing the private key to Git.

---

# Snowflake Initial Load

The current Snowflake implementation performs the initial load:

```text
PostgreSQL
    │
    ▼
PySpark
    │
    ▼
Snowflake
    │
    ▼
BRONZE.CUSTOMERS
```

The pipeline can be executed using:

```bash
python incremental_load_snowflake.py
```

The current implementation successfully loads the PostgreSQL source data into Snowflake.

---

# Snowflake Incremental Load

The next implementation phase will extend the Snowflake pipeline using the same timestamp-based incremental-load concept.

The planned flow is:

```text
PostgreSQL
    │
    ▼
Read Snowflake Watermark
    │
    ▼
Read Changed Records
    │
    ▼
PySpark
    │
    ▼
Snowflake Staging
    │
    ▼
MERGE
    │
    ▼
Snowflake Target
    │
    ▼
Update Watermark
```

The existing incremental-load concepts will therefore be reused rather than creating a completely different pipeline design.

---

# Project Phases

## Phase 1 — Initial Full Load

- Connect PySpark to PostgreSQL
- Read source data using JDBC
- Load the target table
- Validate row counts

## Phase 2 — Watermark-Based Incremental Load

- Introduce `updated_at` as the watermark
- Read only new or modified records
- Store the latest processed timestamp

## Phase 3 — Idempotent Reruns

- Rerun the same batch
- Prevent duplicate target records
- Use MERGE-based upserts

## Phase 4 — Demonstrate Delete Limitation

- Delete a record from the source
- Run the timestamp-based pipeline
- Observe that the target does not automatically detect the delete

## Phase 5 — MERGE-Based Upsert

- Load changed records into staging
- MERGE staging data into the target
- Handle INSERT and UPDATE operations

## Phase 6 — PostgreSQL → Snowflake

- Create Snowflake warehouse
- Create database and schema
- Configure Snowflake Spark Connector
- Configure RSA key-pair authentication
- Connect PySpark to Snowflake
- Perform initial data load

## Phase 7 — Snowflake Incremental Load

- Reuse the timestamp watermark approach
- Read only new or modified PostgreSQL records
- Load changes into Snowflake
- Implement Snowflake staging
- Implement MERGE-based upsert
- Update the Snowflake watermark

## Phase 8 — Advanced Improvements

Potential future enhancements include:

- Metadata-table-based watermark management
- Structured logging
- Data-quality checks
- Better configuration management
- Failure handling and retries
- Dockerization
- Airflow orchestration
- Log-based CDC with Debezium

---

# What You Can Learn

By working through this project, you can learn how to:

### Data Engineering Fundamentals

- Design an incremental ETL pipeline
- Understand full vs incremental loads
- Use timestamps as watermarks
- Manage pipeline state
- Design staging and target layers

### PySpark

- Create Spark applications
- Read relational databases using JDBC
- Write DataFrames to databases
- Work with Spark DataFrames
- Debug Spark execution issues

### PostgreSQL

- Configure source and target databases
- Work with staging tables
- Use MERGE-based upserts
- Understand timestamp-based change detection

### Snowflake

- Understand Snowflake databases, schemas, tables, and warehouses
- Connect PySpark to Snowflake
- Use the Snowflake Spark Connector
- Configure Snowflake JDBC connectivity
- Implement RSA key-pair authentication
- Understand Snowflake compute through warehouses

### Pipeline Reliability

- Design idempotent pipelines
- Manage watermarks
- Understand duplicate processing
- Understand delete limitations
- Separate source extraction from target loading
- Debug failures layer by layer

### Production-Oriented Thinking

- Separate configuration from application logic
- Protect credentials and private keys
- Understand connector/runtime compatibility
- Think about failure recovery
- Understand why simple timestamp loading may not be sufficient for production CDC requirements

---

# Setup

## Clone Repository

```bash
git clone https://github.com/<your-username>/<your-repository>.git
cd <your-repository>
```

## Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

# PostgreSQL Setup

Execute the SQL scripts in order:

```text
1. source_setup.sql
2. target_setup.sql
```

These scripts create the required PostgreSQL source and target environments.

Example:

```text
source_db
└── customers

target_db
├── stg_customers
└── customers
```

---

# PostgreSQL JDBC Driver

Download the PostgreSQL JDBC driver and place it inside:

```text
jars/
```

Example:

```text
jars/
└── postgresql-42.7.12.jar
```

---

# Snowflake Setup

Create the required Snowflake objects:

```sql
CREATE WAREHOUSE DE_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS DE_PROJECTS;

CREATE SCHEMA IF NOT EXISTS DE_PROJECTS.BRONZE;

USE WAREHOUSE DE_WH;
USE DATABASE DE_PROJECTS;
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS CUSTOMERS (
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING,
    CITY STRING,
    UPDATED_AT TIMESTAMP
);
```

---

# Snowflake Connector

The project uses:

```text
Spark: 4.1.1
Scala: 2.13
Snowflake Spark Connector: 3.2.2 for Spark 4.1
Snowflake JDBC: 4.3.4
```

The required JARs are stored in:

```text
jars/
```

---

# RSA Key-Pair Setup

Generate a private RSA key:

```bash
mkdir -p ~/snowflake_keys

openssl genrsa 2048 | \
openssl pkcs8 -topk8 -inform PEM \
-out ~/snowflake_keys/rsa_key.p8 \
-nocrypt
```

Generate the corresponding public key:

```bash
openssl rsa \
-in ~/snowflake_keys/rsa_key.p8 \
-pubout \
-out ~/snowflake_keys/rsa_key.pub
```

The files should exist at:

```text
~/snowflake_keys/
├── rsa_key.p8
└── rsa_key.pub
```

Register the public key with the Snowflake user.

The private key remains on the local machine.

---

# Running the PostgreSQL Pipeline

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Run:

```bash
python incremental_load.py
```

---

# Running the Snowflake Pipeline

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Run:

```bash
python incremental_load_snowflake.py
```

The current implementation reads the PostgreSQL source using JDBC and loads the data into:

```text
DE_PROJECTS.BRONZE.CUSTOMERS
```

---

# Security

Never commit the following to GitHub:

```text
Private RSA keys
Database passwords
Snowflake passwords
API keys
Access tokens
Credential files
```

Recommended `.gitignore` entries:

```gitignore
.venv/
__pycache__/
*.pyc

*.log

.DS_Store

snowflake_keys/
*.p8
*.pem
```

For production systems, credentials should preferably be managed using environment variables, cloud secret managers, or dedicated secrets-management solutions.

---

# Important Design Considerations

## Timestamp-Based Incremental Loading

Timestamp-based loading depends on the source timestamp being correctly maintained.

If an UPDATE occurs without updating the timestamp, the change may not be detected.

If a DELETE occurs, there may be no timestamp event at all.

Therefore, timestamp-based incremental loading is simpler than true CDC but has important limitations.

---

# Incremental Loading vs True CDC

```text
Timestamp-Based Incremental Load

INSERT   → Usually detected
UPDATE   → Usually detected
DELETE   → Not inherently detected


True CDC

INSERT   → Captured
UPDATE   → Captured
DELETE   → Captured
```

The appropriate approach depends on the source system, latency requirements, data volume, reliability requirements, and business needs.

---

# Future Improvements

The project can be extended with:

- Metadata-table-based watermark management
- Snowflake incremental loading
- Snowflake MERGE implementation
- Structured logging
- Data-quality validation
- Error handling
- Retry mechanisms
- Pipeline monitoring
- Reconciliation jobs
- Docker
- Airflow
- Cloud deployment
- Log-based CDC
- Debezium
- Automated testing
- CI/CD

---

# Author

**Nishant Kumar**

Data Engineer | Big Data | PySpark | AWS | SQL

LinkedIn: https://linkedin.com/in/im-nsk
```