
# Data Vault Loading with Airflow and Snowflake

This project implements an incremental data vault loading framework using Apache Airflow and Snowflake.

## Architecture Overview

### Flow Management Control (FMC) Components

1. Initial Setup (`SET_FMC_MTD_FL_INCR_DTA_INIT`):
- Handles core FMC history records
- Manages load cycle information
- Sets up initial window tables

2. Parallel Object Processing:
- `SET_FMC_MTD_FL_INCR_DTA_CUSTOMERS`
- `SET_FMC_MTD_FL_INCR_DTA_PRODUCTS` 
- `SET_FMC_MTD_FL_INCR_DTA_SALES_TRANSACTIONS`

### Data Vault Loading Components

The following mappings are executed after FMC setup:

```text
Extract Layer:
- EXT_DTA_CUSTOMERS_INCR
- EXT_DTA_SALESTRANSACTIONS_INCR

Staging Layer:  
- STG_DTA_CUSTOMERS_INCR
- STG_DTA_SALESTRANSACTIONS_INCR

Hub Layer:
- HUB_DTA_CUSTOMERS_INCR
- HUB_DTA_SALES_TRANSACTIONS_INCR 

Satellite Layer:
- SAT_DTA_SALESTRANSACTIONS_INCR
- SAT_DTA_CUSTOMERS_INCR

Link Layer:
- LNK_DTA_SALESTRANSACTIONS_CUSTOMERS_INCR
- LKS_DTA_SALESTRANSACTIONS_CUSTOMERS_INCR
```

## Implementation

### Airflow DAG Structure

```python
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from vs_fmc_plugin.operators.snowflake_operator import SnowflakeOperator

# FMC Initial Setup
fmc_init = SnowflakeOperator(
    task_id="fmc_init", 
    snowflake_conn_id="SNOW_COL", 
    sql="""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_INIT"(
        '{{ dag_run.dag_id }}', 
        '{{ dag_run.id }}', 
        '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S.%f") }}'
    );""", 
    autocommit=False, 
    dag=INCR
)

# Parallel FMC Object Processing
fmc_customers = SnowflakeOperator(
    task_id="fmc_customers",
    snowflake_conn_id="SNOW_COL",
    sql="""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_CUSTOMERS"(...);""",
    autocommit=False,
    dag=INCR  
)

fmc_products = SnowflakeOperator(
    task_id="fmc_products",
    snowflake_conn_id="SNOW_COL", 
    sql="""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_PRODUCTS"(...);""",
    autocommit=False,
    dag=INCR
)

fmc_sales = SnowflakeOperator(
    task_id="fmc_sales",
    snowflake_conn_id="SNOW_COL",
    sql="""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_SALES_TRANSACTIONS"(...);""",
    autocommit=False,
    dag=INCR
)

# FMC Sync Point
fmc_sync = DummyOperator(task_id="fmc_sync", dag=INCR)

# Dependencies
fmc_init >> [fmc_customers, fmc_products, fmc_sales] >> fmc_sync
```

### Required Snowflake Objects

#### FMC Tables

```sql
CREATE TABLE "ColruytFMC_FMC"."FMC_LOADING_HISTORY" (
    "dag_name" VARCHAR,
    "SRC_BK" VARCHAR,
    "LOAD_CYCLE_ID" INTEGER,
    "LOAD_DATE" TIMESTAMP,
    "FMC_BEGIN_LW_TIMESTAMP" TIMESTAMP,
    "FMC_END_LW_TIMESTAMP" TIMESTAMP,
    "load_start_date" TIMESTAMP,
    "load_end_date" TIMESTAMP,
    "success_flag" INTEGER
);

CREATE TABLE "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" (
    "SRC_BK" VARCHAR,
    "LOAD_CYCLE_ID" INTEGER,
    "FMC_BEGIN_LW_TIMESTAMP" TIMESTAMP,
    "FMC_END_LW_TIMESTAMP" TIMESTAMP,
    "load_start_date" TIMESTAMP,
    "load_end_date" TIMESTAMP,
    "success_flag" INTEGER,
    "OBJECT_NAME" VARCHAR
);
```

#### Metadata Tables

```sql
CREATE TABLE "TEST_FMC_MTD"."LOAD_CYCLE_INFO" (
    "LOAD_CYCLE_ID" INTEGER,
    "LOAD_DATE" TIMESTAMP
);

CREATE TABLE "TEST_FMC_MTD"."FMC_LOADING_WINDOW_TABLE" (
    "FMC_BEGIN_LW_TIMESTAMP" TIMESTAMP,
    "FMC_END_LW_TIMESTAMP" TIMESTAMP,
    "OBJECT_NAME" VARCHAR
);
```

## Configuration Files

- **Mappings Configuration (`85_mappings_INCR.json`)**  
Defines the dependencies and properties for each mapping task.

- **Load Metadata (`85_FL_mtd_INCR.json`)**  
Specifies the loading dependencies for data vault objects.

## Setup Instructions

1. Create required Snowflake database objects  
2. Configure Airflow connections for Snowflake  
3. Deploy DAG and supporting files  
4. Configure environment variables:  
    - `path_to_metadata`  

## Dependencies

- Apache Airflow  
- Snowflake  
- vs_fmc_plugin (VaultSpeed FMC Plugin)  

## Monitoring

The load status can be monitored via:

- Airflow UI  
- FMC history tables in Snowflake  
- Object loading history in Snowflake  

## Error Handling

- Failed tasks trigger the `fmc_load_fail` procedure  
- Object-specific failures update status via `FMC_UPD_OBJECT_STATUS_DTA_{object}`  
- Load cycle status tracked in FMC history tables  

This README provides a comprehensive overview of the project structure, implementation details, and setup instructions. It includes the key code snippets needed to understand and implement the data vault loading framework.
