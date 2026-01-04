# Data Platform – EMR Spark Pipelines

This repository demonstrates a multi-pipeline data platform
using Apache Spark on AWS EMR.

## Architecture
S3 (raw) → EMR Spark → S3 (Parquet) → Glue → Athena

## Pipelines
- Sales ETL
- User Activity ETL

## How pipelines are run
Pipelines are executed via spark-submit by specifying
the pipeline name.

Example:
spark-submit main.py sales

## Key Concepts
- Shared Spark utilities
- Partitioned Parquet output
- Idempotent writes
- Multi-pipeline architecture
