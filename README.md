# AWS Spark Event-Driven Data Pipeline

## Overview
This project implements an end-to-end event-driven data pipeline using AWS services and Apache Spark.

## Architecture
- S3 Bronze Layer for raw data
- Lambda for orchestration
- AWS Glue (Spark) for transformations
- S3 Silver Layer in Parquet format
- Future extension to Postgres and Power BI

## Event Flow
1. CSV file uploaded to S3 Bronze bucket
2. S3 triggers Lambda
3. Lambda triggers AWS Glue job
4. Glue reads CSV, applies schema and transformations
5. Output written to S3 Silver as Parquet

## Tech Stack
- AWS S3
- AWS Lambda
- AWS Glue (Apache Spark)
- IAM
- Python
- PySpark

## Key Design Decisions
- Parquet used for efficient analytics
- Glue used instead of Lambda for large-scale transformations
- Event-driven architecture (no cron jobs)

## Real Issues Solved
- Schema inference failure in Spark
- S3 prefix mismatch preventing Lambda trigger
- IAM permission errors for Glue
- Glue write failure to S3 root path

## Author
Eshaan Belani




