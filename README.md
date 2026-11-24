# Azure Payments Data Pipeline

A small end-to-end data engineering project built with **Azure Data Factory (ADF)**, **Azure Data Lake Storage Gen2**, **Azure Databricks**, **PySpark**, and **Delta Lake**.

The pipeline was successfully executed in Azure on **24 November 2026**. It demonstrates file ingestion, orchestration, data quality checks, deterministic deduplication, and Delta Lake upserts.

## Architecture

```text
CSV files (ADLS Gen2 / raw)
            |
            v
ADF: Get Metadata -> Filter (*.csv) -> ForEach / Copy
            |
            v
ADLS Gen2 / bronze / payments_exam
            |
            v
ADF: Databricks Job activity
            |
            v
Databricks notebook (PySpark)
    |                   |
    v                   v
Delta Silver        Delta Rejected
(valid, deduplicated) (invalid rows + reason)
```

## Processing logic

1. **Discover and filter files** — ADF lists files in the input folder and selects CSV files.
2. **Copy to Bronze** — ADF copies each selected file from `raw` to `bronze` in ADLS Gen2.
3. **Validate payments** — PySpark standardizes identifiers and statuses, casts amounts and timestamps, and assigns rejection reasons to invalid records.
4. **Deduplicate** — A window function selects the latest valid record per payment using `updated_at` and `event_id` as ordering keys.
5. **Write Delta tables** — Valid records are upserted into `payments_silver` using Delta `MERGE`; invalid records are written to `payments_rejected` with duplicate protection for repeated processing of the same input.

The Silver upsert is designed to be **idempotent** for repeated processing of the same files. This project processes the files present in the Bronze folder; it does **not** implement a production watermark-based incremental file ingestion mechanism.

## Test result

A controlled end-to-end run completed successfully:

- ADF `Get Metadata`, `Filter`, `ForEach`, and all three `Copy Data` operations: **Succeeded**.
- ADF `Databricks Job` activity: **Succeeded** (approximately **7 min 29 sec**).
- The resulting Delta tables were visible in Databricks Catalog Explorer.

In the earlier standalone notebook test with the sample dataset, **36 input rows** produced **12 rejected rows** and **11 deduplicated Silver payment records**. Those exact row counts were **not independently re-queried after the final Azure run**.

## Repository structure

```text
azure-payments-data-pipeline/
├── adf/
│   ├── ARMTemplateForFactory.json
│   └── ARMTemplateParametersForFactory.json
├── databricks/
│   ├── payments_exam.py
│   └── databricks_job.yml
└── README.md
```

## Deployment notes

The ADF files contain a **minimal, parameterized** ARM template extracted from a larger training factory. They are intended to show the pipeline configuration, not to deploy a fully working environment without further setup.

To run the project in a new Azure environment, you would need to provision ADLS Gen2 and a Databricks workspace, configure identity and storage access, import the notebook, create a Databricks Job, supply the correct workspace and job identifiers, and deploy the ADF resources. The original Databricks workspace was removed after the test to avoid ongoing costs.

## Scope

This is a **training/portfolio project**, not a production-hardened platform. It does not include an automated infrastructure deployment, full CI/CD, or comprehensive observability. The focus is a compact, working pipeline and clearly explainable data-processing decisions.
