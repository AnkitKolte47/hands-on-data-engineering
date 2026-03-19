# AzureCloudETL — A 3-Day Data Engineering Project

> **Migrate the SwiftShip ETL pipeline to Microsoft Azure — covering Azure Blob Storage, Azure Data Factory, Azure Functions, Azure Synapse Analytics, and Azure Monitor — every tool in the Azure DE stack used in real enterprise environments.**

---

## Table of Contents

1. [What Is This Project?](#what-is-this-project)
2. [Why These Concepts Matter in Data Engineering](#why-these-concepts-matter-in-data-engineering)
3. [Project Architecture Overview](#project-architecture-overview)
4. [Folder Structure](#folder-structure)
5. [Day 1 — Azure Setup, Blob Storage & Azure Data Factory Pipelines](Day-1.md)
6. [Day 2 — Azure Functions Trigger, Databricks Transform & Synapse Load](Day-2.md)
7. [Day 3 — Monitoring, Alerting, Cost Control & Infrastructure as Code](Day-3.md)
8. [How to Run the Full Project](#how-to-run-the-full-project)
9. [Azure Services Summary](#azure-services-summary)
10. [Interview Cheat Sheet](#interview-cheat-sheet)

---

## What Is This Project?

SwiftShip has grown from a 5-person startup to a 500-person company. The on-premise server running the Python ETL pipeline cannot keep up. The CTO makes a decision: **move everything to Azure.**

Your job as the data engineer: re-architect the pipeline on Azure cloud without losing any functionality from Projects 01–03.

New requirements driven by scale:

- **Regional offices now upload files to the cloud** — not a shared file system. Files go into Azure Blob Storage containers, one per region.
- **Load is triggered by file arrival**, not a fixed schedule. When north_region.csv lands in Blob Storage, the pipeline starts immediately — not at 6 AM.
- **Transformation runs in parallel on a cluster**, not a single Python process. Azure Databricks processes all five regions simultaneously.
- **Output lands in a proper cloud data warehouse** — Azure Synapse Analytics (formerly SQL Data Warehouse), not SQLite.
- **All failures send alerts** via Azure Monitor and email/Teams notification.
- **The entire infrastructure is code** — no clicking in the Azure Portal to set up resources. Everything is in Terraform so any environment can be reproduced in 15 minutes.

You will use:

| Azure Service | Role in Pipeline |
|---|---|
| **Azure Blob Storage** | Landing zone for regional source files |
| **Azure Event Grid** | Detects blob upload events and triggers the pipeline |
| **Azure Data Factory** | Orchestrates the end-to-end pipeline (Azure's managed Airflow) |
| **Azure Functions** | Lightweight serverless trigger and pre-processing |
| **Azure Databricks** | Distributed transformation engine (PySpark) |
| **Azure Synapse Analytics** | Cloud data warehouse — target for clean data |
| **Azure Monitor + Alerts** | Pipeline observability and failure notifications |
| **Terraform** | Infrastructure as Code — all resources defined as code |

---

## Why These Concepts Matter in Data Engineering

| Concept | Real Problem It Solves | What Happens Without It |
|---|---|---|
| **Blob Storage** | Scalable, durable object store for files of any size and format | On-premise file servers hit disk limits, have no geo-redundancy, and cost more at scale |
| **Event-driven triggers** | Pipeline starts immediately when data arrives, not at a fixed schedule | Files that arrive early sit idle until the next scheduled run; files that arrive late cause a failed run |
| **ADF Pipelines** | Managed orchestration with built-in retry, monitoring, and 90+ connectors | You build and maintain your own scheduling infrastructure — a full-time job on its own |
| **Databricks / PySpark** | Distributed processing — split 100GB files across a cluster of 10 machines | A single Python process takes 8 hours for what a 10-node cluster does in 45 minutes |
| **Synapse Analytics** | MPP (Massively Parallel Processing) SQL engine for petabyte-scale analytics | Standard SQL Server struggles with billion-row queries; analysts wait minutes for results |
| **Azure Monitor** | Centralised observability — metrics, logs, and alerts in one place | Failures are discovered by stakeholders, not engineers; no performance baseline exists |
| **Terraform** | All cloud resources defined as code — reproducible, reviewable, version-controlled | Click-ops configurations drift between environments; reproducing prod takes days of tribal knowledge |

---

## Project Architecture Overview

```
Regional Offices (5 offices upload files)
        │
        ▼  HTTP PUT to Blob Storage
┌─────────────────────────────────────────┐
│  AZURE BLOB STORAGE  (Day 1)            │
│  Container: swiftship-raw               │
│  Folders: north/ south/ east/ west/     │
│           central/                      │
└──────────────┬──────────────────────────┘
               │  Blob Created Event
               ▼
┌─────────────────────────────────────────┐
│  AZURE EVENT GRID  (Day 2)              │
│  Listens for BlobCreated events         │
│  Triggers Azure Function                │
└──────────────┬──────────────────────────┘
               │  HTTP trigger
               ▼
┌─────────────────────────────────────────┐
│  AZURE FUNCTION  (Day 2)                │
│  Validates blob name / size             │
│  Triggers ADF pipeline via REST API     │
└──────────────┬──────────────────────────┘
               │  ADF pipeline run
               ▼
┌─────────────────────────────────────────┐
│  AZURE DATA FACTORY  (Day 1 + 2)        │
│  Activity 1: Copy raw blob to staging   │
│  Activity 2: Trigger Databricks job     │
│  Activity 3: Load to Synapse            │
│  Activity 4: Send Teams notification    │
└──────────────┬──────────────────────────┘
               │ clean data
               ▼
┌─────────────────────────────────────────┐
│  AZURE DATABRICKS  (Day 2)              │
│  PySpark job: validate + transform      │
│  Writes Parquet to staging container    │
└──────────────┬──────────────────────────┘
               │ Parquet files
               ▼
┌─────────────────────────────────────────┐
│  AZURE SYNAPSE ANALYTICS  (Day 2)       │
│  COPY INTO shipments from Parquet       │
│  Upsert logic via MERGE statement       │
│  Analysts query here                    │
└──────────────┬──────────────────────────┘
               │ metrics
               ▼
┌─────────────────────────────────────────┐
│  AZURE MONITOR + ALERTS  (Day 3)        │
│  ADF pipeline failure → email/Teams     │
│  Synapse query performance metrics      │
│  Cost budget alerts                     │
└─────────────────────────────────────────┘
```

---

## Folder Structure

```
azure_cloud_etl/
│
├── terraform/
│   ├── main.tf                     ← Day 3: all Azure resources
│   ├── variables.tf                ← Day 3: configurable inputs
│   ├── outputs.tf                  ← Day 3: resource IDs and URLs
│   └── modules/
│       ├── storage/                ← Day 1: Blob Storage module
│       ├── adf/                    ← Day 1: Data Factory module
│       ├── databricks/             ← Day 2: Databricks module
│       ├── synapse/                ← Day 2: Synapse module
│       └── monitoring/             ← Day 3: Monitor + Alerts module
│
├── adf/
│   ├── pipelines/
│   │   └── swiftship_pipeline.json ← Day 1: ADF pipeline definition (ARM)
│   ├── datasets/
│   │   ├── ds_blob_source.json     ← Day 1
│   │   └── ds_synapse_sink.json    ← Day 2
│   └── linked_services/
│       ├── ls_blob_storage.json    ← Day 1
│       └── ls_synapse.json         ← Day 2
│
├── functions/
│   └── blob_trigger/
│       ├── __init__.py             ← Day 2: Azure Function code
│       ├── function.json           ← Day 2: trigger binding config
│       └── requirements.txt
│
├── databricks/
│   └── transform_job.py            ← Day 2: PySpark transformation
│
├── synapse/
│   ├── create_tables.sql           ← Day 2: Synapse DDL
│   └── upsert_shipments.sql        ← Day 2: MERGE statement
│
├── monitoring/
│   └── alert_rules.json            ← Day 3: Azure Monitor alert definitions
│
├── scripts/
│   ├── upload_sample_files.py      ← Day 1: upload test files to Blob
│   └── test_end_to_end.py          ← Day 3: end-to-end smoke test
│
└── README.md
```

---

## How to Run the Full Project

### Prerequisites

```bash
# Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az login

# Terraform
brew install terraform   # Mac
# or: https://developer.hashicorp.com/terraform/downloads

# Azure Functions Core Tools
npm install -g azure-functions-core-tools@4

# Python packages
pip install azure-storage-blob azure-mgmt-datafactory azure-identity pyspark
```

### Deploy infrastructure

```bash
cd terraform
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

### Upload sample files and trigger pipeline

```bash
cd scripts
python upload_sample_files.py   # uploads test files to Blob Storage
# Event Grid detects the upload → Azure Function fires → ADF pipeline starts
# Monitor at: Azure Portal → Data Factory → Monitor → Pipeline Runs
```

### Run Databricks job manually

```bash
# Via Azure CLI (after Databricks cluster is running)
az databricks job run-now --resource-group swiftship-rg --workspace-name swiftship-dbr --job-id <job-id>
```

---

## Azure Services Summary

| Service | Tier for This Project | Monthly Cost Estimate |
|---|---|---|
| Azure Blob Storage | LRS, Hot tier | ~$2–5/month (< 100GB) |
| Azure Event Grid | Per-event pricing | ~$0.01/month (< 100K events) |
| Azure Data Factory | Pay-per-use | ~$1–10/month (< 100 runs) |
| Azure Functions | Consumption plan | Free tier sufficient |
| Azure Databricks | Standard tier, autoscale | ~$10–50/month (dev cluster) |
| Azure Synapse Analytics | Serverless SQL | ~$5/TB scanned |
| Azure Monitor | Basic | Free tier sufficient for alerts |

**Total estimated cost for development:** $20–70/month. Delete resources when not using to minimise cost.

---

## Interview Cheat Sheet

**"Why Azure Blob Storage instead of a relational database as the landing zone?"**
> Blob Storage is designed for object storage — arbitrary files of any format, any size, at extremely low cost. Regional offices send CSV, JSON, and XML files — not rows that fit neatly into a table schema. Blob Storage accepts any format, scales to petabytes, and is designed for ingestion-first patterns. The relational database is the destination after transformation, not the landing zone.

**"What is the difference between Azure Data Factory and Azure Databricks?"**
> ADF is the orchestrator — it defines the workflow: copy this file, then run that job, then load this table, then send a notification. ADF does not transform data itself (it can do simple mappings, but not complex logic). Databricks is the compute engine — it runs the actual PySpark transformation code on a cluster. In the pipeline, ADF calls Databricks as one step in a larger workflow, then continues to the next step once the Databricks job completes.

**"What is Event Grid and why use it instead of a schedule trigger in ADF?"**
> Azure Event Grid is a fully managed event routing service. When a blob is uploaded to Storage, Event Grid publishes a `BlobCreated` event. My Azure Function subscribes to that event and triggers the ADF pipeline immediately. This is event-driven architecture — the pipeline reacts to data arriving rather than polling on a fixed schedule. A schedule trigger would waste compute checking for files that may not have arrived yet, and would delay processing for files that arrive outside the schedule window.

**"What is PolyBase and how does COPY INTO work in Synapse?"**
> PolyBase and COPY INTO are Synapse's mechanisms for loading external data at scale. Instead of row-by-row inserts, COPY INTO reads Parquet or CSV files directly from Blob Storage using the Synapse MPP engine — all distribution nodes read their partition of the files in parallel. Loading 100GB of Parquet files with COPY INTO takes minutes; loading the same data with INSERT statements would take hours. It is the equivalent of PostgreSQL's COPY command but distributed across potentially hundreds of nodes.

**"Why Terraform instead of clicking in the Azure Portal?"**
> Three reasons. First, reproducibility — any team member can run `terraform apply` and get an identical environment in 15 minutes. Second, auditability — every infrastructure change is a git commit with a diff, author, and message. Third, disaster recovery — if the Azure subscription is compromised or a resource group is accidentally deleted, running `terraform apply` recreates everything from code. Click-ops configurations exist only in Azure's metadata — no code, no audit trail, no reproducibility.

**"What is the difference between Synapse Dedicated SQL Pool and Serverless SQL Pool?"**
> Dedicated SQL Pool provisions a fixed amount of compute and storage — you pay for it 24/7 whether you use it or not. It is appropriate for predictable, high-volume workloads where query performance must be consistent. Serverless SQL Pool bills per TB of data scanned — there is no provisioning, you pay only for what you query. For development and irregular query patterns, Serverless is dramatically cheaper. For this project, Serverless is used to keep costs near zero.

---

*Cloud DE skills are required for almost every senior data engineering role. Azure is the dominant cloud at enterprise companies. Knowing ADF, Databricks, and Synapse — and how they work together — is a direct path to senior and staff roles.*
