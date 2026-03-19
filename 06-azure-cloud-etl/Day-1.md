# Day 1 — Azure Setup, Blob Storage & Azure Data Factory Pipelines

> **Goal: Set up your Azure environment using the CLI, create Blob Storage containers for all five regions, deploy an Azure Data Factory instance, define linked services and datasets, and build a simple ADF pipeline that copies a file from Blob Storage to a staging container. By end of day, uploading a file to Blob Storage and manually triggering ADF moves it to staging.**

---

## What You Are Building Today

1. Set up the Azure environment — subscription, resource group, naming convention
2. Create Azure Blob Storage with one container per region
3. Create an Azure Data Factory instance
4. Define linked services (connections) and datasets (data shapes) in ADF
5. Build the first ADF pipeline activity: Copy Data from raw to staging container
6. Trigger it manually and verify the file appears in staging

---

## Step 1 — Azure Environment Setup

Every Azure resource lives inside a **Resource Group** — a logical container for billing and access control. Create everything for SwiftShip in one resource group so you can delete all resources with one command when done.

```bash
# Login
az login

# Set your subscription (if you have multiple)
az account list --output table
az account set --subscription "your-subscription-name"

# Create resource group
az group create \
  --name swiftship-rg \
  --location eastus

# Verify
az group show --name swiftship-rg --output table
```

**Naming conventions used in this project:**

| Resource | Name | Pattern |
|---|---|---|
| Resource Group | `swiftship-rg` | `{project}-rg` |
| Storage Account | `swiftshipstorage` | `{project}storage` (no hyphens, max 24 chars, lowercase only) |
| Data Factory | `swiftship-adf` | `{project}-adf` |
| Databricks Workspace | `swiftship-dbr` | `{project}-dbr` |
| Synapse Workspace | `swiftship-syn` | `{project}-syn` |

---

## Step 2 — Create Azure Blob Storage

```bash
# Create storage account
az storage account create \
  --name swiftshipstorage \
  --resource-group swiftship-rg \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --access-tier Hot

# Get the connection string (save this — needed for ADF linked service)
az storage account show-connection-string \
  --name swiftshipstorage \
  --resource-group swiftship-rg \
  --query connectionString \
  --output tsv
```

Create containers — one per region plus staging and output:

```bash
# Store connection string in environment variable for convenience
CONN_STR=$(az storage account show-connection-string \
  --name swiftshipstorage --resource-group swiftship-rg \
  --query connectionString --output tsv)

# Raw input containers — one per regional office
for region in north south east west central; do
  az storage container create \
    --name "swiftship-raw-${region}" \
    --connection-string "$CONN_STR"
  echo "Created container: swiftship-raw-${region}"
done

# Staging container — processed, validated files land here
az storage container create --name "swiftship-staging" --connection-string "$CONN_STR"

# Output container — final Parquet files for Synapse to query
az storage container create --name "swiftship-output" --connection-string "$CONN_STR"

# Verify
az storage container list --connection-string "$CONN_STR" --query "[].name" --output table
```

**Why separate containers per region instead of one container with folders?**

Azure Event Grid triggers on blob creation events. If all regions share one container, you get one generic trigger and must parse the file path to determine which region uploaded it. With separate containers, you can set up one trigger per region container, route each trigger independently, and have different SLA or alert settings per region.

---

## Step 3 — Upload Sample Files to Test

Create `scripts/upload_sample_files.py`:

```python
# scripts/upload_sample_files.py

import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient

STORAGE_CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
if not STORAGE_CONN_STR:
    raise EnvironmentError(
        "Set AZURE_STORAGE_CONNECTION_STRING environment variable. "
        "Get it from: az storage account show-connection-string ..."
    )

SAMPLE_FILES = {
    "swiftship-raw-north":   "data/inputs/north_region.csv",
    "swiftship-raw-south":   "data/inputs/south_region.json",
    "swiftship-raw-east":    "data/inputs/east_region.xml",
    "swiftship-raw-west":    "data/inputs/west_region.yaml",
    "swiftship-raw-central": "data/inputs/central_region.txt",
}

client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

for container_name, local_path in SAMPLE_FILES.items():
    local_file = Path(local_path)
    if not local_file.exists():
        print(f"SKIP: {local_path} not found. Copy your Project 01 input files to data/inputs/")
        continue

    blob_client = client.get_blob_client(
        container=container_name,
        blob=local_file.name
    )

    with open(local_file, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    print(f"Uploaded {local_file.name} → {container_name}/{local_file.name}")

print("\nAll files uploaded. Check the Azure Portal → Storage Account → Containers.")
```

Run: `python scripts/upload_sample_files.py`

---

## Step 4 — Create Azure Data Factory

```bash
# Register the ADF resource provider (one-time per subscription)
az provider register --namespace Microsoft.DataFactory

# Create the Data Factory instance
az datafactory create \
  --resource-group swiftship-rg \
  --factory-name swiftship-adf \
  --location eastus

# Verify
az datafactory show \
  --resource-group swiftship-rg \
  --factory-name swiftship-adf \
  --output table
```

Open the ADF Studio UI:

```
Azure Portal → Data Factories → swiftship-adf → Launch Studio
URL: https://adf.azure.com/en/home?factory=/subscriptions/{your-sub}/resourceGroups/swiftship-rg/providers/Microsoft.DataFactory/factories/swiftship-adf
```

---

## Step 5 — Create Linked Services

A **Linked Service** in ADF is a connection definition — the credentials and endpoint for a data store. Think of it as a database connection string but for ADF.

Create the Blob Storage linked service via CLI (or in the ADF Studio UI under Manage → Linked Services):

```bash
# Create linked service definition JSON
cat > /tmp/ls_blob_storage.json << 'EOF'
{
  "name": "ls_AzureBlobStorage",
  "properties": {
    "type": "AzureBlobStorage",
    "typeProperties": {
      "connectionString": {
        "type": "SecureString",
        "value": "__YOUR_CONNECTION_STRING_HERE__"
      }
    }
  }
}
EOF

# In production: use Azure Key Vault reference instead of inline connection string:
# "connectionString": {
#   "type": "AzureKeyVaultSecret",
#   "store": { "referenceName": "AzureKeyVaultLinkedService", "type": "LinkedServiceReference" },
#   "secretName": "swiftship-storage-connstr"
# }

az datafactory linked-service create \
  --resource-group swiftship-rg \
  --factory-name swiftship-adf \
  --linked-service-name ls_AzureBlobStorage \
  --properties @/tmp/ls_blob_storage.json
```

---

## Step 6 — Create Datasets

A **Dataset** in ADF describes the shape of data — which container, which file pattern, what format (CSV, JSON, Parquet).

Create `adf/datasets/ds_blob_raw_csv.json`:

```json
{
  "name": "ds_BlobRawCsv",
  "properties": {
    "type": "DelimitedText",
    "linkedServiceName": {
      "referenceName": "ls_AzureBlobStorage",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "location": {
        "type": "AzureBlobStorageLocation",
        "container": {
          "value": "@dataset().container_name",
          "type": "Expression"
        },
        "fileName": {
          "value": "@dataset().file_name",
          "type": "Expression"
        }
      },
      "columnDelimiter": ",",
      "rowDelimiter": "\n",
      "firstRowAsHeader": true
    },
    "parameters": {
      "container_name": { "type": "string" },
      "file_name":      { "type": "string" }
    },
    "schema": []
  }
}
```

Create `adf/datasets/ds_blob_staging.json` (for the destination):

```json
{
  "name": "ds_BlobStaging",
  "properties": {
    "type": "DelimitedText",
    "linkedServiceName": {
      "referenceName": "ls_AzureBlobStorage",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "location": {
        "type": "AzureBlobStorageLocation",
        "container": "swiftship-staging",
        "fileName": {
          "value": "@dataset().output_file_name",
          "type": "Expression"
        }
      },
      "columnDelimiter": ",",
      "firstRowAsHeader": true
    },
    "parameters": {
      "output_file_name": { "type": "string" }
    }
  }
}
```

---

## Step 7 — Build the First ADF Pipeline

Create `adf/pipelines/swiftship_pipeline.json`. Start with just the Copy activity — Day 2 adds Databricks and Synapse.

```json
{
  "name": "swiftship_daily_pipeline",
  "properties": {
    "description": "SwiftShip daily ETL — ingest from Blob, transform in Databricks, load to Synapse",
    "activities": [
      {
        "name": "CopyNorthRegion",
        "type": "Copy",
        "inputs": [{
          "referenceName": "ds_BlobRawCsv",
          "type": "DatasetReference",
          "parameters": {
            "container_name": "swiftship-raw-north",
            "file_name": "north_region.csv"
          }
        }],
        "outputs": [{
          "referenceName": "ds_BlobStaging",
          "type": "DatasetReference",
          "parameters": {
            "output_file_name": {
              "value": "@concat('north_', formatDateTime(utcnow(), 'yyyyMMdd'), '.csv')",
              "type": "Expression"
            }
          }
        }],
        "typeProperties": {
          "source": { "type": "DelimitedTextSource" },
          "sink":   { "type": "DelimitedTextSink", "storeSettings": { "type": "AzureBlobStorageWriteSettings" } }
        }
      }
    ],
    "parameters": {
      "execution_date": { "type": "string", "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')" }
    }
  }
}
```

Deploy and trigger:

```bash
# Deploy pipeline
az datafactory pipeline create \
  --resource-group swiftship-rg \
  --factory-name swiftship-adf \
  --name swiftship_daily_pipeline \
  --pipeline @adf/pipelines/swiftship_pipeline.json

# Trigger manually
az datafactory pipeline create-run \
  --resource-group swiftship-rg \
  --factory-name swiftship-adf \
  --name swiftship_daily_pipeline

# Monitor in the ADF Studio: Monitor → Pipeline Runs
```

---

## Day 1 Checklist

Before moving to Day 2, confirm:

- [ ] `az group show --name swiftship-rg` returns the resource group
- [ ] All 7 Blob containers created (5 raw + staging + output) — verify in Azure Portal
- [ ] `az datafactory show --factory-name swiftship-adf` returns the ADF instance
- [ ] `ls_AzureBlobStorage` linked service shows as Connected in ADF Studio → Manage
- [ ] Sample files uploaded to their regional containers (verify in Azure Portal → Containers)
- [ ] `swiftship_daily_pipeline` pipeline run completes successfully in ADF Monitor
- [ ] `north_YYYYMMDD.csv` appears in the `swiftship-staging` container after the run
- [ ] No credentials or connection strings appear in any committed file — use environment variables

---

*Day 2 adds the Azure Function event trigger, the Databricks PySpark transformation job, and the Synapse Analytics load step — completing the full cloud pipeline.*
