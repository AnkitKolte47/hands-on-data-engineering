# Day 2 — Azure Functions Trigger, Databricks Transform & Synapse Load

> **Goal: Replace the manual ADF trigger with an event-driven Azure Function that fires when a file is uploaded to Blob Storage, add a Databricks PySpark transformation job as an ADF activity, and load the clean Parquet output into Azure Synapse Analytics using COPY INTO. By end of day, uploading a file to Blob Storage automatically triggers the complete pipeline end-to-end.**

---

## What You Are Building Today

1. Create an Azure Function triggered by Blob Storage upload events via Event Grid
2. The function validates the uploaded file and triggers the ADF pipeline via REST API
3. Add a Databricks notebook as an ADF pipeline activity for distributed transformation
4. Write the Synapse table DDL and COPY INTO load logic
5. Connect ADF to Synapse as the final load step
6. Test the full event-driven pipeline end-to-end

---

## Step 1 — Create the Azure Function App

Azure Functions is serverless compute — code that runs in response to events. The function runs only when triggered, scales automatically, and costs nothing when idle.

```bash
# Create a storage account for the Function App (Functions need their own storage)
az storage account create \
  --name swiftshipfuncstorage \
  --resource-group swiftship-rg \
  --location eastus \
  --sku Standard_LRS

# Create the Function App (Python runtime)
az functionapp create \
  --name swiftship-blob-trigger \
  --resource-group swiftship-rg \
  --storage-account swiftshipfuncstorage \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --os-type linux

# Set app settings (environment variables for the function)
az functionapp config appsettings set \
  --name swiftship-blob-trigger \
  --resource-group swiftship-rg \
  --settings \
    "SWIFTSHIP_STORAGE_CONN_STR=__your_storage_conn_str__" \
    "ADF_SUBSCRIPTION_ID=__your_subscription_id__" \
    "ADF_RESOURCE_GROUP=swiftship-rg" \
    "ADF_FACTORY_NAME=swiftship-adf" \
    "ADF_PIPELINE_NAME=swiftship_daily_pipeline"
```

---

## Step 2 — Write the Azure Function

Create `functions/blob_trigger/function.json`:

```json
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "type": "httpTrigger",
      "direction": "in",
      "name": "req",
      "methods": ["post"],
      "authLevel": "function"
    },
    {
      "type": "http",
      "direction": "out",
      "name": "$return"
    }
  ]
}
```

Create `functions/blob_trigger/__init__.py`:

```python
# functions/blob_trigger/__init__.py

import json
import logging
import os
import azure.functions as func
from azure.identity import ManagedIdentityCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import CreateRunResponse


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function triggered by Event Grid when a blob is created.

    Event Grid sends a POST request to this function with a JSON body
    describing the blob creation event. The function:
    1. Validates the event is a blob creation (not a test event)
    2. Extracts the container and blob name from the event
    3. Validates the file meets minimum requirements (non-empty, correct extension)
    4. Triggers the ADF pipeline with the blob metadata as parameters

    Event Grid validation: Event Grid sends a validation event first to verify
    the endpoint is live. This function handles that handshake.
    """
    logging.info("SwiftShip blob trigger function received a request.")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    # Handle Event Grid subscription validation handshake
    # This is sent once when you first configure the Event Grid subscription
    for event in body if isinstance(body, list) else [body]:
        if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
            validation_code = event["data"]["validationCode"]
            logging.info(f"Event Grid validation handshake. Code: {validation_code}")
            return func.HttpResponse(
                json.dumps({"validationResponse": validation_code}),
                mimetype="application/json"
            )

    # Process actual blob created events
    results = []
    events = body if isinstance(body, list) else [body]

    for event in events:
        if event.get("eventType") != "Microsoft.Storage.BlobCreated":
            logging.info(f"Ignoring event type: {event.get('eventType')}")
            continue

        blob_url = event["data"]["url"]
        content_length = event["data"].get("contentLength", 0)
        blob_name = blob_url.split("/")[-1]
        container_name = blob_url.split("/")[-2]

        logging.info(f"Blob created: {container_name}/{blob_name} ({content_length} bytes)")

        # Validation 1: minimum file size (avoid triggering on empty files)
        if content_length < 50:
            logging.warning(f"File too small ({content_length} bytes). Skipping.")
            results.append({"blob": blob_name, "status": "skipped_too_small"})
            continue

        # Validation 2: only process known containers
        known_containers = [
            "swiftship-raw-north", "swiftship-raw-south",
            "swiftship-raw-east", "swiftship-raw-west", "swiftship-raw-central"
        ]
        if container_name not in known_containers:
            logging.info(f"Unknown container: {container_name}. Skipping.")
            results.append({"blob": blob_name, "status": "skipped_unknown_container"})
            continue

        # Trigger ADF pipeline
        run_id = _trigger_adf_pipeline(container_name, blob_name)
        results.append({
            "blob": blob_name,
            "container": container_name,
            "adf_run_id": run_id,
            "status": "triggered"
        })

    return func.HttpResponse(
        json.dumps({"processed": len(results), "results": results}),
        mimetype="application/json",
        status_code=200
    )


def _trigger_adf_pipeline(container_name: str, blob_name: str) -> str:
    """
    Trigger the ADF pipeline via the Azure Management API.

    Uses Managed Identity — the Function App's system-assigned identity
    is granted the 'Data Factory Contributor' role on the ADF instance.
    No credentials are stored in code or environment variables.
    """
    subscription_id = os.environ["ADF_SUBSCRIPTION_ID"]
    resource_group  = os.environ["ADF_RESOURCE_GROUP"]
    factory_name    = os.environ["ADF_FACTORY_NAME"]
    pipeline_name   = os.environ["ADF_PIPELINE_NAME"]

    # Managed Identity: no credentials needed — Azure handles authentication
    credential = ManagedIdentityCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    run_response: CreateRunResponse = adf_client.pipelines.create_run(
        resource_group_name=resource_group,
        factory_name=factory_name,
        pipeline_name=pipeline_name,
        parameters={
            "source_container": container_name,
            "source_blob":      blob_name,
            "execution_date":   container_name.split("-")[-1]  # e.g., "north"
        }
    )

    logging.info(f"ADF pipeline triggered. Run ID: {run_response.run_id}")
    return run_response.run_id
```

Create `functions/blob_trigger/requirements.txt`:

```
azure-functions
azure-identity
azure-mgmt-datafactory
```

Deploy the function:

```bash
cd functions
func azure functionapp publish swiftship-blob-trigger
```

---

## Step 3 — Wire Event Grid to the Function

```bash
# Get the function URL (with auth key)
FUNC_URL=$(az functionapp function show \
  --name swiftship-blob-trigger \
  --resource-group swiftship-rg \
  --function-name blob_trigger \
  --query invokeUrlTemplate \
  --output tsv)

# Create Event Grid subscription for north region container
az eventgrid event-subscription create \
  --name swiftship-north-trigger \
  --source-resource-id $(az storage account show \
    --name swiftshipstorage --resource-group swiftship-rg \
    --query id --output tsv) \
  --endpoint "$FUNC_URL" \
  --endpoint-type webhook \
  --included-event-types Microsoft.Storage.BlobCreated \
  --subject-begins-with "/blobServices/default/containers/swiftship-raw-north/"

# Repeat for other regions or use a single subscription covering all containers
# by removing --subject-begins-with to match all blobs in the storage account
```

---

## Step 4 — Write the Databricks PySpark Job

Create `databricks/transform_job.py`. This runs on a Databricks cluster — handles all 5 source formats and produces clean Parquet.

```python
# databricks/transform_job.py
# Run as a Databricks notebook or as a Databricks Jobs API job.

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Widget parameters — passed from ADF pipeline as parameters
SOURCE_CONTAINER = dbutils.widgets.get("source_container")  # e.g., "swiftship-raw-north"
SOURCE_BLOB      = dbutils.widgets.get("source_blob")        # e.g., "north_region.csv"
EXECUTION_DATE   = dbutils.widgets.get("execution_date")     # e.g., "north"
STORAGE_ACCOUNT  = dbutils.widgets.get("storage_account")   # e.g., "swiftshipstorage"

spark = SparkSession.builder.appName("SwiftShipTransform").getOrCreate()

# Azure Storage access — configured in Databricks cluster via service principal
spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
# Service principal credentials injected via Databricks secrets (not hardcoded)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    dbutils.secrets.get(scope="swiftship", key="sp-client-id")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    dbutils.secrets.get(scope="swiftship", key="sp-client-secret")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='swiftship', key='tenant-id')}/oauth2/token"
)

# ── Build paths ───────────────────────────────────────────────────────────────
source_path = f"abfss://{SOURCE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SOURCE_BLOB}"
output_path = f"abfss://swiftship-output@{STORAGE_ACCOUNT}.dfs.core.windows.net/shipments/date={EXECUTION_DATE}/"

print(f"Reading from: {source_path}")
print(f"Writing to:   {output_path}")

# ── Read source (auto-detect format from extension) ───────────────────────────
file_extension = SOURCE_BLOB.rsplit(".", 1)[-1].lower()

if file_extension == "csv":
    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(source_path)
elif file_extension == "json":
    raw_df = spark.read.option("multiLine", "true").json(source_path)
elif file_extension in ("yaml", "yml", "txt", "xml"):
    # For non-native formats: read as text and parse manually
    # In a real project, use a custom UDF or pre-process with Azure Function
    raise ValueError(f"Format {file_extension} requires pre-processing. "
                     f"Add a pre-processing step in the Azure Function.")
else:
    raise ValueError(f"Unsupported file extension: {file_extension}")

# ── Define expected schema ────────────────────────────────────────────────────
EXPECTED_COLUMNS = {
    "shipment_id", "origin_city", "destination_city",
    "dispatch_date", "delivery_date", "status", "weight_kg"
}

# Add missing columns as null (schema evolution — not all sources have all columns)
for col in EXPECTED_COLUMNS:
    if col not in raw_df.columns:
        raw_df = raw_df.withColumn(col, F.lit(None).cast(StringType()))

# ── Transform ─────────────────────────────────────────────────────────────────
VALID_STATUSES = ["DELIVERED", "IN_TRANSIT", "PENDING", "RETURNED", "FAILED"]

transformed_df = (
    raw_df
    # Standardise
    .withColumn("origin_city",      F.lower(F.trim(F.col("origin_city"))))
    .withColumn("destination_city", F.lower(F.trim(F.col("destination_city"))))
    .withColumn("status",           F.upper(F.trim(F.col("status"))))

    # Cast weight — strip "kg" suffix, coerce to double
    .withColumn("weight_kg",
        F.when(
            F.col("weight_kg").isNotNull(),
            F.regexp_replace(F.col("weight_kg"), "[^0-9.]", "").cast(DoubleType())
        ).otherwise(F.lit(None))
    )

    # Cast dates
    .withColumn("dispatch_date", F.to_date(F.col("dispatch_date")))
    .withColumn("delivery_date", F.to_date(F.col("delivery_date")))

    # Add metadata
    .withColumn("source_container", F.lit(SOURCE_CONTAINER))
    .withColumn("source_file",      F.lit(SOURCE_BLOB))
    .withColumn("loaded_at",        F.current_timestamp())
    .withColumn("pipeline_date",    F.lit(EXECUTION_DATE))

    # Filter invalid records
    .filter(F.col("shipment_id").isNotNull() & (F.col("shipment_id") != ""))
    .filter(F.col("status").isin(VALID_STATUSES))
    .filter(
        F.col("weight_kg").isNull() |
        ((F.col("weight_kg") > 0) & (F.col("weight_kg") <= 1000))
    )
)

# ── Write Parquet ─────────────────────────────────────────────────────────────
# Parquet is columnar — Synapse reads only the columns it needs, not the whole file
transformed_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .parquet(output_path)

record_count = transformed_df.count()
print(f"Written {record_count:,} records to {output_path}")

# Return the count to ADF via a notebook exit value (available as ADF activity output)
dbutils.notebook.exit(str(record_count))
```

---

## Step 5 — Create Synapse Tables and Load Logic

```sql
-- synapse/create_tables.sql
-- Run once in Synapse Serverless SQL or Dedicated Pool

-- External data source pointing to Blob Storage
CREATE EXTERNAL DATA SOURCE swiftship_output
WITH (
    LOCATION = 'https://swiftshipstorage.blob.core.windows.net/swiftship-output'
);

-- External file format for Parquet
CREATE EXTERNAL FILE FORMAT swiftship_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- External table — Synapse reads directly from Blob Storage Parquet files
-- No data is stored in Synapse — the data stays in Blob Storage
CREATE EXTERNAL TABLE shipments (
    shipment_id       NVARCHAR(50)    NOT NULL,
    origin_city       NVARCHAR(100),
    destination_city  NVARCHAR(100),
    dispatch_date     DATE,
    delivery_date     DATE,
    status            NVARCHAR(30),
    weight_kg         FLOAT,
    source_container  NVARCHAR(100),
    source_file       NVARCHAR(200),
    loaded_at         DATETIME2,
    pipeline_date     NVARCHAR(20)
)
WITH (
    LOCATION = 'shipments/**',   -- reads all Parquet files recursively
    DATA_SOURCE = swiftship_output,
    FILE_FORMAT = swiftship_parquet
);
```

```sql
-- synapse/upsert_shipments.sql
-- For Dedicated Pool: uses COPY INTO + MERGE for upserts
-- COPY INTO is Synapse's bulk load command — much faster than INSERT

-- Step 1: Load to staging table
CREATE TABLE IF NOT EXISTS shipments_staging (LIKE shipments INCLUDING ALL);

COPY INTO shipments_staging
FROM 'https://swiftshipstorage.blob.core.windows.net/swiftship-output/shipments/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    AUTO_CREATE_TABLE = 'ON'
);

-- Step 2: Merge staging into main table (upsert)
MERGE shipments AS target
USING shipments_staging AS source
    ON target.shipment_id = source.shipment_id
WHEN MATCHED THEN
    UPDATE SET
        status           = source.status,
        delivery_date    = source.delivery_date,
        weight_kg        = source.weight_kg,
        loaded_at        = source.loaded_at
WHEN NOT MATCHED THEN
    INSERT (shipment_id, origin_city, destination_city, dispatch_date,
            delivery_date, status, weight_kg, source_container, source_file, loaded_at)
    VALUES (source.shipment_id, source.origin_city, source.destination_city,
            source.dispatch_date, source.delivery_date, source.status,
            source.weight_kg, source.source_container, source.source_file, source.loaded_at);

-- Step 3: Clean up staging
TRUNCATE TABLE shipments_staging;
```

---

## Step 6 — Add Databricks and Synapse Activities to ADF Pipeline

Update `adf/pipelines/swiftship_pipeline.json` to include all three activities:

```json
{
  "name": "swiftship_daily_pipeline",
  "properties": {
    "activities": [
      {
        "name": "CopyRawToStaging",
        "type": "Copy",
        "inputs":  [{ "referenceName": "ds_BlobRawCsv", "type": "DatasetReference",
                      "parameters": { "container_name": "@pipeline().parameters.source_container",
                                      "file_name": "@pipeline().parameters.source_blob" }}],
        "outputs": [{ "referenceName": "ds_BlobStaging", "type": "DatasetReference",
                      "parameters": { "output_file_name": "@pipeline().parameters.source_blob" }}],
        "typeProperties": { "source": { "type": "DelimitedTextSource" },
                            "sink": { "type": "DelimitedTextSink" }}
      },
      {
        "name": "TransformWithDatabricks",
        "type": "DatabricksNotebook",
        "dependsOn": [{ "activity": "CopyRawToStaging", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": {
          "notebookPath": "/swiftship/transform_job",
          "baseParameters": {
            "source_container": { "value": "@pipeline().parameters.source_container", "type": "Expression" },
            "source_blob":      { "value": "@pipeline().parameters.source_blob", "type": "Expression" },
            "execution_date":   { "value": "@pipeline().parameters.execution_date", "type": "Expression" },
            "storage_account":  { "value": "swiftshipstorage", "type": "Expression" }
          }
        },
        "linkedServiceName": { "referenceName": "ls_AzureDatabricks", "type": "LinkedServiceReference" }
      },
      {
        "name": "LoadToSynapse",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [{ "activity": "TransformWithDatabricks", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": {
          "storedProcedureName": "[dbo].[usp_upsert_shipments]"
        },
        "linkedServiceName": { "referenceName": "ls_AzureSynapse", "type": "LinkedServiceReference" }
      }
    ],
    "parameters": {
      "source_container": { "type": "string" },
      "source_blob":      { "type": "string" },
      "execution_date":   { "type": "string", "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')" }
    }
  }
}
```

---

## Day 2 Checklist

Before moving to Day 3, confirm:

- [ ] Event Grid subscription created for the north region container
- [ ] Uploading a file to `swiftship-raw-north` triggers the Azure Function (verify in Function Monitor)
- [ ] Function logs show the ADF run ID — pipeline was triggered
- [ ] ADF pipeline run shows all three activities: Copy → Databricks → Synapse
- [ ] Parquet files appear in `swiftship-output/shipments/date={date}/` after Databricks runs
- [ ] Querying `SELECT COUNT(*) FROM shipments` in Synapse returns the expected row count
- [ ] No credentials appear anywhere in committed files — all secrets in Databricks Secrets or Azure Key Vault
- [ ] Running the pipeline twice does not create duplicate rows (MERGE ensures upsert behaviour)

---

*Day 3 adds Azure Monitor alerts for pipeline failures, cost budget alerts, and converts all infrastructure to Terraform for reproducible deployments.*
