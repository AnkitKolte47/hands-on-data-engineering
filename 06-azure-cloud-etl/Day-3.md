# Day 3 — Monitoring, Alerting, Cost Control & Infrastructure as Code

> **Goal: Configure Azure Monitor alerts for ADF pipeline failures and Synapse query performance, set a cost budget alert to prevent runaway cloud spend, write Terraform code for all Azure resources, and run a full end-to-end smoke test. By end of day, all infrastructure is defined as code and the pipeline is fully observable.**

---

## What You Are Building Today

1. Configure Azure Monitor diagnostic settings to capture ADF pipeline metrics and logs
2. Create alert rules — ADF pipeline failure → email/Teams notification
3. Set a cost budget with alerts (critical for cloud development — one misconfiguration can cost hundreds)
4. Write Terraform code for all six Azure services created in Days 1 and 2
5. Run `terraform apply` to verify the infrastructure can be reproduced from scratch
6. Run the full end-to-end smoke test

---

## Step 1 — Set Up Azure Monitor

Azure Monitor is Azure's unified observability platform. All Azure services emit metrics and logs to Monitor automatically — you just need to route them.

```bash
# Create a Log Analytics Workspace — where all logs are stored and queried
az monitor log-analytics workspace create \
  --resource-group swiftship-rg \
  --workspace-name swiftship-logs \
  --location eastus \
  --sku PerGB2018   # pay-per-GB pricing

# Get the workspace ID
WORKSPACE_ID=$(az monitor log-analytics workspace show \
  --resource-group swiftship-rg \
  --workspace-name swiftship-logs \
  --query id --output tsv)

echo "Log Analytics Workspace ID: $WORKSPACE_ID"

# Enable diagnostic settings for ADF — send all pipeline logs to Log Analytics
ADF_RESOURCE_ID=$(az datafactory show \
  --resource-group swiftship-rg \
  --factory-name swiftship-adf \
  --query id --output tsv)

az monitor diagnostic-settings create \
  --name "swiftship-adf-diagnostics" \
  --resource "$ADF_RESOURCE_ID" \
  --workspace "$WORKSPACE_ID" \
  --logs '[
    {"category": "PipelineRuns",  "enabled": true, "retentionPolicy": {"days": 30, "enabled": true}},
    {"category": "ActivityRuns",  "enabled": true, "retentionPolicy": {"days": 30, "enabled": true}},
    {"category": "TriggerRuns",   "enabled": true, "retentionPolicy": {"days": 30, "enabled": true}}
  ]'
```

Query ADF logs in Log Analytics:

```kusto
// Kusto Query Language (KQL) — used in Azure Monitor, Log Analytics, Sentinel
// Find all failed pipeline runs in the last 24 hours:
ADFPipelineRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project TimeGenerated, PipelineName, RunId, FailureType, ErrorMessage
| order by TimeGenerated desc

// Average pipeline duration by day:
ADFPipelineRun
| where Status == "Succeeded"
| extend DurationMinutes = DurationInMs / 60000
| summarize avg(DurationMinutes) by bin(TimeGenerated, 1d), PipelineName
| render timechart
```

---

## Step 2 — Create Alert Rules

```bash
# Create an Action Group — who gets notified and how
az monitor action-group create \
  --resource-group swiftship-rg \
  --name swiftship-oncall \
  --short-name oncall \
  --action email \
    oncall_email \
    "oncall@swiftship.com"

# Get the Action Group ID
AG_ID=$(az monitor action-group show \
  --resource-group swiftship-rg \
  --name swiftship-oncall \
  --query id --output tsv)

# Alert Rule 1: ADF pipeline failure
# Fires when any ADF pipeline fails — triggers within 5 minutes
az monitor scheduled-query create \
  --resource-group swiftship-rg \
  --name "alert-adf-pipeline-failure" \
  --scopes "$ADF_RESOURCE_ID" \
  --condition-query "
    ADFPipelineRun
    | where Status == 'Failed'
    | where PipelineName == 'swiftship_daily_pipeline'
    | count
  " \
  --condition-operator GreaterThan \
  --condition-threshold 0 \
  --evaluation-frequency 5m \
  --window-size 5m \
  --severity 2 \
  --description "SwiftShip ADF pipeline failure detected" \
  --action-groups "$AG_ID" \
  --auto-mitigate true

# Alert Rule 2: Long-running pipeline (SLA alert)
# Fires when a pipeline run takes longer than 90 minutes
az monitor scheduled-query create \
  --resource-group swiftship-rg \
  --name "alert-adf-pipeline-slow" \
  --scopes "$ADF_RESOURCE_ID" \
  --condition-query "
    ADFPipelineRun
    | where Status == 'InProgress'
    | where DurationInMs > 5400000
    | count
  " \
  --condition-operator GreaterThan \
  --condition-threshold 0 \
  --evaluation-frequency 15m \
  --window-size 15m \
  --severity 3 \
  --description "SwiftShip pipeline running for more than 90 minutes"
```

---

## Step 3 — Set Cost Budget Alerts

Cloud costs can spiral quickly, especially if a Databricks cluster is accidentally left running or a misconfigured loop generates millions of Azure Function invocations.

```bash
# Create a monthly budget of $50 for this resource group
az consumption budget create \
  --budget-name "swiftship-monthly-budget" \
  --amount 50 \
  --time-grain Monthly \
  --start-date "$(date -d 'first day of this month' +%Y-%m-01)" \
  --end-date "2026-12-31" \
  --resource-group swiftship-rg \
  --notifications '{
    "actual_80_percent": {
      "enabled": true,
      "operator": "GreaterThan",
      "threshold": 80,
      "contactEmails": ["oncall@swiftship.com"],
      "contactRoles": ["Owner"]
    },
    "actual_100_percent": {
      "enabled": true,
      "operator": "GreaterThan",
      "threshold": 100,
      "contactEmails": ["oncall@swiftship.com"],
      "contactRoles": ["Owner"]
    },
    "forecast_110_percent": {
      "enabled": true,
      "operator": "GreaterThan",
      "threshold": 110,
      "contactEmails": ["oncall@swiftship.com"],
      "contactRoles": ["Owner"],
      "thresholdType": "Forecasted"
    }
  }'
```

**Cost optimisation tips for this project:**
- Stop the Databricks cluster when not running jobs (`terminate_after_minutes=30` in cluster config)
- Use Synapse Serverless SQL Pool instead of Dedicated Pool (pay per query, not per hour)
- Set Blob Storage lifecycle policies to move old data to Cool tier after 30 days
- Delete the resource group when the project is complete: `az group delete --name swiftship-rg`

---

## Step 4 — Write Terraform Infrastructure as Code

Create all Terraform files. Every resource created via CLI in Days 1–2 is now defined as code.

**`terraform/variables.tf`**

```hcl
# terraform/variables.tf

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "swiftship-rg"
}

variable "project_name" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "swiftship"
}

variable "alert_email" {
  description = "Email address for monitoring alerts"
  type        = string
  # No default — must be provided at apply time for security
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget in USD"
  type        = number
  default     = 50
}
```

**`terraform/main.tf`**

```hcl
# terraform/main.tf

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
  }
  required_version = ">= 1.5.0"

  # Store Terraform state in Azure Blob Storage (not local .tfstate file)
  # Uncomment after creating the backend storage account manually:
  # backend "azurerm" {
  #   resource_group_name  = "swiftship-tf-rg"
  #   storage_account_name = "swiftshiptfstate"
  #   container_name       = "tfstate"
  #   key                  = "swiftship.tfstate"
  # }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# ── Resource Group ────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    project     = var.project_name
    managed_by  = "terraform"
    environment = "development"
  }
}

# ── Storage Account ───────────────────────────────────────────────────────────

resource "azurerm_storage_account" "main" {
  name                     = "${replace(var.project_name, "-", "")}storage"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  access_tier              = "Hot"

  blob_properties {
    versioning_enabled = true   # enables point-in-time recovery of blobs
  }
}

# Raw input containers — one per region
locals {
  regions = ["north", "south", "east", "west", "central"]
}

resource "azurerm_storage_container" "raw_regions" {
  for_each              = toset(local.regions)
  name                  = "${var.project_name}-raw-${each.value}"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "staging" {
  name                  = "${var.project_name}-staging"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "output" {
  name                  = "${var.project_name}-output"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# Lifecycle policy — move blobs to Cool tier after 30 days
resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.main.id

  rule {
    name    = "move-to-cool-after-30-days"
    enabled = true
    filters {
      prefix_match = ["swiftship-output/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
      }
    }
  }
}

# ── Azure Data Factory ────────────────────────────────────────────────────────

resource "azurerm_data_factory" "main" {
  name                = "${var.project_name}-adf"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"   # ADF uses its managed identity to access other resources
  }
}

# Grant ADF access to read from Storage Account
resource "azurerm_role_assignment" "adf_storage_access" {
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.main.id
}

# ── Log Analytics Workspace ───────────────────────────────────────────────────

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project_name}-logs"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

# Send ADF diagnostic logs to Log Analytics
resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                       = "${var.project_name}-adf-diagnostics"
  target_resource_id         = azurerm_data_factory.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log { category = "PipelineRuns" }
  enabled_log { category = "ActivityRuns" }
  enabled_log { category = "TriggerRuns"  }
}

# ── Action Group (Alerts Notification) ───────────────────────────────────────

resource "azurerm_monitor_action_group" "oncall" {
  name                = "${var.project_name}-oncall"
  resource_group_name = azurerm_resource_group.main.name
  short_name          = "oncall"

  email_receiver {
    name          = "oncall_email"
    email_address = var.alert_email
  }
}

# ── Function App ──────────────────────────────────────────────────────────────

resource "azurerm_storage_account" "functions" {
  name                     = "${replace(var.project_name, "-", "")}funcstorage"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_service_plan" "functions" {
  name                = "${var.project_name}-func-plan"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "Y1"   # Consumption (serverless) plan
}

resource "azurerm_linux_function_app" "blob_trigger" {
  name                       = "${var.project_name}-blob-trigger"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  service_plan_id            = azurerm_service_plan.functions.id
  storage_account_name       = azurerm_storage_account.functions.name
  storage_account_access_key = azurerm_storage_account.functions.primary_access_key

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    "ADF_SUBSCRIPTION_ID" = data.azurerm_subscription.current.subscription_id
    "ADF_RESOURCE_GROUP"  = var.resource_group_name
    "ADF_FACTORY_NAME"    = azurerm_data_factory.main.name
    "ADF_PIPELINE_NAME"   = "swiftship_daily_pipeline"
  }

  identity {
    type = "SystemAssigned"
  }
}

# Data source to get current subscription details
data "azurerm_subscription" "current" {}
```

**`terraform/outputs.tf`**

```hcl
# terraform/outputs.tf

output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "storage_account_connection_string" {
  value     = azurerm_storage_account.main.primary_connection_string
  sensitive = true   # masked in terraform output, stored encrypted in state
}

output "adf_name" {
  value = azurerm_data_factory.main.name
}

output "adf_managed_identity_principal_id" {
  value = azurerm_data_factory.main.identity[0].principal_id
}

output "function_app_name" {
  value = azurerm_linux_function_app.blob_trigger.name
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}
```

---

## Step 5 — Deploy with Terraform

```bash
cd terraform

# Initialise (downloads providers)
terraform init

# Plan (shows what will be created/changed/deleted — no changes made)
terraform plan \
  -var="alert_email=oncall@swiftship.com" \
  -out=tfplan

# Review the plan output carefully. Confirm:
# - Resources to add: ~12
# - Resources to change: 0
# - Resources to destroy: 0

# Apply (creates all resources)
terraform apply tfplan

# View outputs
terraform output
terraform output -json storage_account_connection_string   # sensitive
```

**Terraform state warning:** `terraform.tfstate` contains sensitive information (connection strings, keys). Never commit it to git. Add to `.gitignore`:

```gitignore
# .gitignore
*.tfstate
*.tfstate.*
.terraform/
tfplan
```

---

## Step 6 — End-to-End Smoke Test

Create `scripts/test_end_to_end.py`:

```python
# scripts/test_end_to_end.py

import os
import time
from azure.storage.blob import BlobServiceClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import DefaultAzureCredential

STORAGE_CONN_STR  = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
SUBSCRIPTION_ID   = os.environ["AZURE_SUBSCRIPTION_ID"]
RESOURCE_GROUP    = "swiftship-rg"
FACTORY_NAME      = "swiftship-adf"

print("=== SwiftShip Azure Cloud ETL — End-to-End Smoke Test ===\n")

# Step 1: Upload a test file to north region container
print("[1/4] Uploading test file to swiftship-raw-north...")
blob_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
container_client = blob_client.get_container_client("swiftship-raw-north")

test_content = b"""shipment_id,origin_city,destination_city,dispatch_date,delivery_date,status,weight_kg
SMOKE001,mumbai,delhi,2024-01-10,2024-01-13,DELIVERED,12.5
SMOKE002,pune,bangalore,2024-01-11,,IN_TRANSIT,8.2
"""
container_client.upload_blob("north_region.csv", test_content, overwrite=True)
print("  Uploaded north_region.csv (2 test records)")

# Wait for Event Grid → Function → ADF trigger chain
print("[2/4] Waiting 30 seconds for event-driven trigger chain...")
time.sleep(30)

# Step 2: Check ADF for a recent pipeline run
print("[3/4] Checking ADF for recent pipeline run...")
credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, SUBSCRIPTION_ID)

runs = list(adf_client.pipeline_runs.query_by_factory(
    RESOURCE_GROUP, FACTORY_NAME,
    filter_parameters={
        "lastUpdatedAfter":  "2024-01-01T00:00:00Z",
        "lastUpdatedBefore": "2030-01-01T00:00:00Z",
        "filters": [{"operand": "PipelineName", "operator": "Equals",
                     "values": ["swiftship_daily_pipeline"]}]
    }
).value)

if not runs:
    print("  ERROR: No pipeline runs found. Check Event Grid and Function App logs.")
else:
    latest = runs[0]
    print(f"  Latest run: {latest.run_id}")
    print(f"  Status    : {latest.status}")
    print(f"  Duration  : {latest.duration_in_ms}ms")

    if latest.status == "Succeeded":
        print("  [PASS] Pipeline run succeeded.")
    else:
        print(f"  [FAIL] Pipeline run status: {latest.status}")
        if latest.message:
            print(f"  Error: {latest.message}")

# Step 3: Verify output blob exists
print("[4/4] Checking for output Parquet files...")
output_client = blob_client.get_container_client("swiftship-output")
blobs = list(output_client.list_blobs(name_starts_with="shipments/"))
if blobs:
    print(f"  [PASS] Found {len(blobs)} Parquet file(s) in swiftship-output/shipments/")
    for b in blobs[:3]:
        print(f"    {b.name} ({b.size:,} bytes)")
else:
    print("  [FAIL] No Parquet files found. Databricks job may have failed.")

print("\n=== Smoke Test Complete ===")
```

Run: `python scripts/test_end_to_end.py`

---

## Day 3 Checklist

Before calling Project 07 complete, confirm:

- [ ] `terraform apply` completes with 0 errors and creates all resources
- [ ] `terraform plan` after apply shows: `No changes. Infrastructure is up-to-date.`
- [ ] `az monitor alert-rule list --resource-group swiftship-rg` shows the alert rules
- [ ] Budget alert appears in Azure Portal → Cost Management → Budgets
- [ ] Log Analytics workspace receives ADF logs — query `ADFPipelineRun | take 10` returns rows
- [ ] `scripts/test_end_to_end.py` passes all 4 checks
- [ ] `terraform destroy` cleanly removes all resources (run at end of project to avoid ongoing cost)
- [ ] `.gitignore` excludes `*.tfstate`, `.terraform/`, and any files containing credentials

---

## Clean Up

When you are done with this project, destroy all Azure resources to stop incurring cost:

```bash
cd terraform
terraform destroy -var="alert_email=oncall@swiftship.com"

# Verify everything is gone
az resource list --resource-group swiftship-rg --output table
# Expected: empty table

# Delete the resource group
az group delete --name swiftship-rg --yes --no-wait
```

---

*With Project 07 complete, you have deployed a production-grade event-driven cloud ETL pipeline on Azure — the same architecture used by enterprise data teams worldwide. You have covered every major Azure DE service: Blob Storage, Event Grid, Azure Functions, Data Factory, Databricks, Synapse, Monitor, and Terraform. This project, combined with Projects 01–06, gives you a complete, demonstrable data engineering portfolio.*
