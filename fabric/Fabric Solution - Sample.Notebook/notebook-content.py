# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#### Please Enter Values to define following Parameters #####

Feature_WORKSPACE_ID = "0abd6452-08cc-4d01-86f1-b5e99f4c4701"
Feature_LAKEHOUSE_ID="23884490-f2f9-449a-8d23-b77b54219534"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### Don't Change anything from this code cell onwards #####
LAKEHOUSE_NAME  = "DemoLakehouse" 
NOTEBOOK_NAME   = "IngestAPIData"
PIPELINE_NAME   = "API_Ingestion_Pipeline"

# === Token + API headers (unchanged) ===
try:
    from notebookutils import credentials as nb_creds
except ImportError:
    import notebookutils
    nb_creds = notebookutils.credentials

tok = nb_creds.getToken('pbi')
ACCESS_TOKEN = getattr(tok, "token", tok)

API = "https://api.fabric.microsoft.com/v1"
HEADERS_JSON = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, json, time

NOTEBOOK_IDS = {}

# --- Feature only: create/reuse notebook ---
print(f"\n=== Feature :: Notebook ===")
ws_id = Feature_WORKSPACE_ID
r = requests.get(f"{API}/workspaces/{ws_id}/items?itemType=Notebook", headers=HEADERS_JSON, timeout=90)
r.raise_for_status()
nb_id = next((it["id"] for it in r.json().get("value", []) if it.get("displayName") == NOTEBOOK_NAME), None)
if nb_id:
    print(f"[Notebook] Reusing shell: {NOTEBOOK_NAME} ({nb_id})")
else:
    body = {"displayName": NOTEBOOK_NAME, "type": "Notebook", "description": "Public API → Lakehouse Delta (shell)"}
    r = requests.post(f"{API}/workspaces/{ws_id}/items", headers=HEADERS_JSON, json=body, timeout=90)
    r.raise_for_status()
    nb_id = r.json()["id"]
    print(f"[Notebook] Created shell: {NOTEBOOK_NAME} ({nb_id})")
NOTEBOOK_ID= nb_id
print(f"[Feature] NOTEBOOK_ID: {nb_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64, json, requests

ingest_code = """
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

resp = requests.get("https://countriesnow.space/api/v0.1/countries/population/cities", timeout=60)
resp.raise_for_status()
data = resp.json()

# The API returns the data nested under a "data" key
data_list = data.get("data", [])

spark = SparkSession.builder.getOrCreate()
df = spark.read.json(spark.sparkContext.parallelize(data_list))

if "country" in df.columns and "populationCounts" in df.columns:
    df = df.withColumn("population", col("populationCounts").getItem(0).getItem("value"))
    df.write.mode("overwrite").format("delta").saveAsTable("Countries")
    print("Rows:", df.count())
"""

print(f"\n=== Feature :: Update Notebook Definition ===")
WORKSPACE_ID = Feature_WORKSPACE_ID
LAKEHOUSE_ID = Feature_LAKEHOUSE_ID
nb_id = NOTEBOOK_ID

config_code = f"""%%configure
{{"defaultLakehouse": {{"workspaceId": "{WORKSPACE_ID}", "lakehouseId": "{LAKEHOUSE_ID}", "name": "{LAKEHOUSE_NAME}"}} }}
"""

ipynb = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {"language_info": {"name": "python"},
                 "kernelspec": {"name": "synapse_pyspark", "language": "python", "display_name": "PySpark"}},
    "cells": [
        {"cell_type": "code", "metadata": {}, "source": config_code.splitlines(True), "outputs": [], "execution_count": 0},
        {"cell_type": "code", "metadata": {}, "source": ingest_code.splitlines(True), "outputs": [], "execution_count": 0}
    ]
}
nb_b64 = base64.b64encode(json.dumps(ipynb).encode("utf-8")).decode("utf-8")

url = f"{API}/workspaces/{WORKSPACE_ID}/items/{nb_id}/updateDefinition"
payload = {
    "definition": {
        "format": "ipynb",
        "parts": [{"path": "notebook.ipynb", "payload": nb_b64, "payloadType": "InlineBase64"}]
    }
}
r = requests.post(url, headers=HEADERS_JSON, json=payload, timeout=90)
print("POST updateDefinition ->", r.status_code, getattr(r, "text", "")[:200])
r.raise_for_status()
print("[Notebook] Definition updated.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, json

PIPELINE_ID = {}

print(f"\n=== Feature :: Pipeline ===")
ws_id = Feature_WORKSPACE_ID
r = requests.get(f"{API}/workspaces/{ws_id}/items?itemType=DataPipeline", headers=HEADERS_JSON, timeout=90)
r.raise_for_status()
pipe_id = next((it["id"] for it in r.json().get("value", []) if it.get("displayName") == PIPELINE_NAME), None)
if pipe_id:
    print(f"[Pipeline] Reusing shell: {PIPELINE_NAME} ({pipe_id})")
else:
    payload = {"displayName": PIPELINE_NAME, "type": "DataPipeline", "description": "Runs the ingestion notebook"}
    r = requests.post(f"{API}/workspaces/{ws_id}/items", headers=HEADERS_JSON, json=payload, timeout=90)
    r.raise_for_status()
    pipe_id = r.json()["id"]
    print(f"[Pipeline] Created shell: {PIPELINE_NAME} ({pipe_id})")
PIPELINE_ID = pipe_id
print(f"[Feature] PIPELINE_ID: {pipe_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, base64, requests

print(f"\n=== Feature :: Set Pipeline Definition ===")
WORKSPACE_ID = Feature_WORKSPACE_ID
pipe_id = PIPELINE_ID
nb_id = NOTEBOOK_ID

pipeline_def_json = {
  "properties": {
    "activities": [
      {
        "name": "RunIngestionNotebook",
        "type": "TridentNotebook",
        "dependsOn": [],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 0,
          "retryIntervalInSeconds": 30,
          "secureOutput": False,
          "secureInput": False
        },
        "typeProperties": {
          "notebookId": nb_id,
          "workspaceId": WORKSPACE_ID
        }
      }
    ]
  }
}

payload = {
  "definition": {
    "parts": [{
      "path": "pipeline-content.json",
      "payload": base64.b64encode(json.dumps(pipeline_def_json).encode("utf-8")).decode("utf-8"),
      "payloadType": "InlineBase64"
    }]
  }
}

url = f"{API}/workspaces/{WORKSPACE_ID}/dataPipelines/{pipe_id}/updateDefinition"
r = requests.post(url, headers=HEADERS_JSON, json=payload, timeout=90)
print("updateDefinition ->", r.status_code, r.text[:200])
r.raise_for_status()
print("[Pipeline] Definition set (TridentNotebook).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, time, json

terminal = {"Completed", "Succeeded", "Failed", "Cancelled"}
success = {"Completed", "Succeeded"}

print(f"\n=== Feature :: Trigger + Poll Pipeline ===")
WORKSPACE_ID = Feature_WORKSPACE_ID
pipe_id = PIPELINE_ID

run_url = f"{API}/workspaces/{WORKSPACE_ID}/items/{pipe_id}/jobs/instances?jobType=Pipeline"
start = requests.post(
    run_url,
    headers=HEADERS_JSON,
    json={"executionData": {"pipelineName": PIPELINE_NAME}},
    timeout=90
)
print("POST run ->", start.status_code, start.text[:200])
start.raise_for_status()

loc = start.headers.get("Location") or start.headers.get("Operation-Location")
print("Location:", loc)

status = "InProgress"
time.sleep(20)

for i in range(60):
    r = requests.get(loc, headers=HEADERS_JSON, timeout=90)
    if r.ok:
        data = r.json()
        status = data.get("status") or data.get("properties", {}).get("status") or status
        print(f"[Feature poll {i}] -> {status}")
        if status in terminal:
            print(f"[Feature] Pipeline status:", status, "(OK)" if status in success else "(NOT OK)")
            if status not in success:
                print("Details:", json.dumps(data, indent=2)[:1000])
            break
    else:
        print(f"[Feature poll {i}] error {r.status_code}: {r.text[:200]}")
    time.sleep(2)

if status not in terminal:
    print(f"[Feature] Pipeline did not reach terminal state within timeout.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Recommended Note**
# 
# 
# If the previous step completes successfully, but you see the message:
# **[Feature] Pipeline did not reach terminal state within timeout**
# 
# 
# This typically occurs due to temporary environment slowness and does not indicate a failure.
# 
# 
# _**Next Steps:**_
# 
# Navigate to **Monitoring / Monitor Hub**
# Check the **pipeline execution status**
# Wait until the execution completes successfully
# 
# 
# 
# _**Important:**_
# 
# **Do not rerun the notebook**, as this may trigger duplicate executions or conflicts.
