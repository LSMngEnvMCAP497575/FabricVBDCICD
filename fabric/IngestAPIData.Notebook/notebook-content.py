# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# MAGIC %%configure 
# MAGIC { 
# MAGIC "defaultLakehouse":  
# MAGIC { 
# MAGIC "name": { "parameterName": "TargetLakehouseName","defaultValue": "DemoLakehouse" }, 
# MAGIC 
# MAGIC "id": { "parameterName": "TargetLakehouseId", "defaultValue": "23884490-f2f9-449a-8d23-b77b54219534" },  
# MAGIC 
# MAGIC "workspaceId": { "parameterName": "TargetWorkspaceId","defaultValue": "0abd6452-08cc-4d01-86f1-b5e99f4c4701" }  
# MAGIC } 
# MAGIC } 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
