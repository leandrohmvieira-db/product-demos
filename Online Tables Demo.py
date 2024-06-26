# Databricks notebook source
# MAGIC %md
# MAGIC ## Online tables demo
# MAGIC
# MAGIC A Databricks Online Table is a read-only copy of a Delta Table that is stored in a row-oriented format optimized for online access. These tables are fully serverless, auto-scaling throughput capacity with the request load, and provide low latency and high throughput access to data of any scale. **Online tables can only be accessed by SQL Warehouses**
# MAGIC
# MAGIC This demo shows how to create Online tables and do a small query benchmark against regular DBSQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC To start the demo, let's create several variables that will be used across the entire demonstration.

# COMMAND ----------

# DBTITLE 1,Import several auxiliar functions to speed up demo process
# MAGIC %run ./demo_utils

# COMMAND ----------

# variables
database = "leandro.demos"
input_dir = "dbfs:/tmp/leandro/demos/online_tables"
base_table = f"{database}.base_table"

#Initialize the data generator that will be used for the demo
df_generator = GeneratorDataframe(num_lines=5*1000*1000)

# reset demo state
spark.sql(f"drop database if EXISTS {database} cascade")
spark.sql(f"CREATE DATABASE if not EXISTS {database}")
dbutils.fs.rm(input_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Generation
# MAGIC
# MAGIC This step will generate some synthetic data to work on. The generated data will be landed in an input directory. The data generator supports creating partitioned data, and providing unique_ids for each partition.

# COMMAND ----------

# DBTITLE 1,generating a 5 million rows dataset
df = df_generator.generate_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating benchmark tables
# MAGIC
# MAGIC Now that we have our dataset, let's create 4 tables and do a "needle in haystack query", looking for a given id. The tables created are:
# MAGIC
# MAGIC * basic delta table
# MAGIC * z-order optimized delta table
# MAGIC * Snapshot delta table
# MAGIC * Triggered delta table

# COMMAND ----------

# DBTITLE 1,write it to a delta table
# creating a basic delta table by writing the dataset
df.write.mode("overwrite").saveAsTable(base_table)

# Creating a z-order optimized table
# When searching from a high cardinality column on a table, z-ordering might help on those scenarios
spark.sql(f"create table {base_table}_optimized as select * from {base_table}")
spark.sql(f"optimize {base_table}_optimized zorder by transaction_id")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Enabling Online table using the catalog UI
# MAGIC
# MAGIC In this step, we will create a online table from the Data Catalog interface.
# MAGIC
# MAGIC We will create a Snapshot Online Table, which needs to be **fully recomputed**. This is not the best option for big tables, there are another methods that will be created below

# COMMAND ----------

# this step will happen on UI

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Enabling online tables by API
# MAGIC
# MAGIC In this demo, we will use `databricks-sdk` library to make the requests, but you can also use standard web requests.
# MAGIC
# MAGIC Online tables support on `databricks-sdk` starts on version `0.20`
# MAGIC
# MAGIC Another important aspect is: **to create triggered or continuous sync tables, `change data feed` must be enabled on the source table**
# MAGIC

# COMMAND ----------

# create a table with same schema from base table
spark.sql(f"""
CREATE TABLE if not exists {base_table}_api
AS 
SELECT * FROM {base_table} limit 0;
""")

# enable change data feed for the new table
spark.sql(f"alter table {base_table}_api set tblproperties('delta.enableChangeDataFeed' = 'true')")

# populate the new table
spark.sql(f"INSERT INTO {base_table}_api SELECT * FROM {base_table}")

# COMMAND ----------

from pprint import pprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *

w = WorkspaceClient(host=host_name, token=token)

# Create an online table
spec = OnlineTableSpec(
  primary_key_columns=["transaction_id"],
  source_table_full_name=f"{base_table}_api",
  run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({'triggered': 'true'})
)

w.online_tables.create(name=f"{base_table}_api_online", spec=spec)

# COMMAND ----------

pprint(w.online_tables.get(f"{base_table}_api_online"))

# COMMAND ----------

#w.online_tables.delete(f"{base_table}_api_online")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query comparations
# MAGIC
# MAGIC Let's see some benchmarks for a haystack query, we will fetch a single row from the table. 

# COMMAND ----------

# MAGIC %sql select 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT needle DEFAULT "1"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from leandro.demos.base_table where transaction_id = ${needle}

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from leandro.demos.base_table_optimized where transaction_id = ${needle}

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from leandro.demos.base_table_online where transaction_id = ${needle}

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from leandro.demos.base_table_api_online where transaction_id = ${needle}
