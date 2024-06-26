# Databricks notebook source
# MAGIC %md
# MAGIC ## Autoloader Demo
# MAGIC
# MAGIC This demo shows how to build a pipeline that uses Autoloader.
# MAGIC
# MAGIC Autoloader is a feature that can be used to incrementally ingest files from cloud storage.
# MAGIC
# MAGIC Auto Loader can ingest JSON, CSV, XML, PARQUET, AVRO, ORC, TEXT, and BINARYFILE file formats.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC To start the demo, let's create several variables that will be used across the entire demonstration.

# COMMAND ----------

# DBTITLE 1,Import several auxiliar functions to speed up demo process
# MAGIC %run ./demo_utils

# COMMAND ----------

# DBTITLE 1,Defining variables and reseting demo state
# variables
database = "leandro.demos"
input_dir = "dbfs:/tmp/leandro/demos/input_files"
bronze_table = f"{database}.country_sales_interim"
silver_table = f"{database}.country_sales"

#Initialize the data generator that will be used for the demo
sales_gen = GeneratorJson(num_lines=10, input_dir=input_dir)

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

# DBTITLE 1,creating a first batch of files
# MAGIC %%capture --no-display
# MAGIC countries = ["USA", "Canada", "Mexico", "Brazil"]     # Example list of countries
# MAGIC
# MAGIC for country in countries:
# MAGIC     sales_gen.generate_data(country)

# COMMAND ----------

# DBTITLE 1,Show how the files got arranged
tree(input_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using Autoloader
# MAGIC
# MAGIC To use Autoloader, a Structured Streaming process must be created, along with the process you pass `.format("cloudFiles")` to enable it.
# MAGIC
# MAGIC Autoloder has the following advantages:
# MAGIC
# MAGIC * Exactly one file processing, no more delta calculations for incremental loads
# MAGIC * Support for schema inference and schema evolution
# MAGIC * Scalable to millions of files if needed by activating `notification mode`
# MAGIC * Plenty of configurable [extra options](https://docs.databricks.com/en/ingestion/auto-loader/options.html) for each file type processing needs

# COMMAND ----------

# DBTITLE 1,Defining a Structured Streaming Process to read files from input dir
import pyspark.sql.functions as F

# Read the data using AutoLoader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", f"{input_dir}/_schema/{bronze_table}")
      .load(input_dir)
      .withColumn("filename", F.input_file_name())
      .withColumn("ingestion_time", F.current_timestamp()))

#display(df)

# COMMAND ----------

# DBTITLE 1,Now we can write the autoloader data into a table
query = (df .writeStream
            .outputMode("append")
            .option("checkpointLocation", f"{input_dir}/_checkpoints/{bronze_table}")
            .trigger(availableNow=True)
            .table(bronze_table))

# COMMAND ----------

# DBTITLE 1,Tip: you can have the entire streaming process as a single statement
def process_bronze_layer():
    return (spark   .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.inferColumnTypes", "true")
                    .option("cloudFiles.schemaLocation", f"{input_dir}/_schema/{bronze_table}")
                    .load(input_dir)
                    .withColumn("filename", F.input_file_name())
                    .withColumn("ingestion_time", F.current_timestamp())
                    .writeStream
                    .outputMode("append")
                    .option("checkpointLocation", f"{input_dir}/_checkpoints/{bronze_table}")
                    .trigger(availableNow=True)
                    .table(bronze_table))

# COMMAND ----------

# DBTITLE 1,Check data at the output
display(spark.sql(f"select * from {bronze_table} limit 5"))

# COMMAND ----------

# DBTITLE 1,Check the amount of input rows
display(spark.sql(f"select count(1) from {bronze_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing silver data
# MAGIC
# MAGIC Now that the data was ingested, it is possible to pull this data incrementally from the Bronze layer, transform it and push it to the next layers. Data processed from delta tables are more efficient due to the [Disk Cache](https://docs.databricks.com/en/optimizations/disk-cache.html) which uses the available SSDs to stage sections of the tables, creating a cache operation that is less prone to memory spill.
# MAGIC
# MAGIC Another advantage of querying data from a Delta table is that you may optimize the table layout to support given operations by using `optimize`, `zorder` and `liquid clustering`.
# MAGIC
# MAGIC The following scenario shows how to transform data using Structured Streaming.

# COMMAND ----------

# DBTITLE 1,Create an example transformation
# apply BRL currency to sales
def brazil_currency_update(df):
    return df.withColumn("sales", F.expr("sales * 5.50"))

# COMMAND ----------

# DBTITLE 1,Generate a stream process that apply that transformation to Brazil data
brazil_stream = (spark  .readStream
                        .table(bronze_table)
                        .filter("country = 'Brazil'")
                        .transform(brazil_currency_update)
                        .writeStream
                        .option("checkpointLocation", f"{input_dir}/_checkpoints/{silver_table}/brazil")
                        .trigger(availableNow=True)                        
                        .table(silver_table))

# COMMAND ----------

# DBTITLE 1,Move the other countries data to the same table without applying any changes
other_countries_stream = (spark  .readStream
                        .table(bronze_table)
                        .filter("country <> 'Brazil'")
                        .withColumn("sales", F.col("sales").cast("decimal (24,2)"))
                        .writeStream
                        .option("checkpointLocation", f"{input_dir}/_checkpoints/{silver_table}/other")
                        .trigger(availableNow=True)                        
                        .table(silver_table))

# COMMAND ----------

# DBTITLE 1,check data at the ouput
display(spark.sql(f"select * from {silver_table} limit 5"))

# COMMAND ----------

# DBTITLE 1,check number of rows
# MAGIC %sql select count(1) from leandro.demos.country_sales

# COMMAND ----------

# DBTITLE 1,Generate a second batch of data
# MAGIC %%capture --no-display
# MAGIC sales_gen.generate_data("Peru")
# MAGIC sales_gen.generate_data("Canada")
# MAGIC sales_gen.generate_data("Brazil")

# COMMAND ----------

process_bronze_layer()
