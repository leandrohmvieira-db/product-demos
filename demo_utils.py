# Databricks notebook source
# DBTITLE 1,Install some libraries
!pip install seedir -q
!pip install --upgrade databricks-sdk -q
!pip install dbldatagen -q
dbutils.library.restartPython()

# COMMAND ----------

host_name = 'https://'+dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get() # get current workspace URL
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() # get the current session token

# COMMAND ----------

def dbfs_to_fuse(path):

    if path.startswith("dbfs:/Volumes"):
        return path.replace("dbfs:", "", 1)
    elif path.startswith("dbfs:"):
        return path.replace("dbfs:", "/dbfs", 1)
    else:
        return path

# COMMAND ----------

import seedir as sd
def tree(dir,display_limit=5):
    sd.seedir(dbfs_to_fuse(dir),itemlimit=(None,display_limit),beyond='content')

# COMMAND ----------

# DBTITLE 1,Small data generator, day by day append
from datetime import datetime, timedelta
import random
import json

class GeneratorJson:
    def __init__(self, num_lines, input_dir):
        self.num_lines = num_lines
        self.starting_date = datetime.now().date()
        self.input_dir = input_dir
        self.countries = []
    
    def _iterate_country(self, country):
        country_found = False
        for entry in self.countries:
            if entry['country'] == country:
                entry['starting_date'] += timedelta(days=1)
                entry['last_sales_id'] += self.num_lines
                country_found = True
                break
        if not country_found:
            self.countries.append({'country': country, 'starting_date': self.starting_date, 'last_sales_id': self.num_lines})
        
        return next((x for x in self.countries if x["country"] == country), None)
    
    def generate_data(self, country):
        country_dict = self._iterate_country(country)
        country_dir = f"{self.input_dir}/{country}"
        dbutils.fs.mkdirs(country_dir)
        for i in range(country_dict["last_sales_id"] - self.num_lines,country_dict["last_sales_id"]):
            data = {
                "sales_id": i,
                "country": country,
                "date": str(country_dict['starting_date']),
                "sales": i * 100,  # Example sales data
                "units_sold": random.randint(1, 100),  # Random units sold
                "discount": round(random.uniform(0, 0.5), 2)  # Random discount between 0 and 0.5
            }
            file_path = f"{country_dir}/data_{i}.json"
            dbutils.fs.put(file_path, json.dumps(data), overwrite=True)

# COMMAND ----------

# DBTITLE 1,Volume generator, for big data
import dbldatagen as dg

from pyspark.sql.types import FloatType, IntegerType, StringType

class GeneratorDataframe:
      def __init__(self, num_lines, num_float_columns = 1, num_string_columns=1, customer_range=5,starting_date=datetime.now(), num_partitions=spark.sparkContext.defaultParallelism):
            self.num_lines = num_lines
            self.num_float_columns = num_float_columns
            self.num_string_columns = num_string_columns
            self.customer_range = range(1,customer_range)
            self.starting_date = starting_date
            self.num_partitions = num_partitions
            self.starting_id = 1

      def generate_data(self):

            data_spec = (
                  dg.DataGenerator(spark, name="syslog_data_set", rows=self.num_lines, partitions=self.num_partitions, randomSeedMethod='hash_fieldname')
                  .withColumn("transaction_id", "long",minValue=self.starting_id, maxValue=self.starting_id + self.num_lines)
                  .withColumn("transaction_timestamp", "timestamp",
                              begin=self.starting_date,
                              end=self.starting_date + timedelta(days=1),
                              interval="1 second", random=True)
                  .withColumn("customer", StringType(), values=self.customer_range)
                  .withColumn("ip", StringType(), values=['127.0.0.1', '0.0.0.1', '142.32.74.255','500.50.2.1'])
                  .withColumn("line_status", StringType(), values=['approved', 'reproved'], random=True, weights=[8, 2])
                  .withColumn("float_value", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",numColumns=self.num_float_columns)
                  .withColumn("message", text=dg.ILText(paragraphs=(1), sentences=(2, 10)), numColumns=self.num_string_columns))
            
            result_set =  data_spec.build()

            self.starting_date += timedelta(days=1)
            self.starting_id += self.num_lines

            return result_set
