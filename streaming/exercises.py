# Databricks notebook source
BASE_PATH = "teste-1"
EXERCISE_NAME = "workshop-exercise-1"


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import random


# Define the schema for the generated events
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("order_value", FloatType(), True),
    StructField("timestamp", TimestampType(), True),  # Add timestamp to the schema
])

# Define a generator function to create random events
def generate_merchant_id():
    # Update the choices for you favorite restaurants
    return random.choice(["A", "B", "C"])

def generate_order_value():
    return float(random.uniform(10, 100))

# Register the UDFs to be used with the streaming DataFrame
generate_merchant_id_udf = udf(generate_merchant_id, StringType())
generate_order_value_udf = udf(generate_order_value, FloatType())

spark.udf.register("generate_merchant_id", generate_merchant_id, StringType())
spark.udf.register("generate_order_value", generate_order_value, FloatType())

# Create a streaming DataFrame using the generator and schema
streaming_df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .selectExpr("value as order_id")

# Apply the UDFs to generate the event data
streaming_df = streaming_df \
    .withColumn("merchant_id", generate_merchant_id_udf()) \
    .withColumn("order_value", generate_order_value_udf()) \
    .withColumn("timestamp", current_timestamp())  # Add current timestamp

# Write the streaming DataFrame to a Delta table
query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", f"/{BASE_PATH}/{EXERCISE_NAME}/checkpoint") \
    .option("path", f"{BASE_PATH}/{EXERCISE_NAME}/data") \
    .start()

# Wait for the stream to end
# query.awaitTermination()

# COMMAND ----------

df_events = spark.read.format("delta").load(f"/{BASE_PATH}/{EXERCISE_NAME}/data")

display(df_events)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Exercise 1: Read Table Using Streaming and use group by function
# MAGIC In this exercise, you will learn how to read a streaming Delta table in PySpark and perform a windowing aggregation. Specifically, you will count the number of orders for each merchant and also get the average ticket for each merchant in the dataset.
# MAGIC
# MAGIC Here is your task: Count the number of orders for each merchant and the average ticket, using the "merchant_id" field as the key. Use the groupBy function in PySpark to do this.

# COMMAND ----------

# Your code goes here 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Exercise 2: Windowing with Watermark
# MAGIC In the second exercise, you will be using a technique known as "windowing with watermark".
# MAGIC
# MAGIC Your task: Read the same Delta table, group the data by "merchant_id" and count the number of orders in each window of time, say 1 minute and with a step of 10 seconds. Use a watermark of 10 seconds to allow late data.

# COMMAND ----------

# Your code goes here 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Exercise 3: Using Arbitrary Statefull Aggregation
# MAGIC
# MAGIC In this exercise, you will be using arbitrary stateful processing. This allows you to perform more complex calculations over the data stream.
# MAGIC
# MAGIC Your task: Implement the same aggregation as in the previous exercises, but this time using arbitrary stateful processing.
# MAGIC
# MAGIC
# MAGIC > Remember, the flatMapGroupWithState only works in `Scala`.

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // your code goes here

# COMMAND ----------


