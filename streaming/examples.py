# Databricks notebook source
BUCKET_NAME = "path"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import random


# Define the schema for the generated events
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("words", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

# Define a generator function to create random words
def generate_random_word():
    words = ["apple", "BaNana", "banana","cherry", "mango", "inova" ,"INOVA", "iFood"]
    return random.choice(words)

# Register the UDF to be used with the streaming DataFrame
generate_random_word_udf = udf(generate_random_word, StringType())
spark.udf.register("generate_random_word", generate_random_word, StringType())

# Create a streaming DataFrame using the generator and schema
streaming_df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .selectExpr("value as id")

# Apply the UDF to generate the random word
streaming_df = streaming_df \
    .withColumn("random_word", generate_random_word_udf()) \
    .withColumn("timestamp", current_timestamp())  # Add current timestamp

# Write the streaming DataFrame to a Delta table
query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", f"{BUCKET_NAME}/workshop-exemple-word-count/data-generator/checkpoint") \
    .option("path", f"{BUCKET_NAME}/workshop-exemple-word-count/data-generator/data") \
    .start()

# Wait for the stream to end
# query.awaitTermination()


# COMMAND ----------

df_events = spark.read.format("delta").load(f"{BUCKET_NAME}/workshop-exemple-word-count/data-generator/data")

# COMMAND ----------

display(df_events)

# COMMAND ----------

# Read the Delta table as a streaming DataFrame
streaming_df = spark \
    .readStream \
    .format("delta") \
    .load(f"{BUCKET_NAME}/workshop-exemple-word-count/data-generator/data")  # Replace with your actual path

# Group by the "random_word" column and count the occurrences of each word
word_counts = streaming_df.groupBy("random_word").count()

# Write the streaming word counts to the console
# query = word_counts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("checkpointLocation", f"{BUCKET_NAME}/workshop-exemple-word-count/word-count-simples/checkpoint") \
#     .start()

display(word_counts)

# COMMAND ----------

import pyspark.sql.functions as F


# Read the Delta table as a streaming DataFrame
streaming_df = spark \
    .readStream \
    .format("delta") \
    .load(f"{BUCKET_NAME}/workshop-exemple-word-count/data-generator/data")  # Replace with your actual path

# Group by the "random_word" column and count the occurrences of each word
word_counts = (
  streaming_df
  .withColumn("random_word_lowercase", F.lower("random_word"))
  .groupBy("random_word_lowercase").count()
)

# Write the streaming word counts to the console
# query = word_counts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("checkpointLocation", f"{BUCKET_NAME}/workshop-exemple-word-count/word-count-lower-case/checkpoint") \
#     .start()

display(word_counts)

# COMMAND ----------

import pyspark.sql.functions as F


# Read the Delta table as a streaming DataFrame
streaming_df = spark \
    .readStream \
    .format("delta") \
    .load(f"{BUCKET_NAME}/workshop-exemple-word-count/data-generator/data")  # Replace with your actual path

# Group by the "random_word" column and count the occurrences of each word
word_counts = (
  streaming_df
  .withColumn("random_word_lowercase", F.lower("random_word"))
  .groupBy("random_word_lowercase").count()
)

# Write the streaming word counts to the console
# query = word_counts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("checkpointLocation", f"{BUCKET_NAME}/workshop-exemple-word-count/word-count-lower-case/checkpoint") \
#     .start()

display(word_counts)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val BUCKET_NAME = "path"

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC
# MAGIC import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
# MAGIC import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.GroupState
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.catalyst.encoders.RowEncoder
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC import org.apache.spark.sql.{DataFrame, SparkSession}
# MAGIC
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC // Define the WordCount case class
# MAGIC   val OUTPUT_SCHEMA: StructType = new StructType()
# MAGIC     .add("word", StringType)
# MAGIC     .add("count", LongType)
# MAGIC
# MAGIC     
# MAGIC // Define the mapping function
# MAGIC def flatMapGroupsWithStateFunction
# MAGIC (
# MAGIC   id: String,
# MAGIC   iterator: Iterator[Row],
# MAGIC   state: GroupState[Map[String, Long]]
# MAGIC ): Iterator[Row] = {
# MAGIC   // FIXME: We are only considering 1x for each microbatch. We should iterate over all rows and process each of them.
# MAGIC   var wordCount = state.getOption.getOrElse(Map())
# MAGIC
# MAGIC   val updatedCount:Long = wordCount.get(id).map(_ + 1).getOrElse(1)
# MAGIC   val wordCountUpdated: Map[String, Long] = wordCount + (id -> updatedCount)
# MAGIC
# MAGIC   state.update(wordCountUpdated)
# MAGIC   val rowIterator: Iterator[Row] = wordCountUpdated.map { case (key, value) => Row(key, value.toLong) }.iterator
# MAGIC
# MAGIC   rowIterator
# MAGIC }
# MAGIC
# MAGIC val spark = SparkSession.builder().getOrCreate()
# MAGIC import spark.implicits._
# MAGIC
# MAGIC // Read the Delta table as a streaming DataFrame
# MAGIC val df = spark.readStream.format("delta").load(s"${BUCKET_NAME}/workshop-exemple-word-count/data-generator/data")
# MAGIC
# MAGIC val inputColumnSchema: StructType = df.schema
# MAGIC
# MAGIC val groupedDataFrame = {
# MAGIC   implicit val inputRowEncoder= RowEncoder(inputColumnSchema)
# MAGIC
# MAGIC   df
# MAGIC     .as[Row]
# MAGIC     .groupByKey { row: Row =>
# MAGIC       row.getAs[String]("random_word")
# MAGIC     }
# MAGIC }
# MAGIC
# MAGIC // Split the words into separate rows
# MAGIC val words = df.selectExpr("explode(split(random_word, ' ')) as word")
# MAGIC
# MAGIC // Convert the DataFrame of words to Dataset[WordCount]
# MAGIC
# MAGIC
# MAGIC val wordCounts = {
# MAGIC   implicit val outputRowEncoder = RowEncoder(OUTPUT_SCHEMA)
# MAGIC   groupedDataFrame
# MAGIC           .flatMapGroupsWithState(
# MAGIC             outputMode = OutputMode.Update(),
# MAGIC             timeoutConf = GroupStateTimeout.ProcessingTimeTimeout()
# MAGIC   )(
# MAGIC     flatMapGroupsWithStateFunction
# MAGIC   )
# MAGIC   .toDF()
# MAGIC }
# MAGIC
# MAGIC def saving(dataFrame: DataFrame, batchId: Long): Unit = {
# MAGIC   dataFrame
# MAGIC     .write
# MAGIC     .format("delta")
# MAGIC     .mode("append")
# MAGIC     .save(s"${BUCKET_NAME}/workshop-exemple-word-count-arbirtary-agg/data")
# MAGIC }
# MAGIC wordCounts
# MAGIC           .writeStream
# MAGIC           .queryName("query_nrt")
# MAGIC           .option("checkpointLocation",f"${BUCKET_NAME}/workshop-exemple-word-count-arbirtary-agg/checkpoint")
# MAGIC           .format("delta")
# MAGIC           .outputMode("update")
# MAGIC           /*
# MAGIC            * This is a required trick. Because it uses flatMapGroupsWithState in update mode, we also need to use update mode here.
# MAGIC            * However, we will not be able to write using update mode ("... DeltaDataSource does not support Update output mode").
# MAGIC            * So, to overcome it, we use a foreachBath and, inside it, we perform some appends.
# MAGIC            *
# MAGIC            * Due to some changes in Scala 2.12, the method DataStreamWriter.foreachBatch requires some updates on the code,
# MAGIC            * otherwise this ambiguity happens. Because of that we create a new method to pass to foreachBatch instead
# MAGIC            * the writer the code inside it.
# MAGIC            * Source: https://issues.apache.org/jira/browse/SPARK-26132?focusedCommentId=17178019&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-17178019
# MAGIC            */
# MAGIC           .foreachBatch(saving _)
# MAGIC           .trigger(Trigger.ProcessingTime(5))
# MAGIC           .start()

# COMMAND ----------


df = spark.read.format("delta").load(f"{BUCKET_NAME}/workshop-exemple-word-count-arbirtary-agg/data")
display(df)

# COMMAND ----------


