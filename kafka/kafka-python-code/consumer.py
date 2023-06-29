import findspark
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, col, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import os
from dotenv import load_dotenv

load_dotenv()

findspark.init()
topic_name = os.getenv('kafka_topic_name')

# spark_path = findspark.find()
# print(spark_path)
print(topic_name)


credentials_location = os.getenv('gcp_credentials_path')

# Spark Config
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('TwitterSentimentAnalysis') \
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .set("spark.jars", "gs://path/to/spark-bigquery-latest.jar,gs://path/to/google-cloud-bigquery-latest.jar,/path/to/local-jar-file.jar") \
    .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)\
    .set("spark.jars", "gcs-connector-hadoop3-2.2.5.jar") 


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

schema = StructType([
    StructField("event_name", StringType()),
    StructField("event_type", StringType()),
    StructField("event_id", StringType()),
    StructField("event_url", StringType()),
    StructField("venue_name", StringType()),
    StructField("venue_id", StringType()),
    StructField("venue_zipcode", StringType()),
    StructField("venues_timezone", StringType()),
    StructField("venue_city", StringType()),
    StructField("venue_state_full", StringType()),
    StructField("venue_state_short", StringType()),
    StructField("venue_country_name", StringType()),
    StructField("venue_country_short", StringType()),
    StructField("venue_address", StringType()),
    StructField("venue_longitude", StringType()),
    StructField("venue_latitude", StringType()),
    StructField("attraction_name", StringType()),
    StructField("attraction_type", StringType()),
    StructField("attraction_id", StringType()),
    StructField("attraction_url", StringType()),
    StructField("attraction_segment_id", StringType()),
    StructField("attraction_segment_name", StringType()),
    StructField("attraction_genre_id", StringType()),
    StructField("attraction_genre_name", StringType()),
    StructField("attraction_subgenre_id", StringType()),
    StructField("attraction_subgenre_name", StringType()),
    StructField("event_start_date", StringType()),
    StructField("ticket_status", StringType()),
    StructField("event_start_time", StringType()),
    StructField("currency", StringType()),
    StructField("min_price", StringType()),
    StructField("max_price", StringType())
])


PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_BUCKET')

dataset = os.getenv('GCP_dataset')
table = os.getenv('GCP_table')

gcs_metadata_folder = os.getenv('GCP_metadata_bucket')
gcs_data_folder = os.getenv('GCP_data_bucket')

print(PROJECT_ID)
print(BUCKET)
print(dataset)
print(gcs_data_folder)


##### STREAMING DATA PROCESSING #####

# Read the data from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .option("header", "true") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
    
# Apply deserialization or further processing if needed
df1 = df.withColumn("parsed_data", from_json("value", schema))

### DATA TYPE CONVERSIONS ####
# Extract and convert the "venue_zipcode" as Integer
# df1 = df1.withColumn("venue_zipcode", col("parsed_data.venue_zipcode").cast(IntegerType()))
# # Extract and convert the coordinates as Double
# df1 = df1.withColumn("venue_longitude", col("parsed_data.venue_longitude").cast(DoubleType()))
# df1 = df1.withColumn("venue_latitude", col("parsed_data.venue_latitude").cast(DoubleType()))
# # Extract and Convert the event_start_date as Date 
# df1 = df1.withColumn("event_start_date", col("parsed_data.event_start_date").cast(DateType()))

df2 = df1.select("parsed_data.*")


df2.printSchema()

# path = 'DEFINE IF USING LOCAL FILE STREAMING WRITE'


# # # Write to a local file
# # file_query = df2.writeStream \
# #     .format("csv") \
# #     .outputMode("append") \
# #     .option("header", "true") \
# #     .option("checkpointLocation", path) \
# #     .trigger(processingTime="10 seconds") \
# #     .start(path)

# # # WRITE TO GCS BUCKET 
# gcs_write = df2.writeStream \
#     .format("csv") \
#     .outputMode("append") \
#     .option("path","gs://kafka-spark-data/raw-spark-data") \
#     .option("checkpointLocation", "gs://kafka-spark-data/spark-metadata") \
#     .trigger(processingTime="10 seconds") \
#     .start() 

# gcs_write.awaitTermination()

# WRITE TO CONSOLE TO LOG 
# console_query = df2.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .trigger(processingTime="10 seconds") \
#     .start() \
#     .awaitTermination()

    # .foreachBatch(write_batch) \

gcs_bigquery_stream = df2.writeStream \
    .format("bigquery") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", gcs_metadata_folder) \
    .option("temporaryGcsBucket", 'kafka-spark-data') \
    .option("table",'global-maxim-338114.twitter_kafka_pyspark_test.kafka_pyspark') \
    .option("mode", "FAILFAST") \
    .start()

    # .option("failOnDataLoss",'false') \

gcs_bigquery_stream.awaitTermination()