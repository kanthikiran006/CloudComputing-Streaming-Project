from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, FloatType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("IoT Consumer FINAL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("raw", FloatType()) \
    .add("avg", FloatType()) \
    .add("status", StringType()) \
    .add("timestamp", IntegerType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.21.144.1:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ✅ 1. TERMINAL OUTPUT
console_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# ✅ 2. SAVE TO FILE
file_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/mnt/c/output/consumer") \
    .option("checkpointLocation", "/tmp/checkpoint_consumer") \
    .start()

spark.streams.awaitAnyTermination()