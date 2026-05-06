from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, FloatType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("IoT Analytics FINAL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# SCHEMA
# =========================
schema = StructType() \
    .add("raw", FloatType()) \
    .add("avg", FloatType()) \
    .add("status", StringType()) \
    .add("timestamp", IntegerType())

# =========================
# READ FROM KAFKA
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.21.144.1:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# =========================
# 🚨 ALERTS (NO AGG → APPEND OK)
# =========================
alerts_df = parsed_df.filter(col("status") != "NORMAL")

alerts_console = alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("🚨 ALERTS") \
    .start()

alerts_file = alerts_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/mnt/c/output/alerts") \
    .option("checkpointLocation", "/tmp/checkpoint_alerts") \
    .start()

# =========================
# 📊 WINDOW TREND (FIXED WITH WATERMARK)
# =========================
window_df = parsed_df \
    .withColumn("time", (col("timestamp")/1000).cast("timestamp")) \
    .withWatermark("time", "10 seconds") \
    .groupBy(window(col("time"), "10 seconds")) \
    .agg(avg("raw").alias("window_avg"))

# console (complete allowed)
window_console = window_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("📊 WINDOW AVG") \
    .start()

# file (append allowed because watermark added)
window_file = window_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/mnt/c/output/window") \
    .option("checkpointLocation", "/tmp/checkpoint_window") \
    .start()

# =========================
# RUN STREAMS
# =========================
spark.streams.awaitAnyTermination()