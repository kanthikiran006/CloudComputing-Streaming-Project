from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    when,
    avg,
    sum as _sum,
    count
)
from pyspark.sql.types import *

# ============================================================
# 🚀 SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("BigData Advanced Analytics FINAL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 🚀 READ STREAM FROM KAFKA
# ============================================================

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs,transactions,reviews,social-media") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

# ============================================================
# 🚀 SCHEMA
# ============================================================

schema = StructType([
    StructField("type", StringType()),
    StructField("user_id", IntegerType()),
    StructField("timestamp", LongType()),
    StructField("amount", DoubleType()),
    StructField("fraud", BooleanType()),
    StructField("rating", IntegerType()),
    StructField("sentiment", StringType()),
    StructField("likes", IntegerType())
])

parsed = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ============================================================
# 🚀 EVENT TIME + WATERMARK
# ============================================================

parsed = parsed.withColumn(
    "event_time",
    col("timestamp").cast("timestamp")
).withWatermark("event_time", "10 seconds")

# ============================================================
# 🔥 1️⃣ FRAUD DETECTION
# ============================================================

fraud_scored = parsed.withColumn(
    "risk_score",
    when(col("amount") > 3000, 50).otherwise(0) +
    when(col("fraud") == True, 50).otherwise(0)
)

high_risk = fraud_scored.filter(col("risk_score") >= 50)

# ============================================================
# 🔥 2️⃣ REVENUE ANALYTICS
# ============================================================

revenue_df = parsed.filter(
    col("amount").isNotNull()
).groupBy(
    window(col("event_time"), "10 seconds")
).agg(
    _sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_transaction"),
    count("*").alias("transaction_count")
)

# ============================================================
# 🔥 3️⃣ ENGAGEMENT ANALYTICS
# ============================================================

engagement_df = parsed.filter(
    col("likes").isNotNull()
).groupBy(
    window(col("event_time"), "10 seconds")
).agg(
    _sum("likes").alias("total_likes"),
    avg("likes").alias("avg_likes")
)

# ============================================================
# 🔥 4️⃣ SENTIMENT ANALYTICS
# ============================================================

sentiment_df = parsed.filter(
    col("rating").isNotNull()
).groupBy(
    window(col("event_time"), "10 seconds")
).agg(
    avg("rating").alias("avg_rating"),
    count("*").alias("review_count")
)

# ============================================================
# 🖥️ CONSOLE OUTPUT
# ============================================================

high_risk_console = high_risk.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

revenue_console = revenue_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

engagement_console = engagement_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

sentiment_console = sentiment_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

# ============================================================
# 💾 PARQUET STORAGE
# ============================================================

high_risk_storage = high_risk.writeStream \
    .format("parquet") \
    .option(
        "path",
        "/mnt/c/projects/BigData-Streaming-Project/data/high_risk"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/high_risk_checkpoint"
    ) \
    .outputMode("append") \
    .start()

revenue_storage = revenue_df.writeStream \
    .format("parquet") \
    .option(
        "path",
        "/mnt/c/projects/BigData-Streaming-Project/data/revenue"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/revenue_checkpoint"
    ) \
    .outputMode("append") \
    .start()

engagement_storage = engagement_df.writeStream \
    .format("parquet") \
    .option(
        "path",
        "/mnt/c/projects/BigData-Streaming-Project/data/engagement"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/engagement_checkpoint"
    ) \
    .outputMode("append") \
    .start()

sentiment_storage = sentiment_df.writeStream \
    .format("parquet") \
    .option(
        "path",
        "/mnt/c/projects/BigData-Streaming-Project/data/sentiment"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/sentiment_checkpoint"
    ) \
    .outputMode("append") \
    .start()

# ============================================================
# 🚀 KEEP STREAM RUNNING
# ============================================================

spark.streams.awaitAnyTermination()
