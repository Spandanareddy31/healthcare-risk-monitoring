import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

RAW_DIR = os.path.join("data", "raw_stream")
OUT_DIR = os.path.join("data", "processed_parquet")
CHK_DIR = os.path.join("data", "checkpoint")

def main():
    spark = (
        SparkSession.builder
        .appName("HealthcareRiskMonitoring")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream.json(RAW_DIR)

    df = df.withColumn("event_time", to_timestamp(col("timestamp")))

    risk_score = (
        when((col("spo2") < 90) | (col("heart_rate") > 130) | (col("temp_c") >= 39.0), 2)
        .when((col("spo2") < 94) | (col("heart_rate") > 110) | (col("bp_systolic") > 160), 1)
        .otherwise(0)
    )

    df = df.withColumn("risk_score", risk_score)

    df = df.withColumn(
        "risk_label",
        when(col("risk_score") == 2, "HIGH")
        .when(col("risk_score") == 1, "MEDIUM")
        .otherwise("LOW")
    )

    query = (
        df.writeStream
        .format("parquet")
        .option("path", OUT_DIR)
        .option("checkpointLocation", CHK_DIR)
        .outputMode("append")
        .start()
    )

    print("Streaming job started...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
