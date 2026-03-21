# This is the final PySpark job. It takes the 3 separate results files from CAR, LCR and NPL
# and joins them together into one complete table. It then loads everything into PostgreSQL
# so the dashboard and report generator can display the results.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder.appName("FDICBenchmarkJoin").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read all 3 Parquet files saved by the previous compute jobs
# Using relative Windows paths instead of Linux /opt/data/ paths
car = spark.read.parquet("data/processed/car_metrics").select("bank_id", "metric_date", "car")
lcr = spark.read.parquet("data/processed/lcr_metrics").select("bank_id", "metric_date", "lcr")
npl = spark.read.parquet("data/processed/npl_metrics").select("bank_id", "metric_date", "npl_ratio")

# Join all 3 results on bank_id AND metric_date
# We join on both columns so January data never mixes with February data
bank_metrics = car.join(lcr, ["bank_id", "metric_date"]).join(npl, ["bank_id", "metric_date"])

# Add benchmark values and PASS/FAIL status for each metric
result = bank_metrics \
    .withColumn("benchmark_car", lit(14.0)) \
    .withColumn("benchmark_lcr", lit(120.0)) \
    .withColumn("benchmark_npl", lit(1.5)) \
    .withColumn("car_delta", col("car") - lit(14.0)) \
    .withColumn("lcr_delta", col("lcr") - lit(120.0)) \
    .withColumn("npl_delta", col("npl_ratio") - lit(1.5)) \
    .withColumn("car_status", when(col("car") >= 14.0, "PASS").otherwise("FAIL")) \
    .withColumn("lcr_status", when(col("lcr") >= 120.0, "PASS").otherwise("FAIL")) \
    .withColumn("npl_status", when(col("npl_ratio") <= 1.5, "PASS").otherwise("FAIL"))

# Show results in console
result.show(truncate=False)

# Save to Parquet
result.write.mode("overwrite").parquet("data/processed/benchmark_comparison")

print("FDIC benchmark join complete")
spark.stop()
