from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

spark = SparkSession.builder.appName("LCR").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn("lcr",
    round(col("liquid_assets") / col("cash_outflows_30d") * 100, 2))

df = df.withColumn("benchmark_lcr", lit(120.0))

df = df.withColumn("lcr_status",
    when(col("lcr") >= 120, "PASS").otherwise("FAIL"))

df.write.mode("overwrite").parquet("data/processed/lcr_metrics")
print("LCR computation complete")