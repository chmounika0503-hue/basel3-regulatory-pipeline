
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

spark = SparkSession.builder.appName("CAR").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn("car",
    round(col("tier1_capital") / col("risk_weighted_assets") * 100, 2))

df = df.withColumn("benchmark_car", lit(14.0))

df = df.withColumn("car_status",
    when(col("car") >= 14, "PASS").otherwise("FAIL"))

df.write.mode("overwrite").parquet("data/processed/car_metrics")
print("CAR computation complete")