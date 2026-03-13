
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CAR").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn(
    "car",
    col("tier1_capital") / col("risk_weighted_assets")
)

df.write.mode("overwrite").parquet("data/processed/car_metrics")
print("CAR computation complete")
