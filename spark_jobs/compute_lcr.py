
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("LCR").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn(
    "lcr",
    col("liquid_assets") / col("cash_outflows_30d")
)

df.write.mode("overwrite").parquet("data/processed/lcr_metrics")
print("LCR computation complete")
