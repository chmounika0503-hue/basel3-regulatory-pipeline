
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("NPL").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn(
    "npl_ratio",
    col("non_performing_loans") / col("total_loans")
)

df.write.mode("overwrite").parquet("data/processed/npl_metrics")
print("NPL computation complete")
