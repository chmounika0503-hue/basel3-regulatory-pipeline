from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

spark = SparkSession.builder.appName("NPL").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn("npl_ratio",
    round(col("non_performing_loans") / col("total_loans") * 100, 2))

df = df.withColumn("benchmark_npl", lit(1.5))

df = df.withColumn("npl_status",
    when(col("npl_ratio") <= 1.5, "PASS").otherwise("FAIL"))

df.write.mode("overwrite").parquet("data/processed/npl_metrics")
print("NPL computation complete")