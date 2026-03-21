from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IngestBalanceSheet").getOrCreate()

df = spark.read.csv(
    "data/raw_balance_sheets/balance_sheet.csv",  # <-- comma was missing here
    header=True,
    inferSchema=True
)

df.write.mode("overwrite").parquet("data/processed/balance_sheets")
print("Ingestion complete")