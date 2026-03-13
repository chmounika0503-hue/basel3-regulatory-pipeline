from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round as spark_round

spark = SparkSession.builder.appName("FDICBenchmarkJoin").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

car = spark.read.parquet("/opt/data/processed/car_metrics").select("bank_id","metric_date","car")
lcr = spark.read.parquet("/opt/data/processed/lcr_metrics").select("bank_id","metric_date","lcr")
npl = spark.read.parquet("/opt/data/processed/npl_metrics").select("bank_id","metric_date","npl_ratio")

bank_metrics = car.join(lcr, ["bank_id","metric_date"]).join(npl, ["bank_id","metric_date"])

bench_car, bench_lcr, bench_npl = 0.14, 1.20, 0.015

result = bank_metrics.withColumn("benchmark_car", lit(bench_car)).withColumn("benchmark_lcr", lit(bench_lcr)).withColumn("benchmark_npl", lit(bench_npl)).withColumn("car_delta", col("car") - lit(bench_car)).withColumn("lcr_delta", col("lcr") - lit(bench_lcr)).withColumn("npl_delta", col("npl_ratio") - lit(bench_npl)).withColumn("car_status", (col("car") >= 0.08).cast("string")).withColumn("lcr_status", (col("lcr") >= 1.0).cast("string"))

result.show(truncate=False)
result.write.parquet("/opt/data/processed/benchmark_comparison", mode="overwrite")
result.withColumnRenamed("npl_ratio","npl").write.format("jdbc").option("url","jdbc:postgresql://postgres:5432/basel3").option("dbtable","benchmark_comparison").option("user","bankuser").option("password","bankpass").option("driver","org.postgresql.Driver").mode("overwrite").save()
print("FDIC benchmark join complete")
spark.stop()
