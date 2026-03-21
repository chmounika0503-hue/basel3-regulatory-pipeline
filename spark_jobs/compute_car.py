
# Calculates the Capital Adequacy Ratio for each bank. CAR measures whether a bank has enough of its own capital to absorb losses if borrowers stop paying. A bank with insufficient CAR cannot survive a financial crisis.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

# We import col() to reference columns, when() for IF logic, lit() for fixed values and round() for decimal rounding 

spark = SparkSession.builder.appName("CAR").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")
#We import col(), when(), lit() and round() — the four functions needed for column references, IF logic, fixed values and decimal rounding

df = df.withColumn("car",
    round(col("tier1_capital") / col("risk_weighted_assets") * 100, 2))
#withColumn adds a new car column by dividing tier1_capital by risk_weighted_assets multiplied by 100 to get a percentage, rounded to 2 decimal places.


df = df.withColumn("benchmark_car", lit(14.0))
# withColumn calculates CAR by dividing tier1_capital by risk_weighted_assets times 100 and rounds the result to 2 decimal places.

df = df.withColumn("car_status",
    when(col("car") >= 14, "PASS").otherwise("FAIL"))

#when() checks if CAR is 14 or above and writes PASS otherwise FAIL — exactly like Excel IF formula but applies to all rows at once.

df.write.mode("overwrite").parquet("data/processed/car_metrics")
print("CAR computation complete")
#Save to a new Parquet file. We save to a separate file to keep the original data clean. The car_metrics file now has all original columns plus our 3 new columns: car, benchmark_car and car_status.