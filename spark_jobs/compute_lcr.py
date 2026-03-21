# A high NPL means the bank has too many bad loans.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

spark = SparkSession.builder.appName("LCR").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

#We start Spark and read the same balance_sheets Parquet file. This job runs in parallel with compute_car.py and compute_npl.py at the same time because all three read from the same input file independently.

df = df.withColumn("lcr",
    round(col("liquid_assets") / col("cash_outflows_30d") * 100, 2))
#LCR formula is liquid_assets divided by cash_outflows_30d multiplied by 100. liquid_assets is cash and assets that can be converted to cash within 1 day such as government bonds. cash_outflows_30d is all the money the bank must pay out in the next 30 days including customer withdrawals and loan payments. For BankB this is 800000 divided by 1100000 times 100 which equals 72.7%.

df = df.withColumn("benchmark_lcr", lit(120.0))

#We add the LCR benchmark of 120.0. The reason the threshold is 120% and not 100% is that having exactly 100% means the bank has just barely enough cash. Regulators want a 20% safety cushion above the minimum so that if something unexpected happens like a bank run the bank still has enough to survive.

df = df.withColumn("lcr_status",
    when(col("lcr") >= 120, "PASS").otherwise("FAIL"))

#PASS if LCR is 120 or above else FAIL. BankB gets FAIL with 72.7% and BankD gets FAIL with 55.6%. This means these two banks do not have enough cash to cover their 30 day obligations which is a serious regulatory violation.

df.write.mode("overwrite").parquet("data/processed/lcr_metrics")
print("LCR computation complete")