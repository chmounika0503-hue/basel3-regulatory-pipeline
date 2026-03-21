# A high NPL means the bank has too many bad loans.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

spark = SparkSession.builder.appName("NPL").getOrCreate()

df = spark.read.parquet("data/processed/balance_sheets")

df = df.withColumn("npl_ratio",
    round(col("non_performing_loans") / col("total_loans") * 100, 2))

# NPL formula is non_performing_loans divided by total_loans multiplied by 100. non_performing_loans is the total amount of loans where borrowers have not made any payment for 90 days or more. For BankD this is 400000 divided by 2000000 times 100 which equals 20.0%. This means 1 in every 5 loans at BankD is not being repaid. That is catastrophic.

df = df.withColumn("benchmark_npl", lit(1.5))

# We add the NPL benchmark of 1.5%. This means only 1.5 out of every 100 loans can be non-performing. All 4 of our banks exceed this limit

df = df.withColumn("npl_status",
    when(col("npl_ratio") <= 1.5, "PASS").otherwise("FAIL"))

# CRITICAL DIFFERENCE from CAR and LCR. We use <= not >= because NPL is an inverse metric. Lower is better. For CAR and LCR higher is better so we used >=. For NPL lower is better so we use <=. If NPL is 1.5% or less the bank PASSES. If NPL is more than 1.5% the bank FAILS.

df.write.mode("overwrite").parquet("data/processed/npl_metrics")
print("NPL computation complete")