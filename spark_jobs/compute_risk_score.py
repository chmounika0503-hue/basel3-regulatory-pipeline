from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round as spark_round, least, greatest

# ─── Start Spark ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("BankRiskScore").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ─── Read benchmark comparison results ───────────────────────────────────────
# This file was created by fdic_benchmark_join.py
df = spark.read.parquet("data/processed/benchmark_comparison")

# ─── Benchmarks ──────────────────────────────────────────────────────────────
CAR_MIN  = 14.0   # Minimum CAR required
LCR_MIN  = 120.0  # Minimum LCR required
NPL_MAX  = 1.5    # Maximum NPL allowed

# ─── CAR Score (30% weight) ───────────────────────────────────────────────────
# If CAR >= 14% bank gets full 30 points
# If CAR is below 14% score drops proportionally
# Example: CAR = 7% (half of minimum) = 15 points out of 30
df = df.withColumn("car_score",
    spark_round(
        least(lit(30.0),
            greatest(lit(0.0),
                (col("car") / lit(CAR_MIN)) * lit(30.0)
            )
        ), 1
    )
)

# ─── LCR Score (40% weight) ───────────────────────────────────────────────────
# LCR gets the highest weight because cash shortage is most dangerous
# BankB at 72.7% gets: (72.7/120) * 40 = 24.2 points out of 40
df = df.withColumn("lcr_score",
    spark_round(
        least(lit(40.0),
            greatest(lit(0.0),
                (col("lcr") / lit(LCR_MIN)) * lit(40.0)
            )
        ), 1
    )
)

# ─── NPL Score (30% weight) ───────────────────────────────────────────────────
# NPL is inverse — lower is better
# If NPL <= 1.5% bank gets full 30 points
# If NPL = 3% (double the limit) bank gets 15 points out of 30
# If NPL >= 15% bank gets 0 points
df = df.withColumn("npl_score",
    spark_round(
        least(lit(30.0),
            greatest(lit(0.0),
                (lit(1.0) - (col("npl_ratio") - lit(NPL_MAX)) / lit(15.0)) * lit(30.0)
            )
        ), 1
    )
)

# ─── Total Risk Score ─────────────────────────────────────────────────────────
# Add all 3 scores together for total out of 100
df = df.withColumn("risk_score",
    spark_round(col("car_score") + col("lcr_score") + col("npl_score"), 1)
)

# ─── Risk Category ────────────────────────────────────────────────────────────
# Give each bank a label based on their total score
df = df.withColumn("risk_category",
    when(col("risk_score") >= 80, "SAFE")
    .when(col("risk_score") >= 60, "MODERATE")
    .when(col("risk_score") >= 40, "WARNING")
    .when(col("risk_score") >= 20, "DANGER")
    .otherwise("CRITICAL")
)

# ─── Regulatory Action ───────────────────────────────────────────────────────
# What action should regulators take based on score
df = df.withColumn("regulatory_action",
    when(col("risk_score") >= 80, "No action required")
    .when(col("risk_score") >= 60, "Monitor closely")
    .when(col("risk_score") >= 40, "Issue formal warning")
    .when(col("risk_score") >= 20, "Require improvement plan")
    .otherwise("Emergency intervention required")
)

# ─── Select final columns ─────────────────────────────────────────────────────
final = df.select(
    "bank_id",
    "metric_date",
    "car", "car_score",
    "lcr", "lcr_score",
    "npl_ratio", "npl_score",
    "risk_score",
    "risk_category",
    "regulatory_action"
)

# ─── Show results ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("BANK RISK SCORES — Basel III Compliance")
print("="*60)
final.show(truncate=False)

# ─── Save results ─────────────────────────────────────────────────────────────
final.write.mode("overwrite").parquet("data/processed/risk_scores")
print("Risk score computation complete!")
print("Results saved to data/processed/risk_scores")

spark.stop()
