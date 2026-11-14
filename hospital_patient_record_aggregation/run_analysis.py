"""
Hospital Patient Record Aggregation - Big Data Analytics
Uses Apache Spark (PySpark) to aggregate a synthetic patient CSV dataset.
Only synthetic/demo data is included; do not use real patient-identifiable data.
"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, round as spark_round,
    year, month, when
)

ROOT = Path(__file__).resolve().parent
DATA_PATH = ROOT / "patient_records.csv"
OUTPUT_DIR = ROOT / "output"

def main():
    spark = (SparkSession.builder
             .appName("HospitalPatientRecordAggregation")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(str(DATA_PATH)))

    # Basic validation and derived fields
    df = (df.withColumn("admission_date", col("admission_date").cast("date"))
            .withColumn("discharge_date", col("discharge_date").cast("date"))
            .withColumn("age_group",
                when(col("age") < 18, "0-17")
                .when(col("age") < 36, "18-35")
                .when(col("age") < 56, "36-55")
                .when(col("age") < 76, "56-75")
                .otherwise("76+"))
            .withColumn("admission_year", year(col("admission_date")))
            .withColumn("admission_month", month(col("admission_date"))))

    print("\n=== SAMPLE PATIENT RECORDS ===")
    df.show(10, truncate=False)
    print(f"Total records: {df.count()}")

    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Patient count, average stay and total treatment cost by department
    by_department = (df.groupBy("department")
        .agg(count("*").alias("patient_count"),
             spark_round(avg("length_of_stay_days"), 2).alias("average_stay_days"),
             spark_round(spark_sum("treatment_cost_inr"), 2).alias("total_cost_inr"))
        .orderBy(col("patient_count").desc()))
    by_department.show(truncate=False)
    by_department.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_DIR / "by_department"))

    # 2. Patient count by diagnosis
    by_diagnosis = (df.groupBy("diagnosis")
        .agg(count("*").alias("patient_count"))
        .orderBy(col("patient_count").desc()))
    by_diagnosis.show(truncate=False)
    by_diagnosis.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_DIR / "by_diagnosis"))

    # 3. Patient count by age group and gender
    by_age_gender = (df.groupBy("age_group", "gender")
        .agg(count("*").alias("patient_count"))
        .orderBy("age_group", "gender"))
    by_age_gender.show(truncate=False)
    by_age_gender.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_DIR / "by_age_group_gender"))

    # 4. Monthly admissions aggregation
    by_month = (df.groupBy("admission_year", "admission_month")
        .agg(count("*").alias("admission_count"))
        .orderBy("admission_year", "admission_month"))
    by_month.show(24, truncate=False)
    by_month.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_DIR / "monthly_admissions"))

    print(f"\nAggregation outputs saved under: {OUTPUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()
