import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, month, year, to_date, count, round, when, lit
from pyspark.sql import SparkSession

# ==== Job initialization ====
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==== S3 Paths ====
leave_data_path = "s3://poc-bootcamp-capstone-project-group2/gold/employee_leave_data/"
quota_data_path = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_quota/"
output_path = "s3://poc-bootcamp-capstone-project-group2/gold/employees_80_percent_leave_used/"
txt_output_prefix = "s3://poc-bootcamp-capstone-project-group2/silver/employee_80%_txt_file/"
alerted_emp_ids_parquet = "s3://poc-bootcamp-capstone-project-group2/silver/alerted_emp_ids/"

# ==== Date Info ====
current_month = 6
current_year = 2024

# ==== Read leave data ====
employee_leaves_df = spark.read.format("parquet").load(leave_data_path)
employee_leaves_df = employee_leaves_df.withColumn("date", to_date(col("date")))

# ==== Aggregate leave usage ====
employee_confirmed_leaves_prev_month = employee_leaves_df \
    .where(
        (month(col("date")) < current_month) &
        (year(col("date")) == current_year) &
        (col("status") == "ACTIVE")
    ) \
    .groupBy("emp_id") \
    .agg(count("emp_id").alias("Total_leaves_till_prev_month"))

# ==== Read quota data ====
employee_leaves_quota_df = spark.read.format("parquet").load(quota_data_path)
employee_leaves_byYear_df = employee_leaves_quota_df.where(col("year") == current_year)

# ==== Calculate % leaves used ====
employee_leave_quota_per = employee_confirmed_leaves_prev_month \
    .join(employee_leaves_byYear_df, "emp_id", "left") \
    .withColumn(
        "leaves_per",
        when(col("leave_quota").isNull(), lit(100.0)).otherwise(
            round((col("Total_leaves_till_prev_month") / col("leave_quota")) * 100, 2)
        )
    ) \
    .select("emp_id", "leaves_per")

# ==== Filter employees > 80% used ====
employee_report = employee_leave_quota_per.where(col("leaves_per") > 80.0)
employee_report.write.mode("overwrite").parquet(output_path)

# ==== Load alerted emp_ids from Parquet ====
try:
    existing_alerts_df = spark.read.format("parquet").load(alerted_emp_ids_parquet)
except:
    existing_alerts_df = spark.createDataFrame([], schema="emp_id string, year int")

# ==== Identify new alerts ====
employee_report_with_year = employee_report.withColumn("year", lit(current_year))
new_alerts_df = employee_report_with_year.join(
    existing_alerts_df,
    on=["emp_id", "year"],
    how="left_anti"
)

# ==== Send new alerts (empty txt files) ====
if new_alerts_df.count() > 0:
    s3 = boto3.client("s3")
    bucket = "poc-bootcamp-capstone-project-group2"

    for row in new_alerts_df.select("emp_id").collect():
        emp_id = row["emp_id"]
        s3.put_object(
            Bucket=bucket,
            Key=f"{txt_output_prefix}{emp_id}_{current_year}.txt",
            Body=b""
        )

    # ==== Append new alerted emp_ids ====
    updated_alerts_df = existing_alerts_df.union(new_alerts_df.select("emp_id", "year")).dropDuplicates(["emp_id", "year"])
    updated_alerts_df.write.mode("overwrite").parquet(alerted_emp_ids_parquet)

# ==== Job Completion ====
job.commit()
