import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, month, year, to_date, count, round
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
leave_data_path = "s3://poc-bootcamp-capstone-project-group2/gold/employee_leave_data/"
quota_data_path = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_quota/"
output_path = "s3://poc-bootcamp-capstone-project-group2/gold/employees_80_percent_leave_used/"
txt_output_prefix = "s3://poc-bootcamp-capstone-project-group2/gold/leave_alerts/"

# Load and preprocess data
employee_leaves_df = spark.read.format("parquet").load(leave_data_path)
employee_leaves_df = employee_leaves_df.withColumn("date", to_date(col("date")))
employee_leaves_quota_df = spark.read.format("parquet").load(quota_data_path)

# Static month/year
#current_month = datetime.now().month
current_month = 12
current_year = 2024

# Filter and aggregate leave data
employee_confirmed_leaves_prev_month = employee_leaves_df \
    .where(
        (month(col("date")) < current_month) &
        (year(col("date")) == current_year) &
        (col("status") == "ACTIVE")
    ) \
    .groupBy("emp_id") \
    .agg(count("emp_id").alias("Total_leaves_till_prev_month"))

# Join with quota and calculate %
employee_leaves_byYear_df = employee_leaves_quota_df.where(col("year") == current_year)

employee_leave_quota_per = employee_confirmed_leaves_prev_month \
    .join(employee_leaves_byYear_df, "emp_id", "left") \
    .withColumn(
        "leaves_per",
        round((col("Total_leaves_till_prev_month") / col("leave_quota")) * 100, 2)
    ) \
    .select("emp_id", "leaves_per")

# Filter employees > 80%
employee_report = employee_leave_quota_per.where(col("leaves_per") > 80.0)

# Write Parquet output
employee_report.write.mode("overwrite").parquet(output_path)

# Collect result for .txt generation
over_limit_employees = employee_report.collect()

# Upload each employee’s alert to S3 as .txt
s3 = boto3.client('s3')
bucket = "poc-bootcamp-capstone-project-group2"
txt_prefix = "silver/employee_80%_txt_file//"

for row in over_limit_employees:
    emp_id = row["emp_id"]
    leaves_per = row["leaves_per"]
    file_content = (
        f"Employee ID: {emp_id}\n"
        f"You have used {leaves_per}% of your annual leave quota.\n"
        "Please plan accordingly.\n"
        "This is an automated alert.\n"
    )
    s3.put_object(
        Bucket=bucket,
        Key=f"{txt_prefix}{emp_id}.txt",
        Body=file_content.encode("utf-8")
    )

job.commit()
