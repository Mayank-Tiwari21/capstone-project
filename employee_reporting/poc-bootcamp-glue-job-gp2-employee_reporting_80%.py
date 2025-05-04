import sys
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

# Define S3 paths for input and output
leave_data_path = "s3://poc-bootcamp-capstone-project-group2/gold/employee_leave_data/"
quota_data_path = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_quota/"
output_path = "s3://poc-bootcamp-capstone-project-group2/gold/employees_80_percent_leave_used/"

# Load employee leave records and convert date column to proper format
employee_leaves_df = spark.read.format("parquet").load(leave_data_path)
employee_leaves_df = employee_leaves_df.withColumn("date", to_date(col("date")))

# Load employee leave quota data
employee_leaves_quota_df = spark.read.format("parquet").load(quota_data_path)

# Get the current month and year
# current_month = datetime.now().month
current_month = 12
current_year = 2024  # or use datetime.now().year for dynamic year

# Filter leave data to include only ACTIVE status records before the current month
employee_confirmed_leaves_prev_month = employee_leaves_df \
    .where(
        (month(col("date")) < current_month) &
        (year(col("date")) == current_year) &
        (col("status") == "ACTIVE")
    ) \
    .groupBy("emp_id") \
    .agg(count("emp_id").alias("Total_leaves_till_prev_month"))

# Filter leave quota records for the current year
employee_leaves_byYear_df = employee_leaves_quota_df \
    .where(col("year") == current_year)

# Join confirmed leave counts with leave quota and calculate percentage used
employee_leave_quota_per = employee_confirmed_leaves_prev_month \
    .join(employee_leaves_byYear_df, "emp_id", "left") \
    .withColumn(
        "leaves_per",
        round((col("Total_leaves_till_prev_month") / col("leave_quota")) * 100, 2)
    ) \
    .select("emp_id", "leaves_per")

# Filter employees who have used more than 80% of their quota
employee_report = employee_leave_quota_per \
    .where(col("leaves_per") > 80.0)

# Write the final result to the Gold layer in S3
employee_report.write.mode("overwrite").parquet(output_path)


job.commit()
