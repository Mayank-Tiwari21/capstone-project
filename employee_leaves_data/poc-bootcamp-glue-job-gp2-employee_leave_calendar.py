import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, year, month

# Initialize Glue Context and Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("EmployeeLeaveTransformationJob", args={})

# Define the source and destination paths
bronze_bucket = "s3://poc-bootcamp-capstone-project-group2/bronze/employee_calendar/"
silver_bucket = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_calendar/"

# Define the schema explicitly
schema = StructType([
    StructField("reason", StringType(), True),
    StructField("date", TimestampType(), True)
])

# Read the data from the bronze bucket
employee_leave_df = spark.read.format("csv").schema(schema).load(bronze_bucket)

# Drop duplicate records
employee_leave_df = employee_leave_df.dropDuplicates()

# Add year column
transformed_df = employee_leave_df.withColumn("year", year(col("date")))

# Write to silver bucket in Parquet format, partitioned by year
transformed_df.write.mode("append").partitionBy("year").parquet(silver_bucket)

# Commit the job
job.commit()
