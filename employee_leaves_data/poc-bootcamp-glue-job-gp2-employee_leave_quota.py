import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
INPUT_PATH = "s3://poc-bootcamp-capstone-project-group2/bronze/employee_leave_quota_data/"
OUTPUT_PATH = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_quota/"

# Define the expected schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("leave_quota", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

# Read CSV directly with schema
df = spark.read.option("header", True).schema(schema).csv(INPUT_PATH)

# Drop duplicates
df = df.dropDuplicates(["emp_id", "leave_quota", "year"])

df = df.filter((col("emp_id").isNotNull()) & 
               (col("leave_quota").isNotNull()) & 
               (col("year").isNotNull()))


# Write to silver layer partitioned by year
df.write.partitionBy("year").mode("append").parquet(OUTPUT_PATH)

job.commit()
