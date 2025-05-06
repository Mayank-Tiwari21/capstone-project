import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



# ====================== Part 1: Active Employees by Designation ======================
time_df = spark.read.parquet('s3://poc-bootcamp-capstone-project-group2/gold/employee_timeframe_output/')

active_df = spark.read.parquet('s3://poc-bootcamp-capstone-project-group2/gold/employee_timeframe_output/status=ACTIVE/')

active_count_df = active_df.groupBy("designation").agg(count(col("emp_id")).alias("active_employees"))
designation_df = time_df.select("designation").distinct()

final_cnt_df = designation_df.join(active_count_df, on="designation", how="left").fillna({"active_employees": 0})

today_str = datetime.utcnow().strftime("%Y-%m-%d")
final_cnt_df.withColumn("run_date", lit(today_str)) \
    .write.mode('overwrite').partitionBy("run_date") \
    .parquet('s3://poc-bootcamp-capstone-project-group2/gold/active_emp_by_desg/')

# PostgreSQL Connection Settings
pg_host = "54.165.21.137"
pg_port = "5432"
pg_user = "postgres"
pg_password = "8308"
pg_dbname = "capstone_project2"
pg_staging_table = "active_emp_by_desg_staging"
pg_target_table = "active_emp_by_desg_final_table"

# JDBC URL for PostgreSQL
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_dbname}"

# JDBC properties
pg_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

deduped_df.select("emp_id", "date", "status").write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table=pg_staging_table, properties=pg_properties)

print("✅ Written deduped_df to staging table.")


# Load the data from the staging table
staging_df = spark.read.jdbc(url=jdbc_url, table=pg_staging_table, properties=pg_properties)

# Read the target table
target_df = spark.read.jdbc(url=jdbc_url, table=pg_target_table, properties=pg_properties)

# Perform UPSERT operation: Merge the staging and target dataframes
upsert_df = staging_df.alias("staging").join(
    target_df.alias("target"),
    (staging_df["designation"] == target_df["designation"]),
    how="outer"
).select(
    when(col("staging.active_employees").isNotNull(), col("staging.active_employees")).otherwise(col("target.active_employees")).alias("active_employees")
)
# Write back to PostgreSQL target table (upsert)
upsert_df.write.mode("overwrite").jdbc(url=jdbc_url, table=pg_target_table, properties=pg_properties)

print("\u2705 PostgreSQL UPSERT completed successfully.")

# Commit job
job.commit()
