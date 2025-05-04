import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, from_unixtime, when, row_number, lit, lead
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bucket = "poc-bootcamp-capstone-project-group2"
input_path = f"s3://{bucket}/bronze/employee-timeframe-opt/"
output_path = f"s3://{bucket}/gold/employee-timeframe-opt/"

# PostgreSQL connection details
pg_url = "jdbc:postgresql://54.165.21.137:5432/capstone_project2"
pg_properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}
pg_table = "employee_timeframe_final_table"  

schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("start_date", LongType(), True),
    StructField("end_date", DoubleType(), True),
    StructField("salary", DoubleType(), True),
])

df = spark.read.option("header", "true").schema(schema).csv(input_path)
if df.rdd.isEmpty():
    raise Exception("No data found")

df = df.withColumn("start_date", to_date(from_unixtime(col("start_date").cast(DoubleType())))) \
       .withColumn("end_date", to_date(from_unixtime(col("end_date").cast(DoubleType()))))

w1 = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
df = df.withColumn("rn", row_number().over(w1)).filter(col("rn") == 1).drop("rn")

w2 = Window.partitionBy("emp_id").orderBy("start_date")
df = df.withColumn("next_start_date", lead("start_date").over(w2)) \
       .withColumn("end_date", when(col("end_date").isNull(), col("next_start_date")).otherwise(col("end_date"))) \
       .drop("next_start_date")

df = df.withColumn("status", when(col("end_date").isNull(), lit("ACTIVE")).otherwise(lit("INACTIVE")))

try:
    existing_df = spark.read.parquet(output_path)
except:
    existing_df = spark.createDataFrame([], df.schema)
    
# here we will separate the difference records from the new_data_df that comes daily so that we can union it with the existing df and there are no duplicates 
existing_keys = existing_df.select("emp_id", "start_date", "end_date" ,"salary").dropDuplicates()
new_df = df.alias("new").join(
    existing_keys.alias("exist"),
    on=["emp_id", "start_date", "end_date" ,"salary"],
    how="left_anti"
)

combined = existing_df.unionByName(new_df)

combined = combined.withColumn("next_start_date", lead("start_date").over(w2)) \
                   .withColumn("end_date", col("next_start_date")) \
                   .drop("next_start_date") \
                   .withColumn("status", when(col("end_date").isNull(), lit("ACTIVE")).otherwise(lit("INACTIVE")))

w3 = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
final_df = combined.withColumn("rn", row_number().over(w3)).filter(col("rn") == 1).drop("rn")

final_df.write.mode("overwrite").partitionBy("status").parquet(output_path)
# Write to PostgreSQL
final_df.write \
    .jdbc(pg_url, table=pg_table, mode="append", properties=pg_properties)
