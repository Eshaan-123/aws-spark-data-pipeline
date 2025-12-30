import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([
    StructField("Bank Name", StringType(), True),
    StructField("Bank ID", StringType(), True),
    StructField("Account Number", StringType(), True),
    StructField("Entity ID", StringType(), True),
    StructField("Entity Name", StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(args['BRONZE_PATH'] + "*.csv")

df_clean = df.dropDuplicates()

df_clean = df_clean.toDF(
    "bank_name",
    "bank_id",
    "account_number",
    "entity_id",
    "entity_name"
)

df_clean.write \
    .mode("overwrite") \
    .parquet("s3://eshaan-spark-silver/aml/")

job.commit()
