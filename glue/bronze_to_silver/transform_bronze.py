import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from utils.secrets import get_snowflake_secrets


# ------------------ READ ARGUMENTS ------------------
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'BRONZE_PATH', 'SILVER_PATH']
)

BRONZE_PATH = args['BRONZE_PATH']
SILVER_PATH = args['SILVER_PATH']

# ------------------ SPARK / GLUE INIT ------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------ SCHEMA ------------------
schema = StructType([
    StructField("Bank Name", StringType(), True),
    StructField("Bank ID", StringType(), True),
    StructField("Account Number", StringType(), True),
    StructField("Entity ID", StringType(), True),
    StructField("Entity Name", StringType(), True)
])

# ------------------ READ BRONZE ------------------
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .schema(schema) \
    .load(f"{BRONZE_PATH}/*.csv")

print("Bronze path:", BRONZE_PATH)
print("Silver path:", SILVER_PATH)

df.printSchema()
print("Row count:", df.count())

# ------------------ BASIC TRANSFORM ------------------
df_clean = df.dropDuplicates()

df_clean = df_clean.toDF(
    "bank_name",
    "bank_id",
    "account_number",
    "entity_id",
    "entity_name"
)

# ------------------ WRITE SILVER ------------------

secrets = get_snowflake_secrets("snowflake/credentials")

sf_options = {
    "sfURL": f"{secrets['sf_account']}.snowflakecomputing.com",
    "sfUser": secrets["sf_user"],
    "sfPassword": secrets["sf_password"],
    "sfDatabase": secrets["sf_database"],
    "sfSchema": secrets["sf_schema"],
    "sfWarehouse": secrets["sf_warehouse"],
    "sfRole": secrets["sf_role"]
}

df_clean.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "AML_SILVER") \
    .mode("overwrite") \
    .save()

job.commit()
