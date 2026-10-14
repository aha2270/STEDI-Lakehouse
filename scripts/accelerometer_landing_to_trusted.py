import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Read accelerometer landing and customer trusted from Catalog
accelerometer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node"
)

customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)

# 2. Convert to DataFrames for join
accel_df = accelerometer_landing_node.toDF()
cust_df = customer_trusted_node.toDF()

# 3. Inner Join on user email to keep only readings from research-consenting users
joined_df = accel_df.join(
    cust_df,
    accel_df["user"] == cust_df["email"],
    "inner"
)

# 4. Select only the original accelerometer fields
accelerometer_trusted_df = joined_df.select(
    accel_df["user"],
    accel_df["timestamp"],
    accel_df["x"],
    accel_df["y"],
    accel_df["z"]
)

# 5. Convert back to DynamicFrame and sink to S3
accelerometer_trusted_dynamic = DynamicFrame.fromDF(
    accelerometer_trusted_df,
    glueContext,
    "accelerometer_trusted_dynamic"
)

glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted_dynamic,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-alexander-harvey-d609-project/accelerometer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="accelerometer_trusted_sink"
)

job.commit()