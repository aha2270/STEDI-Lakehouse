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

# 1. Read customer_trusted and accelerometer_trusted from Catalog
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)

accelerometer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node"
)

cust_df = customer_trusted_node.toDF()
accel_df = accelerometer_trusted_node.toDF()

# 2. Inner join customer_trusted with accelerometer_trusted on email/user
# to keep only customers with accelerometer readings
joined_df = cust_df.join(
    accel_df,
    cust_df["email"] == accel_df["user"],
    "inner"
)

# 3. Retain customer fields and deduplicate on email
customer_curated_df = joined_df.select(
    cust_df["customername"],
    cust_df["email"],
    cust_df["phone"],
    cust_df["birthday"],
    cust_df["serialnumber"],
    cust_df["registrationdate"],
    cust_df["lastupdatedate"],
    cust_df["sharewithresearchasofdate"],
    cust_df["sharewithpublicasofdate"],
    cust_df["sharewithfriendsasofdate"]
).dropDuplicates(["email"])

# 4. Convert back to DynamicFrame and sink to S3
customer_curated_dynamic = DynamicFrame.fromDF(
    customer_curated_df,
    glueContext,
    "customer_curated_dynamic"
)

glueContext.write_dynamic_frame.from_options(
    frame=customer_curated_dynamic,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-alexander-harvey-d609-project/customer/curated/",
        "partitionKeys": []
    },
    transformation_ctx="customer_curated_sink"
)

job.commit()