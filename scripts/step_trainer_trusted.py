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

# 1. Read step trainer landing and customer trusted from Catalog
step_trainer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node"
)

customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)

step_df = step_trainer_landing_node.toDF()
cust_df = customer_trusted_node.toDF()

# 2. Inner Join on serialnumber to retain readings only for consented customers
joined_df = step_df.join(
    cust_df,
    step_df["serialnumber"] == cust_df["serialnumber"],
    "inner"
)

# 3. Select original step trainer fields
step_trainer_trusted_df = joined_df.select(
    step_df["sensorreadingtime"],
    step_df["serialnumber"],
    step_df["distancefromobject"]
)

# 4. Sink to S3 trusted prefix
step_trainer_trusted_dynamic = DynamicFrame.fromDF(
    step_trainer_trusted_df,
    glueContext,
    "step_trainer_trusted_dynamic"
)

glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted_dynamic,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-alexander-harvey-d609-project/step_trainer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="step_trainer_trusted_sink"
)

job.commit()