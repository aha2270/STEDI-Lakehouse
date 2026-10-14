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

# 1. Read step_trainer_trusted and accelerometer_trusted from Catalog
step_trainer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node"
)

accelerometer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node"
)

step_df = step_trainer_trusted_node.toDF()
accel_df = accelerometer_trusted_node.toDF()

# 2. Inner join on sensor reading time matching accelerometer timestamp
ml_df = step_df.join(
    accel_df,
    step_df["sensorreadingtime"] == accel_df["timestamp"],
    "inner"
)

# 3. Select all feature and label fields
# (sensorreadingtime, serialnumber, distancefromobject, user, timestamp, x, y, z)
ml_curated_df = ml_df.select(
    step_df["sensorreadingtime"],
    step_df["serialnumber"],
    step_df["distancefromobject"],
    accel_df["user"],
    accel_df["timestamp"],
    accel_df["x"],
    accel_df["y"],
    accel_df["z"]
)

# 4. Sink to S3 curated prefix
ml_curated_dynamic = DynamicFrame.fromDF(
    ml_curated_df,
    glueContext,
    "ml_curated_dynamic"
)

glueContext.write_dynamic_frame.from_options(
    frame=ml_curated_dynamic,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-alexander-harvey-d609-project/machine_learning/curated/",
        "partitionKeys": []
    },
    transformation_ctx="ml_curated_sink"
)

job.commit()