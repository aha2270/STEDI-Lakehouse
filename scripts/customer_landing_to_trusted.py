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

# 1. Read directly from the Glue Catalog customer_landing table
customer_landing_dynamic = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_landing",
    transformation_ctx="customer_landing_dynamic"
)

# 2. Filter using PySpark DataFrame for precise null / zero checks on the normalized column
customer_df = customer_landing_dynamic.toDF()

# Handles both camelCase and lowercase column schemas safely
col_name = "sharewithresearchasofdate" if "sharewithresearchasofdate" in customer_df.columns else "shareWithResearchAsOfDate"

customer_trusted_df = customer_df.filter(
    customer_df[col_name].isNotNull() & 
    (customer_df[col_name] != 0)
)

# 3. Convert back to DynamicFrame
customer_trusted_dynamic = DynamicFrame.fromDF(
    customer_trusted_df, 
    glueContext, 
    "customer_trusted_dynamic"
)

# 4. Write output to S3 customer/trusted/
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted_dynamic,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-alexander-harvey-d609-project/customer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="customer_trusted_sink"
)

job.commit()