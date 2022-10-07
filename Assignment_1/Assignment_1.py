import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "assignment 1", table_name = "assignment_1", transformation_ctx = "DataSource0")




Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("serial number", "long", "serial number", "long"), ("company name", "string", "company name", "string"), ("employee markme", "string", "employee markme", "string"), ("leave", "long", "leave", "int")], transformation_ctx = "Transform0")

final_df=Transform0.toDF().withColumn("Timestamp",F.current_timestamp())

df_with_year_and_month = (final_df
    .withColumn("year", F.year(F.col("Timestamp").cast("timestamp")))
    .withColumn("month", F.month(F.col("Timestamp").cast("timestamp"))))


df=DynamicFrame.fromDF(df_with_year_and_month,glueContext,"final_dynamic_frame")


DataSink0 = glueContext.write_dynamic_frame.from_options(frame =df , format_options = {"compression": "snappy"}, connection_type = "s3", format = "glueparquet", connection_options = {"path": "s3://testglue100/Output_files/Assignment_1/","partitionKeys":["year","month"]}, transformation_ctx = "DataSink0")




job.commit()