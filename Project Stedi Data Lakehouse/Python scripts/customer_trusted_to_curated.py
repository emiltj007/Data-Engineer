import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1726984159465 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="customer_trusted_node1726984159465")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1726984471995 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1726984471995")

# Script generated for node SQL Query
SqlQuery3861 = '''
select distinct ct.* 
from customer_trusted ct
join accelerometer_trusted at
on ct.email=at.user

'''
SQLQuery_node1726984164050 = sparkSqlQuery(glueContext, query = SqlQuery3861, mapping = {"customer_trusted":customer_trusted_node1726984159465, "accelerometer_trusted":accelerometer_trusted_node1726984471995}, transformation_ctx = "SQLQuery_node1726984164050")

# Script generated for node customer_curated
customer_curated_node1726986279726 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1726984164050, database="stedi-db", table_name="customer_curated", transformation_ctx="customer_curated_node1726986279726")

job.commit()