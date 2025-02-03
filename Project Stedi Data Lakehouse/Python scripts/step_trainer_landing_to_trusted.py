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

# Script generated for node step_trainer_landing
step_trainer_landing_node1726986634259 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1726986634259")

# Script generated for node customer_curated
customer_curated_node1726986633558 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_curated", transformation_ctx="customer_curated_node1726986633558")

# Script generated for node SQL Query
SqlQuery4095 = '''
select distinct stl.* 
from step_trainer_landing stl
 join customer_curated cc
 on stl.serialnumber=cc.serialnumber

'''
SQLQuery_node1726986636602 = sparkSqlQuery(glueContext, query = SqlQuery4095, mapping = {"step_trainer_landing":step_trainer_landing_node1726986634259, "customer_curated":customer_curated_node1726986633558}, transformation_ctx = "SQLQuery_node1726986636602")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1726986639818 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1726986636602, database="stedi-db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1726986639818")

job.commit()