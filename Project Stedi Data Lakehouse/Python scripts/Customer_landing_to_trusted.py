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

# Script generated for node Customer_landing
Customer_landing_node1726907738028 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="Customer_landing_node1726907738028")

# Script generated for node Customer data sanitize
SqlQuery4305 = '''
select  distinct * from customer_landing cl
where cl.shareWithResearchAsOfDate is not null
'''
Customerdatasanitize_node1726907746109 = sparkSqlQuery(glueContext, query = SqlQuery4305, mapping = {"customer_landing":Customer_landing_node1726907738028}, transformation_ctx = "Customerdatasanitize_node1726907746109")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1726980346639 = glueContext.write_dynamic_frame.from_catalog(frame=Customerdatasanitize_node1726907746109, database="stedi-db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1726980346639")

job.commit()