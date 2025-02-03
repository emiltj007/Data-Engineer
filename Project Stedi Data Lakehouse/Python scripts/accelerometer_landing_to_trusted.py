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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1726909186749 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1726909186749")

# Script generated for node customer_trusted
customer_trusted_node1726981470960 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="customer_trusted_node1726981470960")

# Script generated for node Accelerometer sanitize
SqlQuery4432 = '''
select distinct al.* from 
accelerometer_landing al
join customer_trusted ct
    on al.user=ct.email

'''
Accelerometersanitize_node1726975486133 = sparkSqlQuery(glueContext, query = SqlQuery4432, mapping = {"accelerometer_landing":Accelerometerlanding_node1726909186749, "customer_trusted":customer_trusted_node1726981470960}, transformation_ctx = "Accelerometersanitize_node1726975486133")

# Script generated for node accelerometer trusted
accelerometertrusted_node1726982175641 = glueContext.write_dynamic_frame.from_catalog(frame=Accelerometersanitize_node1726975486133, database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1726982175641")

job.commit()