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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1726987348980 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1726987348980")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1726987350587 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1726987350587")

# Script generated for node customer_curated
customer_curated_node1726987351219 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_curated", transformation_ctx="customer_curated_node1726987351219")

# Script generated for node SQL Query
SqlQuery4386 = '''
select distinct stt.serialnumber,at.user,stt.sensorreadingtime,stt.distancefromobject,
at.x,at.y,at.z
from step_trainer_trusted stt
join customer_curated cc on stt.serialnumber=cc.serialnumber
join accelerometer_trusted at on cc.email=at.user
and at.timestamp=stt.sensorreadingtime
order by at.user,stt.sensorreadingtime

'''
SQLQuery_node1726987520564 = sparkSqlQuery(glueContext, query = SqlQuery4386, mapping = {"customer_curated":customer_curated_node1726987351219, "step_trainer_trusted":step_trainer_trusted_node1726987348980, "accelerometer_trusted":accelerometer_trusted_node1726987350587}, transformation_ctx = "SQLQuery_node1726987520564")

# Script generated for node machine_learning_curated
machine_learning_curated_node1726987548811 = glueContext.getSink(path="s3://stedi-human-balance/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1726987548811")
machine_learning_curated_node1726987548811.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1726987548811.setFormat("json")
machine_learning_curated_node1726987548811.writeFrame(SQLQuery_node1726987520564)
job.commit()