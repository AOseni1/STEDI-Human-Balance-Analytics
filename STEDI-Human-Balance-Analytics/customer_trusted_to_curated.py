import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1734307348765 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1734307348765")

# Script generated for node Accelerometer Landing 
AccelerometerLanding_node1734307260867 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1734307260867")

# Script generated for node Join
Join_node1734307395793 = Join.apply(frame1=AccelerometerLanding_node1734307260867, frame2=CustomerTrusted_node1734307348765, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1734307395793")

# Script generated for node SQL Query
SqlQuery7171 = '''
select distinct customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate from myDataSource
'''
SQLQuery_node1734308312615 = sparkSqlQuery(glueContext, query = SqlQuery7171, mapping = {"myDataSource":Join_node1734307395793}, transformation_ctx = "SQLQuery_node1734308312615")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734308312615, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734305407121", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734308442801 = glueContext.getSink(path="s3://aoo-stedi-human-balance/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734308442801")
AmazonS3_node1734308442801.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1734308442801.setFormat("json")
AmazonS3_node1734308442801.writeFrame(SQLQuery_node1734308312615)
job.commit()