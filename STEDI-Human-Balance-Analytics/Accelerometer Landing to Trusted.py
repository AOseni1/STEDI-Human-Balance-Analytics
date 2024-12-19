import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1734307395793, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734305407121", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734307470980 = glueContext.getSink(path="s3://aoo-stedi-human-balance/accelorometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734307470980")
AmazonS3_node1734307470980.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1734307470980.setFormat("json")
AmazonS3_node1734307470980.writeFrame(Join_node1734307395793)
job.commit()