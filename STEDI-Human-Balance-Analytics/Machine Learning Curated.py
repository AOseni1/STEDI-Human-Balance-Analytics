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

# Script generated for node acceleromter trusted
acceleromtertrusted_node1734309369386 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acceleromtertrusted_node1734309369386")

# Script generated for node step trainer trusted
steptrainertrusted_node1734309320655 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1734309320655")

# Script generated for node SQL Query
SqlQuery7273 = '''
select s.serialnumber, s.distancefromobject, a.x, a.y, a.z, a.timestamp from s
join a on s.sensorreadingtime = a.timestamp

'''
SQLQuery_node1734311481384 = sparkSqlQuery(glueContext, query = SqlQuery7273, mapping = {"a":acceleromtertrusted_node1734309369386, "s":steptrainertrusted_node1734309320655}, transformation_ctx = "SQLQuery_node1734311481384")

# Script generated for node machine learning curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734311481384, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734305407121", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearningcurated_node1734309429623 = glueContext.getSink(path="s3://aoo-stedi-human-balance/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1734309429623")
machinelearningcurated_node1734309429623.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machinelearningcurated_node1734309429623.setFormat("json")
machinelearningcurated_node1734309429623.writeFrame(SQLQuery_node1734311481384)
job.commit()