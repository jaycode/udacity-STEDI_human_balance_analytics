import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1660848263152 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1660848263152",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1660969671672 = DynamicFrame.fromDF(
    CustomerTrustedZone_node1660848263152.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1660969671672",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=DropDuplicates_node1660969671672,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Filter By Consent Date
SqlQuery0 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
FilterByConsentDate_node1660849230571 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": JoinCustomer_node2},
    transformation_ctx="FilterByConsentDate_node1660849230571",
)

# Script generated for node Drop Fields
DropFields_node1660848581902 = DropFields.apply(
    frame=FilterByConsentDate_node1660849230571,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1660848581902",
)

# Script generated for node Customers Curated
CustomersCurated_node3 = glueContext.getSink(
    path="s3://jaycode-stedi-lakehouse/curated/customers/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCurated_node3",
)
CustomersCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
CustomersCurated_node3.setFormat("json")
CustomersCurated_node3.writeFrame(DropFields_node1660848581902)
job.commit()
