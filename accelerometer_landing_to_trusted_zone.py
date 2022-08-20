import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame


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

# Script generated for node Join Customer
AccelerometerLanding_node1DF = AccelerometerLanding_node1.toDF()
CustomerTrustedZone_node1660848263152DF = CustomerTrustedZone_node1660848263152.toDF()
JoinCustomer_node2 = DynamicFrame.fromDF(
    AccelerometerLanding_node1DF.join(
        CustomerTrustedZone_node1660848263152DF,
        (
            AccelerometerLanding_node1DF["user"]
            == CustomerTrustedZone_node1660848263152DF["email"]
        ),
        "left",
    ),
    glueContext,
    "JoinCustomer_node2",
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
    paths=[
        "email",
        "phone",
        "customername",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1660848581902",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.getSink(
    path="s3://jaycode-stedi-lakehouse/trusted/accelerometer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node3",
)
AccelerometerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node3.setFormat("json")
AccelerometerTrusted_node3.writeFrame(DropFields_node1660848581902)
job.commit()
