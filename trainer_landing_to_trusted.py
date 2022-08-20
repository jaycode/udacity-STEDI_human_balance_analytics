import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1660970187405 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1660970187405",
)

# Script generated for node Customers Curated
CustomersCurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="CustomersCurated_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1660975652522 = ApplyMapping.apply(
    frame=StepTrainerLanding_node1660970187405,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "`(st) serialnumber`", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1660975652522",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=CustomersCurated_node1,
    frame2=RenamedkeysforJoin_node1660975652522,
    keys1=["serialnumber"],
    keys2=["`(st) serialnumber`"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1660971274844 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "timestamp",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "lastupdatedate",
        "phone",
        "`(st) serialnumber`",
    ],
    transformation_ctx="DropFields_node1660971274844",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.getSink(
    path="s3://jaycode-stedi-lakehouse/trusted/step_trainer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node3",
)
StepTrainerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node3.setFormat("json")
StepTrainerTrusted_node3.writeFrame(DropFields_node1660971274844)
job.commit()
