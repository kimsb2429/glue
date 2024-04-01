import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from py4j.java_gateway import java_import

# set up env
args = getResolvedOptions(sys.argv, ["JOB_NAME","pg-cluster","pg-db","pg-schema"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# global vars
pg_cluster = args["pg_cluster"]
pg_db = args["pg_db"]
pg_schema = args["pg_schema"]

# get postgres conn
source_jdbc_conf = glueContext.extract_jdbc_conf(pg_cluster)

java_import(sc._gateway.jvm,"java.sql.Connection")
java_import(sc._gateway.jvm,"java.sql.DatabaseMetaData")
java_import(sc._gateway.jvm,"java.sql.DriverManager")
java_import(sc._gateway.jvm,"java.sql.SQLException")

conn = sc._gateway.jvm.DriverManager.getConnection(source_jdbc_conf.get('url')+"/"+pg_db, source_jdbc_conf.get('user'), source_jdbc_conf.get('password'))

print(conn.getMetaData().getDatabaseProductName())


procedure_sql= '''
'''

# call stored procedure
cstmt = conn.prepareStatement(procedure_sql);
results = cstmt.execute();

# call stored procedure
cstmt = conn.prepareCall("{call "+ pg_schema + ".some_proc()}");
results = cstmt.execute();

conn.close()

# commit job for the table
job.commit()