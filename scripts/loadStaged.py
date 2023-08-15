import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

spark = SparkSession.builder.config("spark.jars", "postgresql-42.6.0.jar") \
    .master("local").appName("ANAC").getOrCreate()

emp_RDD = spark.sparkContext.emptyRDD()

NACIONAL_PATH = "Data/Nacional/"
INTERNACIONAL_PATH = "Data/Internacional"
NACIONAL_DATA = os.listdir(NACIONAL_PATH)
INTERNACIONAL_DATA = os.listdir(INTERNACIONAL_PATH)

schema_nacional = StructType([
    StructField("ANO", IntegerType(), True),
    StructField("MES", IntegerType(), True),
    StructField("EMPRESA", StringType(), True),
    StructField("ORIGEM", StringType(), True),
    StructField("DESTINO", StringType(), True),
    StructField("TARIFA", IntegerType(), True),
    StructField("ASSENTOS", IntegerType(), True)
])

schema_internacional = StructType([
    StructField("ANO", IntegerType(), True),
    StructField("MES", IntegerType(), True),
    StructField("EMPRESA", StringType(), True),
    StructField("ORIGEM", StringType(), True),
    StructField("DESTINO", StringType(), True),
    StructField("RETORNO", StringType(), True),
    StructField("CLASSE_IDA", StringType(), True),
    StructField("CLASSE_VOLTA", StringType(), True),
    StructField("TARIFA", IntegerType(), True),
    StructField("ASSENTOS", IntegerType(), True)
])

# Dataframe Nacional - Vazio
df_n = spark.createDataFrame(emp_RDD, schema_nacional)
# Dataframe Internacional - Vazio
df_i = spark.createDataFrame(emp_RDD, schema_internacional)

for file in range(len(NACIONAL_DATA)):
    df_n = df_n.union(spark.read.option("delimiter", ";")
                      .option("header", "true")
                      .csv(NACIONAL_PATH + "/" + NACIONAL_DATA[file]))

for file in range(len(INTERNACIONAL_DATA)):
    df_i = df_i.union(spark.read.option("delimiter", ";")
                      .option("header", "true")
                      .csv(INTERNACIONAL_PATH + "/" + INTERNACIONAL_DATA[file]))

df_i.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", "jdbc:postgresql://localhost:5432/staged") \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "internacional") \
    .option("user", os.environ["DB_USER"])\
    .option("password", os.environ["PASSWORD_DB"]).save()

df_n.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", "jdbc:postgresql://localhost:5432/staged") \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "nacional") \
    .option("user", os.environ["DB_USER"])\
    .option("password", os.environ["PASSWORD_DB"]).save()

spark.stop()
