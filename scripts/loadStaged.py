import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%y-%d-%m %H:%M:%S',
    filename=os.path.join('../logs', 'carga_staged.log')
)

database = 'platform'
JAR_PATH = os.path.abspath("../jars/postgresql-42.7.5.jar")

spark = SparkSession.builder \
    .config("spark.jars", f"file:///{JAR_PATH.replace(os.sep, '/')}") \
    .master("local") \
    .appName("ANAC") \
    .getOrCreate()

logging.info(f"Iniciando extração")

emp_RDD = spark.sparkContext.emptyRDD()

NACIONAL_PATH = os.path.abspath("../Data/NACIONAL/")
INTERNACIONAL_PATH = os.path.abspath("../Data/Internacional")

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
                      .csv(os.path.join(NACIONAL_PATH, NACIONAL_DATA[file]))
                      )

for file in range(len(INTERNACIONAL_DATA)):
    df_i = df_i.union(spark.read.option("delimiter", ";")
                      .option("header", "true")
                      .csv(os.path.join(NACIONAL_PATH, NACIONAL_DATA[file]))
                      )

try:
    logging.info(f'Iniciando carga na tabela {database}.anac.voos_internacionais')
    df_i.write.format("jdbc")\
        .mode("overwrite")\
        .option("url", f"jdbc:postgresql://localhost:5432/{database}") \
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "voos_internacionais") \
        .option("user", os.environ["DB_USER"])\
        .option("password", os.environ["PASSWORD_DB"]).save()
except Exception as e:
    logging.error(f'Erro ao salvar na tabela anac.voos_internacionais {e}')


try:
    logging.info(f'Iniciando carga na tabela {database}.anac.voos_nacionais')
    df_n.write.format("jdbc")\
        .mode("overwrite")\
        .option("url", "jdbc:postgresql://localhost:5432/platform") \
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "anac.voos_nacionais") \
        .option("user", os.environ["DB_USER"])\
        .option("password", os.environ["PASSWORD_DB"]).save()
except Exception as e:
    logging.error(f'Erro ao salvar na tabela anac.voos_nacionais {e}')


spark.stop()
