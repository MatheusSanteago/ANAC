import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import time

spark = SparkSession.builder.config("spark.jars", "postgresql-42.6.0.jar") \
                            .master("local")\
                            .appName("ANAC")\
                            .config("spark.driver.memory", "8g")  \
                            .config("spark.executor.memory", "8g")   \
                            .appName("sampleCodeForReference")\
                            .getOrCreate()

url = "jdbc:postgresql://localhost:5432/dwarehouse"

properties = {
    "user": os.environ["DB_USER"],
    "password": os.environ["PASSWORD_DB"],
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=url, table="anac_warehouse", properties=properties)

df.createOrReplaceTempView("view_voo")

spark.sql("""
create temp view dim_companies as (
    with companies as (
        select distinct ICAO, EMPRESA 
            from view_voo
            order by ICAO
    ) select ROW_NUMBER () OVER (ORDER BY ICAO) id, * 
        from companies order by id
    )
""")

spark.sql("""
create temp view dim_airports as (
    with airports as (
            select distinct 
                ORIGEM as ICAO,
                CIDADE_ORIGEM as CIDADE, 
                PAIS_ORIGEM as PAIS 
            from view_voo
            union 
            select distinct 
                DESTINO as ICAO,
                CIDADE_DESTINO as CIDADE, 
                PAIS_DESTINO as PAIS
            from view_voo
    ) select distinct 
        ROW_NUMBER () OVER (ORDER BY ICAO) id, 
        * 
        from airports 
        order by id
    )
""")

spark.sql("""
create temp view fato_voos as (
 with fato as (
        select 
                ANO, 
                c.id as ID_EMPRESA, 
                a.id as ID_ORIGEM, 
                a2.id as ID_DESTINO, 
                CLASSE_IDA, 
                CLASSE_VOLTA, 
                TARIFA, 
                ASSENTOS,
                MES
        from view_voo vs
        join dim_companies as c on (vs.EMPRESA = c.EMPRESA)
        join dim_airports as a on (vs.ORIGEM = a.ICAO)
        join dim_airports as a2 on (vs.DESTINO = a2.ICAO)
        order by vs.MES
    ) select ROW_NUMBER () OVER (ORDER BY MES) id, * 
        from fato
        order by id asc)
""")

fato = spark.sql("SELECT * FROM fato_voos")
dim_c = spark.sql("SELECT * FROM dim_companies")
dim_a = spark.sql("SELECT * FROM dim_airports")

fato.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", url) \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "fato_voos") \
    .option("user", properties["user"])\
    .option("password", properties["password"]).save()

time.sleep(10)

dim_c.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", url) \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "dim_companies") \
    .option("user", properties["user"])\
    .option("password", properties["password"]).save()

time.sleep(10)

dim_a.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", url) \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "dim_airports") \
    .option("user", properties["user"])\
    .option("password", properties["password"]).save()


spark.stop()
