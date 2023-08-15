import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
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
    StructField("CLASSE IDA", StringType(), True),
    StructField("CLASSE VOLTA", StringType(), True),
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

estados = {
    "SBGR": "São Paulo",
    "SBRJ": "Rio de Janeiro",
    "SBSP": "São Paulo",
    "SBBR": "Distrito Federal",
    "SBCF": "Minas Gerais",
    "SBPA": "Rio Grande do Sul",
    "SBCT": "Paraná",
    "SBRF": "Pernambuco",
    "SBSV": "Bahia",
    "SBKP": "São Paulo",
    "SBFZ": "Ceará",
    "SBGO": "Goiás",
    "SBBE": "Pará",
    "SBEG": "Amazonas",
    "SBCY": "Mato Grosso do Sul",
    "SBFL": "Santa Catarina",
    "SBVT": "Espírito Santo",
    "SBGL": "Rio de Janeiro",
    "SBSL": "Maranhão",
    "SBCG": "Mato Grosso do Sul",
    "SBNF": "Rio de Janeiro",
    "SBFI": "Santa Catarina",
    "SBMO": "Alagoas",
    "SBSG": "Sergipe",
    "SBJP": "Paraíba",
    "SBTE": "São Paulo",
    "SBPS": "Rio Grande do Sul",
    "SBAR": "Pernambuco",
    "SBUL": "Goiás",
    "SBPJ": "Paraná",
    "SBPV": "Paraná",
    "SBRP": "São Paulo",
    "SBSR": "Santa Catarina",
    "SBLO": "São Paulo",
    "SBMG": "Minas Gerais",
    "SBMQ": "Pará",
    "SBCH": "Santa Catarina",
    "SBMA": "Maranhão",
    "SBBV": "Roraima",
    "SBIL": "Rio Grande do Norte",
    "SWSI": "São Paulo",
    "SBSN": "Rio Grande do Norte",
    "SBCA": "São Paulo",
    "SBJV": "Rio de Janeiro",
    "SBRB": "Rio Grande do Sul",
    "SBPL": "Pará",
    "SBJU": "Ceará",
    "SBIZ": "Amazonas",
    "SBMK": "Mato Grosso",
    "SBPF": "Rio Grande do Sul",
    "SBCX": "Mato Grosso",
    "SBDN": "Bahia",
    "SBIP": "Pará",
    "SBQV": "Piauí",
    "SBFN": "Rio Grande do Norte",
    "SBZM": "Paraíba",
    "SBKG": "Rio Grande do Norte",
    "SBJE": "Espírito Santo",
    "SBPP": "Pará",
    "SBHT": "Minas Gerais",
    "SBAU": "São Paulo",
    "SNBR": "Pará",
    "SBGV": "São Paulo",
    "SBSO": "Rio de Janeiro",
    "SBCJ": "São Paulo",
    "SBJA": "Bahia",
    "SBUR": "Paraná",
    "SBAT": "Santa Catarina",
    "SBPK": "Parana",
    "SBSI": "Pará",
    "SBVC": "Espírito Santo",
    "SBNM": "Pará",
    "SBAE": "São Paulo",
    "SBTT": "Pará",
    "SBVH": "Goiás",
    "SBSM": "Rio Grande do Sul",
    "SSGG": "Rio Grande do Sul",
    "SBJI": "São Paulo",
    "SBML": "Minas Gerais",
    "SSKW": "São Paulo",
    "SBRD": "Distrito Federal",
    "SNGI": "Amazonas",
    "SBTG": "São Paulo",
    "SBMS": "Mato Grosso do Sul",
    "SBPO": "São Paulo",
    "SBPG": "Rio Grande do Sul",
    "SWLC": "Santa Catarina",
    "SNCP": "Amazonas",
    "SBTF": "Paraná",
    "SBUG": "Minas Gerais",
    "SBCB": "Paraíba",
    "SBCZ": "Ceará",
    "SBME": "Bahia",
    "SBCR": "São Paulo",
    "SBPB": "Paraíba",
    "SBCP": "São Paulo",
    "SBTD": "São Paulo",
    "SBLE": "Espírito Santo",
    "SBUF": "Bahia",
    "SBIH": "Mato Grosso",
    "SBUA": "Amazonas",
    "SBDB": "Distrito Federal",
    "SNJD": "Amazonas",
    "SBCN": "Rio Grande do Norte",
    "SWPI": "São Paulo",
    "SBTB": "Bahia",
    "SWGN": "Goiás",
    "SWKQ": "Goiás",
    "SWEI": "São Paulo",
    "SBJR": "Rio de Janeiro",
    "SNTF": "Pará",
    "SBVG": "Goiás",
    "SWCA": "Santa Catarina",
    "SBMY": "Bahia",
    "SWKO": "Goiás",
    "SNTO": "Amazonas",
    "SBBG": "Santa Catarina",
    "SBTC": "Mato Grosso",
    "SNEB": "Pará",
    "SNPD": "Pará",
    "SBBW": "Rio Grande do Sul",
    "SNTS": "Pará",
    "SSUM": "Rio Grande do Sul",
    "SWYN": "Santa Catarina",
    "SBMD": "Mato Grosso",
    "SNVS": "Pará",
    "SNZR": "Pará",
    "SWBE": "Bahia",
    "SSOU": "São Paulo",
    "SSRS": "Rio Grande do Sul",
    "SWBC": "Bahia",
    "SWHP": "Santa Catarina",
    "SBTU": "São Paulo",
    "SNHS": "Pará",
    "SSLT": "Pará",
    "SWTP": "Santa Catarina",
    "SSZR": "Rio Grande do Sul",
    "SNOB": "Bahia",
    "SWMW": "Mato Grosso",
    "SNAB": "Pará",
    "SNRU": "Pará",
    "SNWS": "Pará",
    "SNMZ": "Pará",
    "SWBR": "Bahia",
    "SSCN": "Rio Grande do Sul",
    "SNCL": "Pará",
    "SSGY": "Rio Grande do Sul",
    "SNIG": "Amazonas",
    "SBJD": "São Paulo",
    "SSVL": "Rio Grande do Sul",
    "SWLB": "Bahia",
    "SNGN": "Amazonas",
    "SBAX": "Minas Gerais",
    "SIRI": "Bahia",
    "SNYA": "Pará",
    "SSSC": "Rio Grande do Sul",
    "SSUV": "Paraná",
    "SNSM": "Pará",
    "SBAC": "Ceará",
    "SWJN": "Mato Grosso",
    "SNMA": "Pará",
    "SDCG": "Amazonas",
    "SNOX": "Pará",
    "SNTI": "Pará",
    "SBFE": "Bahia",
    "SNVB": "Valença"
}
empresas = {
    'ABJ': 'Aerotáxi Abaeté',
    'PTB': 'Passaredo',
    'AAL': 'American Airlines',
    'ACA': 'Air Canada',
    'AEA': 'European Regions Airlines Association',
    'AFR': 'Air France',
    'AMX': 'Aeroméxico',
    'ARG': 'Aerolineas Argentinas',
    'AVA': 'Avianca',
    'AZU': 'Azul',
    'BAW': 'British Airways',
    'BOV': 'Batavia Air',
    'CMP': 'Copa Airlines',
    'DAL': 'Delta Air Lines',
    'DLH': 'Lufthansa',
    'DTA': 'Delta Connection',
    'ETH': 'Ethiopian Airlines',
    'FBZ': 'Flightline',
    'GLO': 'Gol Transportes Aéreos',
    'IBE': 'Iberia',
    'ITY': 'Air Italy',
    'JAT': 'Jat Airways',
    'JES': 'Jes Air',
    'KLM': 'KLM Royal Dutch Airlines',
    'QTR': 'Qatar Airways',
    'SKU': 'Sky Airline',
    'SWR': 'Swiss International Air Lines',
    'TAM': 'LATAM Brasil',
    'TAP': 'TAP Air Portugal',
    'THY': 'Turkish Airlines',
    'UAE': 'Emirates',
    'UAL': 'United Airlines',
    'VVC': 'Aero VIP'
}
paisesDestinos = {
    "CYUL": "Canadá",
    "CYYZ": "Canadá",
    "DTTA": "Tunísia",
    "EDDF": "Alemanha",
    "EDDM": "Alemanha",
    "EGLL": "Reino Unido",
    "EHAM": "Países Baixos",
    "FAOR": "África do Sul",
    "FNLU": "Angola",
    "HAAB": "Etiópia",
    "KATL": "Estados Unidos",
    "KBOS": "Estados Unidos",
    "KDFW": "Estados Unidos",
    "KEWR": "Estados Unidos",
    "KFLL": "Estados Unidos",
    "KIAD": "Estados Unidos",
    "KIAH": "Estados Unidos",
    "KJFK": "Estados Unidos",
    "KLAX": "Estados Unidos",
    "KMCO": "Estados Unidos",
    "KMIA": "Estados Unidos",
    "KORD": "Estados Unidos",
    "LEBL": "Espanha",
    "LEMD": "Espanha",
    "LFMN": "França",
    "LFPG": "França",
    "LFPO": "França",
    "LGAV": "Grécia",
    "LIBD": "Itália",
    "LIBR": "Itália",
    "LICA": "Itália",
    "LICC": "Itália",
    "LICJ": "Itália",
    "LICR": "Itália",
    "LIMC": "Itália",
    "LIMF": "Itália",
    "LIMJ": "Itália",
    "LIPE": "Itália",
    "LIPQ": "Itália",
    "LIPZ": "Itália",
    "LIRF": "Itália",
    "LIRN": "Itália",
    "LIRQ": "Itália",
    "LPPR": "Portugal",
    "LPPT": "Portugal",
    "LSZH": "Suíça",
    "LTFM": "Turquia",
    "MDPC": "República Dominicana",
    "MMMX": "México",
    "MMUN": "México",
    "MPTO": "Panamá",
    "OMDB": "Emirados Árabes Unidos",
    "OTHH": "Catar",
    "SAAR": "Argentina",
    "SABE": "Argentina",
    "SACO": "Argentina",
    "SAEZ": "Argentina",
    "SAME": "Argentina",
    "SCEL": "Chile",
    "SGAS": "Paraguai",
    "SKBO": "Colômbia",
    "SKBQ": "Colômbia",
    "SKCG": "Colômbia",
    "SKCL": "Colômbia",
    "SKRG": "Colômbia",
    "SKSM": "Colômbia",
    "SKSP": "Colômbia",
    "SLCB": "Bolívia",
    "SLVR": "Bolívia",
    "SMJP": "Suriname",
    "SOCA": "Guiana Francesa",
    "SPJC": "Peru",
    "SULS": "Uruguai",
    "SUMU": "Uruguai",
    "TNCC": "Curaçao"
}
cidadesDestinos = {
    "CYUL": "Montreal",
    "CYYZ": "Toronto",
    "DTTA": "Túnis",
    "EDDF": "Frankfurt",
    "EDDM": "Munique",
    "EGLL": "Londres",
    "EHAM": "Amsterdã",
    "FAOR": "Joanesburgo",
    "FNLU": "Luanda",
    "HAAB": "Adis Abeba",
    "KATL": "Atlanta",
    "KBOS": "Boston",
    "KDFW": "Dallas",
    "KEWR": "Nova Jersey",
    "KFLL": "Fort Lauderdale",
    "KIAD": "Washington, D.C.",
    "KIAH": "Houston",
    "KJFK": "Nova York",
    "KLAX": "Los Angeles",
    "KMCO": "Orlando",
    "KMIA": "Miami",
    "KORD": "Chicago",
    "LEBL": "Barcelona",
    "LEMD": "Madri",
    "LFMN": "Nice",
    "LFPG": "Paris",
    "LFPO": "Paris",
    "LGAV": "Atenas",
    "LIBD": "Bari",
    "LIBR": "Brindisi",
    "LICA": "Lamezia Terme",
    "LICC": "Catânia",
    "LICJ": "Palermo",
    "LICR": "Comiso",
    "LIMC": "Milão",
    "LIMF": "Turim",
    "LIMJ": "Gênova",
    "LIPE": "Bolonha",
    "LIPQ": "Trieste",
    "LIPZ": "Veneza",
    "LIRF": "Roma",
    "LIRN": "Nápoles",
    "LIRQ": "Florença",
    "LPPR": "Porto",
    "LPPT": "Lisboa",
    "LSZH": "Zurique",
    "LTFM": "Istambul",
    "MDPC": "Punta Cana",
    "MMMX": "Cidade do México",
    "MMUN": "Cancún",
    "MPTO": "Cidade do Panamá",
    "OMDB": "Dubai",
    "OTHH": "Doha",
    "SAAR": "Rosário",
    "SABE": "Buenos Aires",
    "SACO": "Córdoba",
    "SAEZ": "Buenos Aires",
    "SAME": "Mendoza",
    "SCEL": "Santiago",
    "SGAS": "Assunção",
    "SKBO": "Bogotá",
    "SKBQ": "Barranquilla",
    "SKCG": "Cartagena",
    "SKCL": "Cali",
    "SKRG": "Medellín",
    "SKSM": "Santa Marta",
    "SKSP": "San Andrés",
    "SLCB": "Cochabamba",
    "SLVR": "Santa Cruz",
    "SMJP": "Paramaribo",
    "SOCA": "Caiena",
    "SPJC": "Lima",
    "SULS": "Montevidéu",
    "SUMU": "Montevidéu",
    "TNCC": "Willemstad"
}

setEstado = F.udf(lambda row: estados.get(row, row))
setEmpresa = F.udf(lambda row: empresas.get(row, row))
setCidadeDestino = F.udf(lambda row: cidadesDestinos.get(row, row))
setPaisDestino = F.udf(lambda row: paisesDestinos.get(row, row))

df_n = df_n.withColumn("TARIFA", F.regexp_replace(F.col("TARIFA"), ",00", ""))
df_n = df_n.withColumn("ICAO", df_n["EMPRESA"])
df_n = df_n.withColumn("ESTADO_DESTINO", df_n["DESTINO"])
df_n = df_n.withColumn("ESTADO_ORIGEM", df_n["ORIGEM"])
df_n = df_n.withColumn("EMPRESA", setEmpresa(F.col("EMPRESA")))
df_n = df_n.select("ANO", "MES", "ICAO", "EMPRESA",
                   "ORIGEM", "ESTADO_ORIGEM", "DESTINO",
                   "ESTADO_DESTINO", "TARIFA", "ASSENTOS")

df_n = df_n.withColumn("CIDADE_ORIGEM", setEstado(F.col("ESTADO_ORIGEM")))
df_n = df_n.withColumn("CIDADE_DESTINO", setEstado(F.col("ESTADO_DESTINO")))

df_n = df_n.withColumn("PAIS_ORIGEM", F.lit("Brasil"))
df_n = df_n.withColumn("PAIS_DESTINO", F.lit("Brasil"))
df_n = df_n.withColumn("CLASSE IDA", F.lit("NA"))
df_n = df_n.withColumn("CLASSE VOLTA", F.lit("NA"))

df_n = df_n.select("ANO", "MES", "ICAO", "EMPRESA", "CLASSE IDA",
                   "CLASSE VOLTA", "ORIGEM", "PAIS_ORIGEM",
                   "CIDADE_ORIGEM", "DESTINO", "PAIS_DESTINO",
                   "CIDADE_DESTINO", "TARIFA", "ASSENTOS")

df_i = df_i.withColumn("TARIFA", F.regexp_replace(
    "TARIFA", ",", ".").cast("double"))
df_i = df_i.withColumn("ICAO", df_i["EMPRESA"])
df_i = df_i.withColumn("PAIS_DESTINO", df_i["DESTINO"])
df_i = df_i.withColumn("PAIS_ORIGEM", F.lit("Brasil"))
df_i = df_i.withColumn("CIDADE_DESTINO", df_i["DESTINO"])
df_i = df_i.withColumn("CIDADE_ORIGEM", df_i["ORIGEM"])

df_i = df_i.select("ANO", "MES", "ICAO", "EMPRESA", "CLASSE IDA",
                   "CLASSE VOLTA", "ORIGEM", "PAIS_ORIGEM",
                   "CIDADE_ORIGEM", "DESTINO", "PAIS_DESTINO",
                   "CIDADE_DESTINO", "TARIFA", "ASSENTOS")

df_i = df_i.withColumn("EMPRESA", setEmpresa(F.col("EMPRESA")))

df_i = df_i.withColumn("PAIS_DESTINO", setPaisDestino(F.col("PAIS_DESTINO")))

df_i = df_i.withColumn(
    "CIDADE_DESTINO", setCidadeDestino(F.col("CIDADE_DESTINO")))

df = df_n.union(df_i)

df = df.withColumn("CLASSE VOLTA", F.when(
    F.col("CLASSE VOLTA") == "NA", None).otherwise(F.col("CLASSE VOLTA")))

df = df.withColumn("CLASSE IDA", F.when(
    F.col("CLASSE IDA") == "NA", None).otherwise(F.col("CLASSE IDA")))

winSpec = Window.partitionBy("ANO").orderBy("MES")
df = df.withColumn("ID", F.row_number().over(winSpec))

df = df.select("ID", "ANO", "MES", "ICAO", "EMPRESA", "CLASSE IDA",
               "CLASSE VOLTA", "ORIGEM", "PAIS_ORIGEM",
               "CIDADE_ORIGEM", "DESTINO", "PAIS_DESTINO",
               "CIDADE_DESTINO", "TARIFA", "ASSENTOS").orderBy("ID")

df.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", os.environ.get("DB_URL", "jdbc:postgresql://localhost:5432/anac")) \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "voos_staged") \
    .option("user", os.environ.get("DB_USER", "postgres"))\
    .option("password", os.environ.get("PASSWORD_DB", "2705")).save()

spark.stop()
