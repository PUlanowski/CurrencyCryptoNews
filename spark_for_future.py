from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('postgres-cnn') \
        .master('spark://0.0.0.0:7077') \
        .config('spark.jars', 'C:\\Users\\pit\\Google Drive\\Udacity\\Capstone Project\\jdbc\\postgresql-42.2.5.jar',) \
        .getOrCreate()

###

df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost:5432/cnn') \
    .option('dbtable', 'ccy_crypto_stage') \
    .option('user', 'postgres') \
    .option('password', '') \
    .option('driver', 'org.postgresql.Driver') \
    .load()

df.printSchema()