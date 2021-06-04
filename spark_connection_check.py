from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import pandas as pd
import os

spark = SparkSession.builder \
        .appName('postgres-cnn') \
        .master('spark://0.0.0.0:7077') \
        .config('spark.jars', 'C:\\Users\\pit\\Google Drive\\Udacity\\Capstone Project\\jdbc\\postgresql-42.2.5.jar',) \
        .getOrCreate()

spark.stop()

df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost:5432/cnn') \
    .option('dbtable', 'ccy_crypto') \
    .option('user', 'postgres') \
    .option('password', '') \
    .option('driver', 'org.postgresql.Driver') \
    .load()

df.printSchema()
df.columns


################################################
#CASSANDRA######################################
################################################

cluster = Cluster(['localhost'])
session = cluster.connect('cnn')
rows = session.execute('select * from wallstreetbets_all limit 5;')
for row in rows:
    print(row)
df = pd.DataFrame(list(rows))

