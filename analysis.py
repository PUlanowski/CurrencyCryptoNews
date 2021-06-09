#%%
#create a spark session
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import abs
import pandas as pd
from datetime import datetime

spark = SparkSession.builder \
.appName("spark_on_docker") \
.config("spark.jars", "jdbc/postgresql-42.2.5.jar") \
.getOrCreate()

#%%
#spark.stop()
#%%

df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost/cnn') \
    .option('dbtable', 'information_schema.tables') \
    .option('user', 'postgres') \
    .option('password', '') \
    .option('driver', 'org.postgresql.Driver') \
    .load() \
    .filter("table_schema = 'public'")

#%%
df.createTempView('DIMS')
spark.sql("select table_name from DIMS where table_name like 'dim%'")

#%%
spark.sql("select table_name from DIMS where table_name like 'dim%'").count()
pdf_tables = spark.sql("select table_name from DIMS where table_name like 'dim%'").toPandas()
pdf_tables = pdf_tables.head(10) #head(n) for testing
#%%
pdf_all_diff = pd.DataFrame()

for table in pdf_tables['table_name']:

    df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost/cnn') \
        .option('dbtable', '{}'.format(table)) \
        .option('user', 'postgres') \
        .option('password', '') \
        .option('driver', 'org.postgresql.Driver') \
        .load()


    df_avg = df.groupby().avg('closure_rate').head()[0]
    df_vals = df.withColumn('diff', (df_avg - df['closure_rate']))
    df_vals = df_vals.withColumn('abs_diff', abs(df_vals.diff)).distinct()
    df_vals = df_vals.sort(col('abs_diff').desc())
    pdf_vals = df_vals.limit(5).toPandas()
    pdf_vals
    pdf_all_diff = pdf_all_diff.append(pdf_vals, ignore_index=True)

#%%
dates = pdf_all_diff['date']
dates = dates.unique()
datelst = []

for date in dates:
    dat = str(date.strftime('%Y-%m-%d'))
    datelst += [dat]

datelst = str(datelst).strip('[]')

#%%
######################CASSANDRA#####################
from cassandra.cluster import Cluster
from cassandra import query

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()
session.set_keyspace('cnn')


q = """
SELECT author,
 created_utc_date,
 selftext
 FROM cnn.wallstreetbets_all
WHERE created_utc_date IN {}{}{}
ALLOW FILTERING;
""".format('(',datelst,')')

pdf_posts = pd.DataFrame(session.execute(q))

#%%
pdf_posts
pdf_all_diff

pdf_posts = pdf_posts.rename(columns={'created_utc_date': 'date'})

new_row = {'author':'TEST', 'date':'2016-02-26', 'selftext':'TEST'}
pdf_posts = pdf_posts.append(new_row, ignore_index=True)


pdf_merged = pdf_all_diff.merge(pdf_posts, on='date')