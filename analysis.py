#!/usr/bin/python3
#%%
'''
This is analytics final file. This can be parsed to Jupyter notebook or run in cells. Cell is marked as "#%%" here
'''


#create a spark session
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import abs
import pandas as pd
from datetime import datetime
try:
    spark = SparkSession.builder \
    .appName("spark_on_docker") \
    .config("spark.jars", "jdbc/postgresql-42.2.5.jar") \
    .getOrCreate()
except Exception as e:
    print(e)

#%%
#Spark read schema and fetch all dim tablenames and expose to python
df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost/cnn') \
    .option('dbtable', 'information_schema.tables') \
    .option('user', 'postgres') \
    .option('password', '') \
    .option('driver', 'org.postgresql.Driver') \
    .load() \
    .filter("table_schema = 'public'")

df.createTempView('DIMS')
spark.sql("select table_name from DIMS where table_name like 'dim%'")


spark.sql("select table_name from DIMS where table_name like 'dim%'").count()
pdf_tables = spark.sql("select table_name from DIMS where table_name like 'dim%'").toPandas()

#%%
#Spark creates some table for future analysis
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
    pdf_vals = df_vals.limit(10).toPandas()
    pdf_vals
    pdf_all_diff = pdf_all_diff.append(pdf_vals, ignore_index=True)

#%%
#Building dates list that we're interested in
dates = pdf_all_diff['date']
dates = dates.unique()
datelst = []

for date in dates:
    dat = str(date.strftime('%Y-%m-%d'))
    datelst += [dat]

datelst = sorted(datelst)
datelst = str(datelst).strip('[]')

#%%
#Cassandra fetching data from created tables but oonly for selected dates
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()
session.set_keyspace('cnn')

q = """
SELECT *
FROM wb_titles
WHERE date IN {}{}{}
ALLOW FILTERING;
""".format('(',datelst,')')

q = """
SELECT date, count(*)
FROM wb_titles
WHERE date IN {}{}{}
ALLOW FILTERING;
""".format('(',datelst,')')

try:
    pdf_titles = pd.DataFrame(session.execute(q))
except Exception as e:
    print(e)

#As we hava format mismatch with python we're converting cassandra.util.Date to datetime.date
for n in range(0, len(pdf_titles)):
    pdf_titles['date'][n] = pdf_titles['date'][n].date()


#%%
#SAMPLE QUERIES
#get all titles text from selected day
pdf_group_titles = pdf_titles.groupby('date')['title'].apply('| '.join).reset_index()
#get all titles quantity from selected day
pdf_group_titles_count = pdf_titles.groupby('date')['title'].agg('count').reset_index()
#get all authors from selected day
pdf_group_authors = pdf_titles.groupby('date')['author'].apply('| '.join).reset_index()
#get dates when ccy rates were posted
pdf_group_ccy_name = pdf_all_diff.groupby('date')['name'].apply('| '.join).reset_index()
#get all currencies rates quantity from selected day
pdf_group_ccy_name_count = pdf_all_diff.groupby('date')['name'].agg('count').reset_index()
#get type, name of ccy and calc min/mean/max from abs_diff
pdf_group_ccy_diff = pdf_all_diff.groupby(['type','name']).agg({'abs_diff':['min', 'mean', 'max']})
pdf_group_ccy_diff = pdf_group_ccy_diff.sort_index()
#get dates and ccy names alongside titles for further association analysis
pdf_merge1 = pdf_group_ccy_name.merge(pdf_group_titles, on='date')
#get dates and ccy names alongside posts author for further association analysis
pdf_merge2 = pdf_group_ccy_name.merge(pdf_group_authors, on='date')
#get ccy names quantity and titles posted quantity for further analysis
pdf_merge3 = pdf_group_ccy_name_count.merge(pdf_group_titles_count, on='date')

#%%
#SAMPLE CHART
from matplotlib import pyplot as plt
ax=plt.gca()
pdf_merge3.plot(kind='line', x='date', y='name', color='red', ax=ax)
pdf_merge3.plot(kind='scatter', x='date', y='title', color='blue', ax=ax)
plt.show()