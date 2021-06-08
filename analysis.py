#%%
#create a spark session
from pyspark.sql import SparkSession


spark = SparkSession.builder \
.appName("spark_on_docker") \
.config("spark.jars", "jdbc/postgresql-42.2.5.jar") \
.getOrCreate()

#%%
spark.stop()
#%%

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

df.show(5)
#%%
df.createTempView('DIMS')
spark.sql("select table_name from DIMS where table_name like 'dim%'").show(5)
print('total nr of dim tables:')
spark.sql("select table_name from DIMS where table_name like 'dim%'").count()

#%%

dim_tables_df = spark.sql("select table_name from DIMS where table_name like 'dim%'").toPandas()
dim_tables_df
