from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('postgres-cnn') \
        .master('spark://0.0.0.0:7077') \
        .config('spark.jars', 'C:\\Users\\pit\\Google Drive\\Udacity\\Capstone Project\\jdbc\\postgresql-42.2.5.jar') \
        .getOrCreate()

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

spark.stop()

################################################
# https://docs.datastax.com/en/archived/datastax_enterprise/4.6/datastax_enterprise/spark/sparkPySpark.html
# https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
from pyspark.sql import SQLContext, SparkSession

spark = SparkSession.builder \
    .appName('cassandra-cnn') \
    .config('spark.cassandra.connection.host', 'localhost') \
    .config('spark.cassandra.connection.port', '9042') \
    .config('spark.jars', 'C:\\Users\\pit\\Google Drive\\Udacity\\Capstone Project\\jdbc\\spark-cassandra-connector_2.11-2.0.2.jar') \
    .getOrCreate()

ds = sqlContext \
  .read \
  .format('org.apache.spark.sql.cassandra') \
  .options(table='tablename', keyspace='keyspace_name') \
  .load()

ds.show(10)