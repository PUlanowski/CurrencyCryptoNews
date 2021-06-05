
#%%
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import configparser
import sql_queries
import pandas as pd
import pandas.io.sql as sqlio

config = configparser.ConfigParser()
config.read('cfg.cfg')

conn = psycopg2.connect(
    host=config['POSTGRES']['HOST'],
    user=config['POSTGRES']['USER'],
    database=config['POSTGRES']['DATABASE'],
    password=config['POSTGRES']['PASS'],
    port=config['POSTGRES']['PORT']
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()


table_names = sqlio.read_sql_query(sql_queries.get_table_names, conn)
table_names.head()
