#!/usr/bin/python3
import sql_queries
import psycopg2
import configparser
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import itertools
from datetime import datetime

config = configparser.ConfigParser()
config.read('cfg.cfg')

def create_postgress_db():
    conn = psycopg2.connect(
        host = config['POSTGRES']['HOST'],
        user = config['POSTGRES']['USER'],
        password =config['POSTGRES']['PASS'],
        port = config['POSTGRES']['PORT']
    )
    cur = conn.cursor()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur.execute(sql.SQL(sql_queries.create_postgres_db))
    conn.close()

def reconecct_to_db():
    conn = psycopg2.connect(
        host = config['POSTGRES']['HOST'],
        user = config['POSTGRES']['USER'],
        database = config['POSTGRES']['DATABASE'],
        password =config['POSTGRES']['PASS'],
        port = config['POSTGRES']['PORT']
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    global cur
    cur = conn.cursor()
    return cur

def stage_ccy_mapping(cur):
    ccy_map = pd.read_csv(config['HELPERS']['CCYMAP'])
    max_num = len(ccy_map) -1

    cur.execute(sql.SQL(sql_queries.drop_ccy_map))
    cur.execute(sql.SQL(sql_queries.create_ccy_map))

    for i in range(max_num):
        ccy = ccy_map['symbol'][i]
        ccy_nm = ccy_map['name'][i]
        cur.execute(sql.SQL(sql_queries.insert_ccy_map.format\
            ("'"+ccy+"'" , "'"+ccy_nm+"'")))

def stage_ccy_rates(cur):
    data = pd.read_csv(config['KAGGLE']['CCY'])
    #print for SQL create statement
    columns = data.columns
    columns = list(columns)
    columns = columns[1:]
    l_columns = []

    for col in columns:
        col = col.replace('.','')
        col = col.replace(' ','_')
        col = col.lower()
        col = (col + ' FLOAT,')
        l_columns.append(col)


    s_columns = ' '.join(map(str, l_columns))
    s_columns = s_columns[:-1]

    cur.execute(sql.SQL(sql_queries.drop_ccy_rates))
    cur.execute(sql.SQL(sql_queries.create_ccy_rates.format \
                        (s_columns)))

def stage_crypto(cur):
    config = configparser.ConfigParser()
    config.read('cfg.cfg')
    data = pd.read_csv(config['KAGGLE']['CRYPTO'])
    cur.execute(sql.SQL(sql_queries.drop_crypto))
    cur.execute(sql.SQL(sql_queries.create_crypto))


if __name__=="__main__":
    create_postgress_db()
    reconecct_to_db()
    stage_ccy_mapping(cur)
    stage_ccy_rates(cur)
    stage_crypto(cur)
