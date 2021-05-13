#!/usr/bin/python3
import iso4217
import sql_queries
import psycopg2
import configparser
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import itertools
from datetime import datetime

def create_postgress_conn():
    config = configparser.ConfigParser()
    config.read('cfg.cfg')
    conn = psycopg2.connect(
        host=config['RDS']['ENDPOINT'],
        dbname=config['RDS']['DBNAME'],
        user=config['RDS']['USER'],
        password=config['RDS']['PASSWORD'],
        port=config['RDS']['PORT'])
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    global cur
    cur = conn.cursor()

    return cur

def stage_ccy_mapping(cur):
    ccy_dict = iso4217.raw_table
    ccy_dict_v = ccy_dict.values()
    max_num = len(ccy_dict)

    cur.execute(sql.SQL(sql_queries.drop_ccy_map))
    cur.execute(sql.SQL(sql_queries.create_ccy_map))

    for i in range(max_num):
        ccy = (list(ccy_dict_v)[i]).get('Ccy')
        ccy_nm = (list(ccy_dict_v)[i]).get('CcyNm')
        if ccy != None:
            cur.execute(sql.SQL(sql_queries.insert_ccy_map.format\
            ("'"+ccy+"'" , "'"+ccy_nm+"'")))

def stage_ccy_rates(cur):
    config = configparser.ConfigParser()
    config.read('cfg.cfg')
    data = pd.read_csv(config['KAGGLE']['CCY'])
    row_no = len(data)
    #print for SQL create statement
    columns = data.columns
    columns = columns.values[1:]
    l_columns = []
    cur.execute(sql.SQL(sql_queries.drop_ccy_rates))

    for col in columns:
        col = col.replace('.','')
        col = col.replace(' ','_')
        col = (col + ' FLOAT,')
        l_columns.append(col)

    s_columns = ' '.join(map(str, l_columns))
    s_columns = s_columns[:-1]
    cur.execute(sql.SQL(sql_queries.create_ccy_rates.format \
                        (s_columns)))

def stage_crypto(cur):
    config = configparser.ConfigParser()
    config.read('cfg.cfg')
    data = pd.read_csv(config['KAGGLE']['CRYPTO'])
    cur.execute(sql.SQL(sql_queries.drop_crypto))
    cur.execute(sql.SQL(sql_queries.create_crypto))


if __name__=="__main__":
    create_postgress_conn()
    stage_ccy_mapping(cur)
    stage_ccy_rates(cur)
    stage_crypto(cur)
