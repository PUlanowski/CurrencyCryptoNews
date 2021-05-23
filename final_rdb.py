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

config = configparser.ConfigParser()
config.read('cfg.cfg')

def conecct_to_db():
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

def create_star_schema(cur):

    cur.execute(sql.SQL(sql_queries.drop_fact))
    cur.execute(sql.SQL(sql_queries.fact_table))

    ccy_dict = iso4217.raw_table
    ccy_dict_v = ccy_dict.values()
    max_num = len(ccy_dict)
    global ccy_list
    ccy_list = []
    global crypto
    crypto = pd.read_csv(config['KAGGLE']['CRYPTO'])
    crypto_dates = crypto.date
    crypto_dates = crypto_dates.drop_duplicates()
    crypto = crypto.symbol
    crypto = crypto.drop_duplicates()
    max_crypto_num = len(crypto)

    for i in range(max_num):
        ccy = (list(ccy_dict_v)[i]).get('Ccy')
        ccy_list.append(ccy)

    #changing symbol @ in crypto name to _ for db compliance reasons
    for i in range(max_crypto_num):
        if '@' in crypto.values[i]:
            cv = crypto.values[i]
            cv = cv.replace("@", "_")
            crypto.values[i] = cv


    for i in range(max_num):
        cur.execute(sql.SQL(sql_queries.drop_dim_ccy.format \
                                (ccy_list[i])))
        cur.execute(sql.SQL(sql_queries.dim_ccy.format \
                                (ccy_list[i])))

    for i in range(max_crypto_num):
        cur.execute(sql.SQL(sql_queries.drop_dim_crypto.format \
                                (crypto.values[i])))

    for i in range(max_crypto_num):
        cur.execute(sql.SQL(sql_queries.dim_crypto.format \
                                (crypto.values[i])))

    return cur, crypto, ccy_list, crypto_dates

def insert_fact_data(cur, crypto, ccy_list, crypto_dates):
    all_ccy = ccy_list + crypto.values.tolist()
    global_ccy_no = len(ccy_list)
    crypto_ccy_no = len(crypto.values.tolist())
    type = []
    for i in range(global_ccy_no):
        type.append('Global')
    for i in range(crypto_ccy_no):
        type.append('Crypto')


    for i in range(len(all_ccy)):
        cur.execute(sql.SQL(sql_queries.insert_fact.format
                            ("'"+str(all_ccy[i])+"'",
                             "'"+str(type[i])+"'")))






