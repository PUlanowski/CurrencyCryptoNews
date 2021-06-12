#!/usr/bin/python3
from db import sql_queries
import psycopg2
import configparser
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import logging


config = configparser.ConfigParser()
config.read('cfg.cfg')

def conecct_to_db():
    """
    This is standard connection to Postgres database. It uses cfg data from separate file.
    :return: cursor that is required to query database.
    """
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

def check_staging_rdb(cur):
    """
    This quality check verify if staging tables in RDB exists and have expected number of records
    :param cur: Postgres connection cursor
    :return: print check result and log
    """
    staging_tables = {'ccy_crypto_stage': 942297,
                      'ccy_rates_stage': 5978
                      }

    for table in staging_tables.keys():
        cur.execute(sql.SQL(sql_queries.exists_stage \
                            .format(table)))
        exists = cur.fetchall()
        if str(exists[0][0]) == 'True':
            print('table', table, 'exists')
            logging.info('table', table, 'exists')
            cur.execute(sql.SQL(sql_queries.count_stage\
                                    .format(table)))
            rows = cur.fetchall()
            if staging_tables.get(table) == rows[0][0]:
                print('rows count in', table, 'match')
                logging.info('rows count in',table,'match')
            else:
                print('rows count in',table,'does not match')
                logging.warning('rows count in',table,'does not match')
        else:
            print('table', table,'does not exists')
            logging.warning('table', table,'does not exists')


def create_star_schema(cur):
    """
    This function automaticaly creates star schema for all currencies in RDB
    :param cur: Postgres connection cursor
    :return: RDB have all tables setup and ready to be populated
    """

    #create fact table
    cur.execute(sql.SQL(sql_queries.drop_fact))
    cur.execute(sql.SQL(sql_queries.fact_table))

    global ccy_map
    global ccy_map_count
    ccy_map = pd.read_csv(config['HELPERS']['CCYMAP'])
    ccy_map_count = len(ccy_map) -1

    for i in range(ccy_map_count):
    #drop ccy dim tables
        cur.execute(sql.SQL(sql_queries.drop_dim_ccy.format \
                                (ccy_map['symbol'][i])))
    #create ccy dim tables
        cur.execute(sql.SQL(sql_queries.dim_ccy.format \
                                (ccy_map['symbol'][i])))
    ###
    global crypto
    global crypto_count
    crypto = pd.read_csv(config['KAGGLE']['CRYPTO'])
    crypto = crypto.symbol
    crypto = crypto.drop_duplicates()
    crypto_count = len(crypto)
    for i in range(crypto_count):
        if '@' in crypto.values[i]:
            cv = crypto.values[i]
            cv = cv.replace("@", "_")
            crypto.values[i] = cv


    #drop crypto dim tables
    for i in range(crypto_count):
        cur.execute(sql.SQL(sql_queries.drop_dim_crypto.format \
                                (crypto.values[i])))
    #create crypto dim tables
    for i in range(crypto_count):
        cur.execute(sql.SQL(sql_queries.dim_crypto.format \
                                (crypto.values[i])))

    return cur, crypto, crypto_count

def insert_fact_data(cur, crypto, ccy_map):
    """
    Inserting data in fact table in RDB
    :param cur: Postgres connection cursor
    :param crypto: crypto_ccy list
    :param ccy_map: currency mapper
    :return: Populated foct table
    """
    crypto_list = crypto.values.tolist()

    #insert values into fact table - ccy
    for i in range(ccy_map_count):
        cur.execute(sql.SQL(sql_queries.insert_fact.format
                            ("'"+str(ccy_map['symbol'][i])+"'",
                             "'"+'global'+"'")))

    #insert values into fact table - crypto
    for i in range(len(crypto_list)):
        cur.execute(sql.SQL(sql_queries.insert_fact.format
                            ("'"+str(crypto_list[i])+"'",
                             "'"+'crypto'+"'")))

def insert_dim_ccy(cur, ccy_map):
    """
    Inserting data in dimension tables in RDB
    :param cur: Postgres connection cursor
    :param ccy_map: currency mapper
    :return: Populated dimension tables
    """

    for i in range(ccy_map_count):
        ccy = ccy_map['symbol'][i]
        ccy = ccy.lower()
        col_nm = ccy_map['col_name'][i]
        cur.execute(sql.SQL(sql_queries.insert_dim_ccy.format(
            ccy, col_nm)))

        for i in range(ccy_map_count):
            ccy = ccy_map['symbol'][i]
            ccy = ccy.lower()
            name = ccy_map['name'][i]
            cur.execute(sql.SQL(
                sql_queries.update_dim_ccy.format(
                ccy, "'"+name+"'", "'"ccy"'")))



def insert_crypto(cur, crypto):
    """
    Inserting data in dimension tables in RDB for crypto ccy
    :param cur: Postgres connection cursor
    :param crypto: crypto_ccy list
    :return: Populated crypto ccy dimension tables
    """

    for symbol in crypto:
        symbol_l = symbol.lower()
        cur.execute(sql.SQL(sql_queries.create_crypto_view.format(
            "'"+symbol+"'")))
        cur.execute(sql.SQL(sql_queries.insert_crypto.format(
            symbol_l, "'"+symbol+"'" )))
        cur.execute(sql.SQL(sql_queries.update_crypto.format(
            symbol_l, "'"+symbol+"'")))
        cur.execute(sql.SQL(sql_queries.drop_crypto_view))


if __name__=="__main__":
    conecct_to_db()
    check_staging_rdb(cur)
    create_star_schema(cur)
    insert_fact_data(cur, crypto, ccy_map)
    insert_dim_ccy(cur, ccy_map)
    insert_crypto(cur, crypto)
