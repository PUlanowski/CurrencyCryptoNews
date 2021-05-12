#!/usr/bin/python3
import psycopg2
import configparser
from sql_queries import *

config = configparser.ConfigParser()
config.read('cfg.cfg')

#setting up fresh database
conn = psycopg2.connect(
    host = config['RDS']['ENDPOINT'],
    user = config['RDS']['USER'],
    password = config['RDS']['PASSWORD'],
    port = config['RDS']['PORT'])
cur = conn.cursor()
cur.execute ('CREATE DATABASE '+config['RDS']['DBNAME'])
cur.commit()
conn.close()

#establishing connection with new database
conn = psycopg2.connect(
    host = config['RDS']['ENDPOINT'],
    dbname = config['RDS']['DBNAME'],
    user = config['RDS']['USER'],
    password = config['RDS']['PASSWORD'],
    port = config['RDS']['PORT'])
cur = conn.cursor()
conn.autocommit = True

cur.execute('CREATE TABLE test (one varchar);')
cur.execute('DROP TABLE test')

