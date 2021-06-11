#!/usr/bin/python3
#%%
from cassandra.cluster import Cluster
from cassandra import query

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()
session.set_keyspace('cnn')

#Create smaller table to match with Postgress data
q = "CREATE TABLE IF NOT EXISTS wb_texts"
q = q + """
(date date,
author text,
id text,
selftext text,
PRIMARY KEY(date, author, id))
"""
try:
    session.execute(q)
except Exception as e:
    print(e)

###in CQLsh:###
# USE cnn;
# COPY wallstreetbets_all (created_utc_date, author, id, selftext) TO 'temp.csv';
# COPY wb_texts(date, author, id, selftext) FROM 'temp.csv';

#Create table to calculate some statistics
q = "CREATE TABLE IF NOT EXISTS wb_titles"
q = q + """
(date date,
author text,
id text,
title text,
PRIMARY KEY(date, author, id))
"""
try:
    session.execute(q)
except Exception as e:
    print(e)

###in CQLsh:###
# USE cnn;
# COPY wallstreetbets_all (created_utc_date, author, id, title) TO 'temp.csv';
# COPY wb_titles(date, author, id, title) FROM 'temp.csv';