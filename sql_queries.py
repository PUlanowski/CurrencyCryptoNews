#DROP STAGING TABLES

drop_ccy_map = 'DROP TABLE IF EXISTS ccy_map;'
drop_ccy_rates = 'DROP TABLE IF EXISTS ccy_rates;'
drop_crypto = 'DROP TABLE IF EXISTS ccy_crypto;'


#CREATE DATABASES

create_postgres_db = 'CREATE DATABASE cnn'

#CREATE STAGING TABLES

create_ccy_map = 'CREATE TABLE IF NOT EXISTS ccy_map( \
                  ccy VARCHAR(3), \
                  ccy_full VARCHAR \
                 );'

create_ccy_rates = 'CREATE TABLE IF NOT EXISTS ccy_rates( \
                    date DATE, \
                    {} \
                 );'

create_crypto = 'CREATE TABLE IF NOT EXISTS ccy_crypto( \
                     slug VARCHAR, \
                     symbol VARCHAR, \
                     name VARCHAR, \
                     date DATE, \
                     ranknow INTEGER, \
                     open FLOAT, \
                     high FLOAT, \
                     low FLOAT, \
                     close FLOAT, \
                     volume FLOAT, \
                     market FLOAT, \
                     close_ratio FLOAT, \
                     spread FLOAT \
                 );'

#INSERT VALUES

insert_ccy_map = 'INSERT INTO ccy_map(ccy, ccy_full) VALUES ({},{});'

#CREATE FACT & DIMENSION TABLES

fact_table = 'CREATE TABLE IF NOT EXISTS fact( \
                     date DATE,\
                     symbol VARCHAR, \
                     market VARCHAR);'

dim_table = 'CREATE TABLE IF NOT EXISTS dim_ccy_{}( \
                    date DATE, \
                    closure_rate FLOAT);'
