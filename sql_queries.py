#DROP STAGING TABLES

drop_ccy_map = 'DROP TABLE IF EXISTS ccy_map;'
drop_ccy_rates = 'DROP TABLE IF EXISTS ccy_rates;'
drop_crypto = 'DROP TABLE IF EXISTS ccy_crypto;'
drop_dim_ccy = 'DROP TABLE IF EXISTS dim_ccy_{}'
drop_dim_crypto = 'DROP TABLE IF EXISTS dim_crypto_{}'
drop_fact = 'DROP TABLE IF EXISTS fact'

#CREATE DATABASES

create_postgres_db = 'CREATE DATABASE cnn'

#CREATE STAGING TABLES

create_ccy_map = 'CREATE TABLE IF NOT EXISTS ccy_map( \
                  ccy VARCHAR(3), \
                  ccy_full VARCHAR \
                 );'

create_ccy_rates = 'CREATE TABLE IF NOT EXISTS ccy_rates_stage( \
                    date DATE, \
                    {} \
                 );'

create_crypto = 'CREATE TABLE IF NOT EXISTS ccy_crypto_stage( \
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

#CREATE FACT & DIMENSION TABLES

fact_table = 'CREATE TABLE IF NOT EXISTS fact( \
                     symbol VARCHAR, \
                     type VARCHAR);'

dim_ccy = 'CREATE TABLE IF NOT EXISTS dim_ccy_{}( \
                    type VARCHAR, \
                    date DATE, \
                    name VARCHAR, \
                    closure_rate FLOAT);'

dim_crypto = 'CREATE TABLE IF NOT EXISTS dim_crypto_{}( \
                    type VARCHAR, \
                    date DATE, \
                    name VARCHAR, \
                    closure_rate FLOAT, \
                    close_ratio FLOAT, \
                    spread FLOAT);'


#INSERT VALUES

insert_ccy_map = 'INSERT INTO ccy_map(ccy, ccy_full) VALUES ({},{});'

insert_fact = 'INSERT INTO fact(symbol, type) VALUES ({},{});'

insert_dim_ccy = 'INSERT INTO dim_ccy_{}(date, closure_rate) \
                SELECT date, {} \
                FROM ccy_rates;'

update_dim_ccy = 'UPDATE dim_ccy_{} SET type = "global", name = {};'

create_crypto_view = 'CREATE VIEW temp_view AS \
                        SELECT * FROM ccy_crypto \
                          WHERE symbol = {};'
insert_crypto = 'INSERT INTO dim_crypto_{}(date, name, closure_rate, \
                close_ratio, spread) \
                SELECT date, name, close, close_ratio, spread \
                FROM ccy_crypto \
                WHERE symbol = {};'

update_crypto = "UPDATE dim_crypto_{} SET type = 'crypto';"

drop_crypto_view = 'DROP VIEW temp_view;'