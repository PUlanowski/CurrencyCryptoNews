#DROP STAGING TABLES

drop_ccy_map = 'DROP TABLE IF EXISTS ccy_map;'
drop_ccy_rates = 'DROP TABLE IF EXISTS ccy_rates;'



#CREATE STAGING TABLES



#CREATE TABLES

create_ccy_map = 'CREATE TABLE IF NOT EXISTS ccy_map( \
                  ccy VARCHAR(3), \
                  ccy_full VARCHAR \
                 );'

create_ccy_rates = 'CREATE TABLE IF NOT EXISTS ccy_rates( \
                    date DATE, \
                    {} \
                 );'


#INSERT VALUES

insert_ccy_map = 'INSERT INTO ccy_map(ccy, ccy_full) VALUES ({},{});'
insert_ccy_rates = 'INSERT INTO ccy_rates({}) VALUES ({})'