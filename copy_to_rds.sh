#connect to psql
 psql -h cnn.ckveontkveyj.us-east-1.rds.amazonaws.com --port=5432 -U postgres --dbname=cnn
#run in psql
\copy ccy_rates FROM '~/currency_exchange_rates_02-01-1995_-_02-05-2018.csv' DELIMITER ',' CSV HEADER;

\copy ccy_crypto FROM '~/crypto-markets.csv' DELIMITER ',' CSV HEADER;
