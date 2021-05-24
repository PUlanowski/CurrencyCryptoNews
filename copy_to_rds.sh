#files need to be moved to server/container which is running databases
#example:
#docker cp "C:\Users\pit\Google Drive\Udacity\Capstone Project\crypto-markets.csv" 69dab82864e5091c94fd33c2dc15e48756d5e8150cf24ff7b56c3624f71006e6:/crypto-markets.csv

#connect to psql
 psql -h [HOST] --port=5432 -U postgres --dbname=cnn

#run in psql
\copy ccy_rates FROM './currency_exchange_rates_02-01-1995_-_02-05-2018.csv' DELIMITER ',' CSV HEADER;

\copy ccy_crypto FROM './crypto-markets.csv' DELIMITER ',' CSV HEADER;

