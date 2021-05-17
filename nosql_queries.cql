--Create tablespace

CREATE KEYSPACE cnn WITH REPLICATION = {
'class' : 'SimpleStrategy',
'replication_factor' : 1 };

--Use tablespace
'USE cnn;'

--Create table example
 'CREATE TABLE commits ( \
 id text, \
 path text, \
 author_email text, \
 author_name text, \
 subject text, \
 body text, \
 date timestamp, \
 PRIMARY KEY (id, date));'