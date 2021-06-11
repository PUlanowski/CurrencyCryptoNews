--Connect to Cassandra from docker cli via 'cqlsh' command

--Create tablespace

CREATE KEYSPACE cnn WITH REPLICATION = {
'class' : 'SimpleStrategy',
'replication_factor' : 1 };

--Use tablespace
USE cnn;

--Create table example
 CREATE TABLE commits ( 
 id text, 
 path text, 
 author_email text, 
 author_name text, 
 subject text, 
 body text, 
 date timestamp, 
 PRIMARY KEY (id, date));'

--Create wallstreetbets_all table
CREATE TABLE wallstreetbets_all (
domain text,
created_utc_date date,
author text,
author_created_utc int,
author_flair_css_class text,
author_flair_text text,
created_utc int,
edited int,
full_link text,
id text,
is_self boolean,
num_comments int,
over_18 boolean,
permalink text,
score int,
selftext varchar,
subreddit text,
subreddit_id text,
thumbnail text,
title text,
url text,
PRIMARY KEY (domain, created_utc));


