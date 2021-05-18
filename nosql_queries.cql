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

--Create wallstreetbets_all table
'CREATE TABLE wallstreetbets (\
author text,\
author_flair_css_class text,\
author_flair_text text,\
created_utc int,\
domain text,\
full_link text,\
id text,\
is_self boolean,\
num_comments int,\
over_18 boolean,\
permalink text,\
score int,\
selftext text,\
subreddit text,\
subreddit_id text,\
thumbnail text,\
title text,\
url text,\
created_utc_date date,\
PRIMARY KEY (domain, created_utc));'



author,
author_flair_css_class,
author_flair_text,
created_utc,
domain,
full_link,
id,
is_self,
num_comments,
over_18,
permalink,
score,
selftext,
subreddit,
subreddit_id,
thumbnail,
title,
url,
created_utc_date,