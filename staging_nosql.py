from cassandra.cluster import Cluster

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

session.execute('USE cnn;')
session.execute('CREATE TABLE commits ( \
id text, \
path text, \
author_email text, \
author_name text, \
subject text, \
body text, \
date timestamp, \
PRIMARY KEY (id, date));'
)
