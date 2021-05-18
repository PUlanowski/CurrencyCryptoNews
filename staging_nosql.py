#!/usr/bin/python3
from cassandra.cluster import Cluster
import json
from datetime import datetime

def stage_wallstreetbets():
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()

    session.execute('USE cnn;')

    session.execute('CREATE TABLE wallstreetbets_all ( \
    author text, \
    author_flair_css_class text, \
    author_flair_text text, \
    created_utc int, \
    domain text, \
    full_link text, \
    id text, \
    is_self boolean, \
    media_embed text, \
    num_comments int, \
    over_18 boolean, \
    permalink text, \
    score int, \
    selftext text, \
    subreddit text, \
    subreddit_id text, \
    thumbnail text, \
    title text, \
    url text, \
    created_utc_date date, \
    PRIMARY KEY (domain, created_utc));')

def insert_data():
    wb = []
    for line in open('wallstreetbets_submission.json', 'r'):
        wb.append(json.loads(line))

    for i in range(0, len(wb)):
        t = wb[i].get('created_utc')
        d = datetime.fromtimestamp(t)
        d = d.date()
        wb[i]['created_utc_date'] = d
        wb[i] = list(wb[i].values())



# from datetime import datetime
# t = wb[0].get('created_utc')
# d = date.fromtimestamp(t)
# d = d.date()


if __name__=="__main__":
    stage_wallstreetbets()
    insert_data()
