#!/usr/bin/python3
from cassandra.cluster import Cluster
import json
from datetime import datetime


def stage_wallstreetbets():
    cluster = Cluster(['localhost'], port=9042)
    global session
    session = cluster.connect()

    session.execute('USE cnn;')

    drop_table_all = 'DROP TABLE IF EXISTS wallstreetbets_all;'
    session.execute(drop_table_all)

    create_table_all = 'CREATE TABLE wallstreetbets_all ( \
    created_utc_date date, \
    domain text, \
    author text, \
    author_created_utc int, \
    author_flair_css_class text, \
    author_flair_text text, \
    author_fullname text, \
    created_utc int, \
    distinguished text, \
    edited int, \
    full_link text, \
    gilded int, \
    id text, \
    is_self boolean, \
    num_comments int, \
    over_18 boolean, \
    permalink text, \
    retrieved_on int,\
    score int, \
    selftext varchar, \
    stickied boolean, \
    subreddit text, \
    subreddit_id text, \
    thumbnail text, \
    title text, \
    url text, \
    post_hint text, \
    banned_by text, \
    link_flair_text text, \
    contest_mode boolean, \
    clicked boolean, \
    PRIMARY KEY ((author, id),created_utc_date));'
    session.execute(create_table_all)



def insert_data():

    keys = ['created_utc_date', 'domain', 'author', 'author_created_utc', 'author_flair_css_class', 'author_flair_text', 'author_fullname', 'created_utc', 'distinguished', 'edited', 'full_link', 'gilded', 'id', 'is_self', 'num_comments', 'over_18', 'permalink', 'retrieved_on', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'thumbnail', 'title', 'url', 'post_hint', 'banned_by', 'link_flair_text', 'contest_mode', 'clicked']
    wb = []

    for line in open(
            'wallstreetbets_submission.json', 'r'):
        wb.append(json.loads(line))

    for i in range(0, len(wb)):
        #removing unwanted keys
        key_list = list(wb[i].keys())
        for key in key_list:
            if key not in keys:
                wb[i].pop(key, None)

        #appending time in date format
        t = wb[i].get('created_utc')
        d = datetime.fromtimestamp(t)
        d = d.date().strftime('%Y-%m-%d')
        wb[i]['created_utc_date'] = d

        #escaping characters in body
        txt_fields = ['title','url', 'description', 'selftext', 'author_flair_text', 'link_flair_text']

        for field in txt_fields:
            if field in key_list:
                if wb[i][field] is not None:
                    x = wb[i][field]
                    x = x.replace("'","''")
                    wb[i][field] = x

        #parsing dict as json
        j = json.dumps(wb[i])

        #inserting prepared json into database
        insert_json = 'INSERT INTO cnn.wallstreetbets_all JSON {}'.format("'"+j+"'")
        session.execute(insert_json)


if __name__=="__main__":
    stage_wallstreetbets()
    insert_data()
