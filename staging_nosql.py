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
    PRIMARY KEY ((author, id),created_utc_date));'
    session.execute(create_table_all)



def insert_data():

    wb = []
    for line in open('wallstreetbets_submission.json', 'r'):
        wb.append(json.loads(line))

    for i in range(0, len(wb)):
        t = wb[i].get('created_utc')
        d = datetime.fromtimestamp(t)
        d = d.date().strftime('%Y-%m-%d')
        wb[i]['created_utc_date'] = d
        wb[i].pop('media_embed',None)
        wb[i].pop('media',None)
        wb[i].pop('mod_reports', None)
        wb[i].pop('secure_media_embed', None)
        wb[i].pop('secure_media', None)
        wb[i].pop('user_reports', None)
        wb[i].pop('preview', None)
        wb[i].pop('locked', None)
        wb[i].pop('link_flair_css_class', None)
        wb[i].pop('spoiler', None)
        wb[i].pop('brand_safe', None)
        wb[i].pop('suggested_sort', None)
        wb[i].pop('author_cakeday', None)
        wb[i].pop('thumbnail_height', None)
        wb[i].pop('thumbnail_width', None)
        wb[i].pop('is_video', None)
        wb[i].pop('approved_at_utc', None)
        wb[i].pop('banned_at_utc', None)
        wb[i].pop('can_mod_post', None)
        wb[i].pop('view_count', None)


        tt = wb[i]['title']
        tt = tt.replace("'","''")
        wb[i]['title'] = tt

        url = wb[i]['url']
        url = url.replace("'","''")
        wb[i]['url'] = url



        if 'description' in wb[i]:
            ds = wb[i]['description']
            ds = st.replace("'","''")
            wb[i]['description'] = ds

        if 'selftext' in wb[i]:
            st = wb[i]['selftext']
            st = st.replace("'", "''")
            wb[i]['selftext'] = st

        if 'author_flair_text' in wb[i]:
            if wb[i]['author_flair_text'] is not None:
                aft = wb[i]['author_flair_text']
                aft = aft.replace("'", "''")
                wb[i]['author_flair_text'] = aft

        if 'link_flair_text' in wb[i]:
            lft = wb[i]['link_flair_text']
            lft = lft.replace("'", "''")
            wb[i]['link_flair_text'] = lft


        j = json.dumps(wb[i])

        insert_json = 'INSERT INTO cnn.wallstreetbets_all JSON {}'.format("'"+j+"'")
        session.execute(insert_json)


# from datetime import datetime
# t = wb[0].get('created_utc')
# d = date.fromtimestamp(t)
# d = d.date()


if __name__=="__main__":
    stage_wallstreetbets()
    insert_data()
