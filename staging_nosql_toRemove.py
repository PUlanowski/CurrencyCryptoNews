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

    wb = []
    for line in open('wallstreetbets_submission.json', 'r'):
        wb.append(json.loads(line))

    for i in range(0, len(wb)):
        t = wb[i].get('created_utc')
        d = datetime.fromtimestamp(t)
        d = d.date().strftime('%Y-%m-%d')
        wb[i]['created_utc_date'] = d

        wb[i].pop('approved_at_utc', None)
        wb[i].pop('saved', None)
        wb[i].pop('mod_reason_title', None)
        wb[i].pop('link_flair_richtext', None)
        wb[i].pop('subreddit_name_prefixed', None)
        wb[i].pop('hidden', None)
        wb[i].pop('pwls', None)
        wb[i].pop('link_flair_css_class', None)
        wb[i].pop('downs', None)
        wb[i].pop('thumbnail_height', None)
        wb[i].pop('top_awarded_type', None)
        wb[i].pop('hide_score', None)
        wb[i].pop('name', None)
        wb[i].pop('quarantine', None)
        wb[i].pop('link_flair_text_color', None)
        wb[i].pop('upvote_ratio', None)
        wb[i].pop('author_flair_background_color', None)
        wb[i].pop('subreddit_type', None)
        wb[i].pop('ups', None)
        wb[i].pop('total_awards_received', None)
        wb[i].pop('media_embed', None)
        wb[i].pop('thumbnail_width', None)
        wb[i].pop('author_flair_template_id', None)
        wb[i].pop('is_original_content', None)
        wb[i].pop('user_reports', None)
        wb[i].pop('secure_media', None)
        wb[i].pop('is_reddit_media_domain', None)
        wb[i].pop('is_meta', None)
        wb[i].pop('category', None)
        wb[i].pop('secure_media_embed', None)
        wb[i].pop('can_mod_post', None)
        wb[i].pop('approved_by', None)
        wb[i].pop('author_premium', None)
        wb[i].pop('author_flair_richtext', None)
        wb[i].pop('gildings', None)
        wb[i].pop('content_categories', None)
        wb[i].pop('mod_note', None)
        wb[i].pop('created', None)
        wb[i].pop('link_flair_type', None)
        wb[i].pop('wls', None)
        wb[i].pop('removed_by_category', None)
        wb[i].pop('author_flair_type', None)
        wb[i].pop('allow_live_comments', None)
        wb[i].pop('selftext_html', None)
        wb[i].pop('likes', None)
        wb[i].pop('suggested_sort', None)
        wb[i].pop('banned_at_utc', None)
        wb[i].pop('view_count', None)
        wb[i].pop('archived', None)
        wb[i].pop('no_follow', None)
        wb[i].pop('is_crosspostable', None)
        wb[i].pop('pinned', None)
        wb[i].pop('all_awardings', None)
        wb[i].pop('awarders', None)
        wb[i].pop('media_only', None)
        wb[i].pop('link_flair_template_id', None)
        wb[i].pop('can_gild', None)
        wb[i].pop('spoiler', None)
        wb[i].pop('locked', None)
        wb[i].pop('treatment_tags', None)
        wb[i].pop('visited', None)
        wb[i].pop('removed_by', None)
        wb[i].pop('num_reports', None)
        wb[i].pop('mod_reason_by', None)
        wb[i].pop('removal_reason', None)
        wb[i].pop('link_flair_background_color', None)
        wb[i].pop('is_robot_indexable', None)
        wb[i].pop('report_reasons', None)
        wb[i].pop('discussion_type', None)
        wb[i].pop('send_replies', None)
        wb[i].pop('whitelist_status', None)
        wb[i].pop('mod_reports', None)
        wb[i].pop('author_patreon_flair', None)
        wb[i].pop('author_flair_text_color', None)
        wb[i].pop('parent_whitelist_status', None)
        wb[i].pop('subreddit_subscribers', None)
        wb[i].pop('num_crossposts', None)
        wb[i].pop('media', None)
        wb[i].pop('is_video', None)
        wb[i].pop('crosspost_parent', None)
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
        wb[i].pop('parent_whitelist_status', None)
        wb[i].pop('whitelist_status', None)
        wb[i].pop('is_crosspostable', None)
        wb[i].pop('num_crossposts', None)
        wb[i].pop('pinned', None)
        wb[i].pop('is_reddit_media_domain', None)
        wb[i].pop('preview', None)
        wb[i].pop('brand_safe', None)
        wb[i].pop('author_cakeday', None)
        wb[i].pop('crosspost_parent_list', None)
        wb[i].pop('rte_mode', None)
        wb[i].pop('previous_visits', None)
        wb[i].pop('author_id', None)
        wb[i].pop('url_overridden_by_dest', None)


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
            if wb[i]['link_flair_text'] is not None:
                lft = wb[i]['link_flair_text']
                lft = lft.replace("'", "''")
                wb[i]['link_flair_text'] = lft


        j = json.dumps(wb[i])

        insert_json = 'INSERT INTO cnn.wallstreetbets_all JSON {}'.format("'"+j+"'")
        session.execute(insert_json)


if __name__=="__main__":
    stage_wallstreetbets()
    insert_data()
