# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import logging
import pymongo
import json
import os
import time
from datetime import datetime
import requests
import requests_cache
# Requests Cache name, DB to use, expiration time
requests_cache.install_cache('user_details_cache', backend='sqlite', expire_after=604830)



## User Data Collection Stuff via Twitter API
import tweepy
from tweepy import OAuthHandler

# Temporarily using Amogh's Keys
CONSUMER_KEY = 'hcwZphJGh2jFUn2y03hBLyQyx'
CONSUMER_SECRET = 'ofi9o68Q7jCY3tAo00P0ervLcpHdVqlfNy1pbL7KQ3cGDthCTa'
ACCESS_KEY = '4002557415-MSa5YxOKeKOwu009Q8No35JA3fzLYvnvGIcrk3x'
ACCESS_SECRET = 'Hn74FZiy9usvOi0yhNSS1Kbn33BTMxRTLDlAeDWkVYWfg'

auth = OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
api = tweepy.API(auth)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
## End User Data Collection Stuff via Twitter API


# Custom tablename Stuff
# from spiders.TweetCrawler import *
import spiders.TweetCrawler

# Postgres Stuff
import psycopg2
from sqlalchemy import create_engine

# for mysql
import mysql.connector
from mysql.connector import errorcode

from TweetScraper.items import Tweet, User
from TweetScraper.utils import mkdirs

logger = logging.getLogger(__name__)


## Postgres Pipeline
class SaveToPostgresPipeline(object):

    ''' pipeline that save data to Postgres '''
    def __init__(self):
        self.connection = psycopg2.connect(dbname = settings['PGDB'], user = settings['PGUSER'], password = settings['PGPASS'])
        self.cursor = self.connection.cursor()
        self.clock = datetime.now()
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.tablename = ((str(spiders.TweetCrawler.keywords)[:31]).replace(" ", "_").replace(":", "_").replace("#", "").replace("-", "_"))+ "_at_" +(str(start_time).replace(" ", "_").replace(":", "_").replace("-", "_").replace(".", "_"))
        global tabname
        tabname = self.tablename
        global connection
        connection = self.connection
        global cursor
        cursor = self.cursor

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " + self.tablename + "(\
            timestamp TIMESTAMP,\
            id BIGINT PRIMARY KEY,\
            user_id BIGINT,\
            followers_count BIGINT,\
            url VARCHAR(2048),\
            username VARCHAR(36),\
            text VARCHAR(2048),\
            is_reply BOOLEAN,\
            is_reply_to VARCHAR(100),\
            is_retweet BOOLEAN,\
            reply_count INT,\
            favorite_count INT,\
            retweet_count INT\
        )")
        self.connection.commit()
        self.insertTweetStatement = "INSERT INTO " + self.tablename + " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(settings['POSTGRES_TWEET_TABLE'])

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            x = requests.get("https://cdn.syndication.twimg.com/widgets/followbutton/info.json?user_ids=" + (item['user_id']))
            ## Print if taken from Requests_Cache
            # print "Using Cache: " + str(x.from_cache)
            y = x.json()
            follower_count = y[0]['followers_count']

            self.cursor.execute(self.insertTweetStatement, (
            item['datetime'],
            item['ID'],
            item['user_id'],
            follower_count,
            item['url'],
            item['usernameTweet'],
            item['text'],
            item['is_reply'],
            item['is_reply_to'],
            item['is_retweet'],
            item['nbr_reply'],
            item['nbr_favorite'],
            item['nbr_retweet']
          ))
        self.connection.commit()

## =================================================================================================================== ##
## ============================================ END POSTGRES PIPELINE ================================================ ##
## =================================================================================================================== ##
