# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import logging
import pymongo
import json
import os
import time
from datetime import datetime

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
        self.tablename = (str(spiders.TweetCrawler.keywords).replace(" ", "_").replace(":", "_").replace("#", "").replace("-", "_"))+ "_at_" +(str(start_time).replace(" ", "_").replace(":", "_").replace("-", "_").replace(".", "_"))
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
        self.insertTweetStatement = "INSERT INTO " + self.tablename + " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(settings['POSTGRES_TWEET_TABLE'])

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            self.cursor.execute(self.insertTweetStatement, (
            item['datetime'],
            item['ID'],
            item['user_id'],
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

    # Experimental Code for Counting Tweets and Retweets
    # follower count lookup


    # def countemup(self, item):
    #     self.cursor.execute("SELECT SUM(retweet_count) AS total_retweets FROM " + self.tablename)
    #     self.connection.commit()
    #     print self.cursor.fetchall()
        # resp = api.lookup_users(user_ids=['1123728482,5539932'])
        #
        # for user in resp:
        #     print user.followers_count
    # self.cursor.execute("SELECT SUM(retweet_count) AS total_retweets FROM" + self.tablename)
    # def count_items(self, item, spider):
    # self.connection.commit()
    #     print str(cursor.fetchone()).replace("L,","").replace("(","").replace(")","")
        # elif isinstance(item, User):
        #   logger.info("User storage is not implemented!")
        # else:
        #     logger.info("Item type is not recognized! type = %s" %type(item))

## =================================================================================================================== ##
## ============================================ END POSTGRES PIPELINE ================================================ ##
## =================================================================================================================== ##
