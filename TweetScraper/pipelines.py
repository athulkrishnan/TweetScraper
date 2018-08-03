# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import logging
import pymongo
import json
import os
import time
import datetime

# Custom tablename Stuff
from spiders.TweetCrawler import *

# Postgres Stuff
import psycopg2
from sqlalchemy import create_engine

# for mysql
import mysql.connector
from mysql.connector import errorcode

from TweetScraper.items import Tweet, User
from TweetScraper.utils import mkdirs


logger = logging.getLogger(__name__)



class SaveToMongoPipeline(object):
    ''' pipeline that save data to mongodb '''
    def __init__(self):
        connection = pymongo.MongoClient(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        db = connection[settings['MONGODB_DB']]
        self.tweetCollection = db[settings['MONGODB_TWEET_COLLECTION']]
        self.userCollection = db[settings['MONGODB_USER_COLLECTION']]
        self.tweetCollection.ensure_index([('ID', pymongo.ASCENDING)], unique=True, dropDups=True)
        self.userCollection.ensure_index([('ID', pymongo.ASCENDING)], unique=True, dropDups=True)


    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            dbItem = self.tweetCollection.find_one({'ID': item['ID']})
            if dbItem:
                pass # simply skip existing items
                ### or you can update the tweet, if you don't want to skip:
                # dbItem.update(dict(item))
                # self.tweetCollection.save(dbItem)
                # logger.info("Update tweet:%s"%dbItem['url'])
            else:
                self.tweetCollection.insert_one(dict(item))
                logger.debug("Add tweet:%s" %item['url'])

        elif isinstance(item, User):
            dbItem = self.userCollection.find_one({'ID': item['ID']})
            if dbItem:
                pass # simply skip existing items
                ### or you can update the user, if you don't want to skip:
                # dbItem.update(dict(item))
                # self.userCollection.save(dbItem)
                # logger.info("Update user:%s"%dbItem['screen_name'])
            else:
                self.userCollection.insert_one(dict(item))
                logger.debug("Add user:%s" %item['screen_name'])

        else:
            logger.info("Item type is not recognized! type = %s" %type(item))

## Postgres Pipeline
class SaveToPostgresPipeline(object):

    ''' pipeline that save data to mongodb '''
    def __init__(self):
        self.connection = psycopg2.connect(dbname = settings['PGDB'], user = settings['PGUSER'], password = settings['PGPASS'])
        self.cursor = self.connection.cursor()
        self.clock = datetime.now()
        self.tablename = (str(keywords).replace(" ", "_"))+(str(datetime.now()).replace(" ", "_").replace(":", "_").replace("-", "_").replace(".", "_"))
        # self.tablename = keywords.replace(" ", "_")

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

        # elif isinstance(item, User):
        #   logger.info("User storage is not implemented!")
        # else:
        #     logger.info("Item type is not recognized! type = %s" %type(item))

class SaveToFilePipeline(object):
    ''' pipeline that save data to disk '''
    def __init__(self):
        self.saveTweetPath = settings['SAVE_TWEET_PATH']
        self.saveUserPath = settings['SAVE_USER_PATH']
        mkdirs(self.saveTweetPath) # ensure the path exists
        mkdirs(self.saveUserPath)


    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            savePath = os.path.join(self.saveTweetPath, item['ID'])
            if os.path.isfile(savePath):
                pass # simply skip existing items
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.info("Update tweet:%s"%dbItem['url'])
            else:
                self.save_to_file(item,savePath)
                logger.debug("Add tweet:%s" %item['url'])

        elif isinstance(item, User):
            savePath = os.path.join(self.saveUserPath, item['ID'])
            if os.path.isfile(savePath):
                pass # simply skip existing items
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.info("Update user:%s"%dbItem['screen_name'])
            else:
                self.save_to_file(item, savePath)
                logger.debug("Add user:%s" %item['screen_name'])

        else:
            logger.info("Item type is not recognized! type = %s" %type(item))


    def save_to_file(self, item, fname):
        ''' input:
                item - a dict like object
                fname - where to save
        '''
        with open(fname,'w') as f:
            json.dump(dict(item), f)
