# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import logging
import pymongo
import json
import os
import time

#Postgres Stuff
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

class SaveToPostgresPipeline(object):
    ''' Pipeline that saves Tweets to Postgres Database'''
    def __init__(self):
        self.engine = create_engine('postgresql+psycopg2://' + settings['PGUSER'] + ':' + settings['PGPASS'] + '@' + settings['PGSERVER'] + '/' + settings['PGDB'])
        # conn = psycopg2.connect("dbname=" + settings['PGDB'] + " user= "+ settings['PGUSER'] + " password=" + settings['PGPASS'] +"")
        self.conn = psycopg2.connect("dbname=" + settings['PGDB'] + " user= "+ settings['PGUSER'] + " password=" + settings['PGPASS'] +"")
        self.cursor = self.conn.cursor()
        self.table_name = "Hello1"+str(time.time()).replace(".", "")

        create_table_query = "CREATE TABLE " + self.table_name + " (\
                ID CHAR(20) NOT NULL,\
                url VARCHAR(140) NOT NULL,\
                datetime VARCHAR(22) NOT NULL,\
                text VARCHAR(280) NOT NULL,\
                user_id CHAR(20) NOT NULL,\
                usernameTweet VARCHAR(20) NOT NULL,\
                nbr_favorite CHAR(100),\
                is_retweet CHAR(100),\
                nbr_retweet CHAR(100),\
                nbr_reply CHAR(100),\
                is_reply CHAR(100),\
                is_reply_to CHAR(100),\
                medias CHAR(100),\
                has_media CHAR(100)\
                )"

        print "Creating table..."
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def find_one(self, trait, value):
        select_query = "SELECT " + trait + " FROM " + self.table_name + " WHERE " + trait + " = " + value + ";"
        val = self.cursor.execute(select_query)

        if (val == None):
            return False
        else:
            return True

    def check_vals(self, item):
        ID = item['ID']
        url = item['url']
        datetime = item['datetime']
        text = item['text']
        user_id = item['user_id']
        username = item['usernameTweet']
        
        nbr_retweet = item['nbr_retweet']
        nbr_favorite = item['nbr_favorite']
        nbr_reply = item['nbr_reply']
        is_reply = item['is_reply']
        is_reply_to = item['is_reply_to']
        is_retweet = item['is_retweet']
        has_media = item['has_media']
        medias = item['medias']



        if (ID is None):
            return False
        elif (user_id is None):
            return False
        elif (url is None):
            return False
        elif (text is None):
            return False
        elif (username is None):
            return False
        elif (datetime is None):
            return False
        elif (nbr_retweet is None):
            return False
        elif (nbr_favorite is None):
            return False
        elif (nbr_reply is None):
            return False
        elif (is_reply is None):
            return False
        elif (is_reply_to is None):
            return False
        elif (is_retweet is None):
            return False
        elif (has_media is None):
            return False
        elif (medias is None):
            return False
        else:
            return True

    def insert_one(self, item):
        ret = self.check_vals(item)

        if not ret:
            return None

        ID = item['ID']
        url = item['url']
        datetime = item['datetime']
        text = item['text']
        user_id = item['user_id']
        username = item['usernameTweet']
        nbr_retweet = item['nbr_retweet']
        nbr_favorite = item['nbr_favorite']
        nbr_reply = item['nbr_reply']
        is_reply = item['is_reply']
        is_reply_to = item['is_reply_to']
        is_retweet = item['is_retweet']
        has_media = item['has_media']
        medias = item['medias']

        insert_query =  "INSERT INTO " + self.table_name + " (ID, url, datetime, text, user_id, usernameTweet, \
        nbr_retweet, nbr_favorite, nbr_favorite, nbr_reply, is_reply, is_reply_to, is_retweet, has_media, medias )"
        insert_query += " VALUES ( '" + ID + "', '" + url + "', '"
        insert_query += datetime + "', '" + text + "', '" + user_id + "', '" + username + "', '"
        insert_query += nbr_retweet + "', '" + nbr_favorite + "', '" + nbr_reply + "', '" + is_reply + "', '"
        insert_query += is_reply_to + "', '" + is_retweet + "', '" + has_media + "', '" + medias + "' )"

        
        try:
            print "Inserting..."
            self.cursor.execute(insert_query)
            self.conn.commit()
        except mysql.connector.Error as err:
            print err.msg
        else:
            print "Successfully inserted."
            self.conn.commit()

        # print "Inserting..."
        # self.cursor.execute(insert_query)
        # self.conn.commit()

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            dbItem = self.find_one('user_id', item['ID'])
            if dbItem:
                pass
            else:
                self.insert_one(dict(item))
                logger.debug("Add Tweet:%s" %item['url'])


class SavetoMySQLPipeline(object):

    ''' pipeline that save data to mysql '''
    def __init__(self):
        # connect to mysql server
        user = raw_input("MySQL User: ")
        pwd = raw_input("Password: ")
        self.cnx = mysql.connector.connect(user=user, password=pwd,
                                host='localhost',
                                database='tweets', buffered=True)
        self.cursor = self.cnx.cursor()
        self.table_name = raw_input("Table name: ")
        create_table_query =   "CREATE TABLE `" + self.table_name + "` (\
                `ID` CHAR(20) NOT NULL,\
                `url` VARCHAR(140) NOT NULL,\
                `datetime` VARCHAR(22),\
                `text` VARCHAR(280),\
                `user_id` CHAR(20) NOT NULL,\
                `usernameTweet` VARCHAR(20) NOT NULL\
                )"

        try:
            print "Creating table..."
            self.cursor.execute(create_table_query)
        except:
            print err.msg
        else:
            self.cnx.commit()
            print "Successfully created table."

    def find_one(self, trait, value):
        select_query =  "SELECT " + trait + " FROM " + self.table_name + " WHERE " + trait + " = " + value + ";"
        try:
            val = self.cursor.execute(select_query)
        except mysql.connector.Error as err:
            return False
        
        if (val == None):
            return False
        else:
            return True

    def check_vals(self, item):
        ID = item['ID']
        url = item['url']
        datetime = item['datetime']
        text = item['text']
        user_id = item['user_id']
        username = item['usernameTweet']

        if (ID is None):
            return False
        elif (user_id is None):
            return False
        elif (url is None):
            return False
        elif (text is None):
            return False
        elif (username is None):
            return False
        elif (datetime is None):
            return False
        else:
            return True


    def insert_one(self, item):
        ret = self.check_vals(item)

        if not ret:
            return None

        ID = item['ID']
        user_id = item['user_id']
        url = item['url']
        text = item['text']
        username = item['usernameTweet']
        datetime = item['datetime']

        insert_query =  "INSERT INTO " + self.table_name + " (ID, url, datetime, text, user_id, usernameTweet )"
        insert_query += " VALUES ( '" + ID + "', '" + url + "', '"
        insert_query += datetime + "', '" + text + "', '" + user_id + "', '" + username + "' )"

        try:
            print "Inserting..."
            self.cursor.execute(insert_query)
        except mysql.connector.Error as err:
            print err.msg
        else:
            print "Successfully inserted."
            self.cnx.commit()


    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            dbItem = self.find_one('user_id', item['ID'])
            if dbItem:
                pass # simply skip existing items
                ### or you can update the tweet, if you don't want to skip:
                # dbItem.update(dict(item))
                # self.tweetCollection.save(dbItem)
                # logger.info("Update tweet:%s"%dbItem['url'])
            else:
                self.insert_one(dict(item))
                logger.debug("Add tweet:%s" %item['url'])


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
