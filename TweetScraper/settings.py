# -*- coding: utf-8 -*-

# !!! # Crawl responsibly by identifying yourself (and your website/e-mail) on the user-agent
USER_AGENT = 'TweetScraper'

# settings for spiders
BOT_NAME = 'TweetScraper'
LOG_LEVEL = 'INFO'
DOWNLOAD_HANDLERS = {'s3': None,} # from http://stackoverflow.com/a/31233576/2297751, TODO

SPIDER_MODULES = ['TweetScraper.spiders']
NEWSPIDER_MODULE = 'TweetScraper.spiders'
ITEM_PIPELINES = {
    # 'TweetScraper.pipelines.SaveToFilePipeline':100,
    # 'TweetScraper.pipelines.SaveToMongoPipeline':100, # replace `SaveToFilePipeline` with this to use MongoDB
    # 'TweetScraper.pipelines.SavetoMySQLPipeline':100, # replace `SaveToFilePipeline` with this to use MySQL
    'TweetScraper.pipelines.SaveToPostgresPipeline':100,
}

# settings for where to save data on disk
SAVE_TWEET_PATH = './Data/tweet/'
SAVE_USER_PATH = './Data/user/'

# settings for mongodb
MONGODB_SERVER = "127.0.0.1"
MONGODB_PORT = 27017
MONGODB_DB = "TweetScraper"        # database name to save the crawled data
MONGODB_TWEET_COLLECTION = "tweet" # collection name to save tweets
MONGODB_USER_COLLECTION = "user"   # collection name to save users

# Settings for Postgresql
PGSERVER = "127.0.0.1"
PGPORT = 5432
PGUSER = "postgres"
PGPASS = "password123"
PGDB = "TweetScraper"
POSTGRES_TWEET_TABLE = "tweet" # collection name to save tweets
POSTGRES_USER_TABLE  = "user"   # collection name to save users
