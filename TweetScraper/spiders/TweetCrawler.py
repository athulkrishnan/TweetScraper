from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.conf import settings
from scrapy import http
from scrapy.shell import inspect_response  # for debugging
import re
from .. import pipelines as ppl
import json
import time
import logging
from decimal import Decimal

try:
    from urllib import quote  # Python 2.X
except ImportError:
    from urllib.parse import quote  # Python 3+

from datetime import datetime

from TweetScraper.items import Tweet, User

logger = logging.getLogger(__name__)


class TweetScraper(CrawlSpider):
    name = 'TweetScraper'
    allowed_domains = ['twitter.com']

    def __init__(self, query='', lang='', crawl_user=False, top_tweet=False):

        self.query = query
        global keywords
        keywords = self.query
        self.url = "https://twitter.com/i/search/timeline?l={}".format(lang)


        if not top_tweet:
            self.url = self.url + "&f=tweets"

        self.url = self.url + "&q=%s&src=typed&max_position=%s"

        self.crawl_user = crawl_user

    def start_requests(self):
        url = self.url % (quote(self.query), '')
        yield http.Request(url, callback=self.parse_page)

    def parse_page(self, response):
        # inspect_response(response, self)
        # handle current page
        data = json.loads(response.body.decode("utf-8"))
        for item in self.parse_tweets_block(data['items_html']):
            yield item

        # get next page
        min_position = data['min_position']
        url = self.url % (quote(self.query), min_position)
        yield http.Request(url, callback=self.parse_page)

    def parse_tweets_block(self, html_page):
        page = Selector(text=html_page)

        ### for text only tweets
        items = page.xpath('//li[@data-item-type="tweet"]/div')
        for item in self.parse_tweet_item(items):
            yield item

    def parse_tweet_item(self, items):
        for item in items:
            try:
                tweet = Tweet()

                tweet['usernameTweet'] = item.xpath('.//span[@class="username u-dir u-textTruncate"]/b/text()').extract()[0]

                ID = item.xpath('.//@data-tweet-id').extract()
                if not ID:
                    continue
                tweet['ID'] = ID[0]

                ### get text content
                tweet['text'] = ' '.join(
                    item.xpath('.//div[@class="js-tweet-text-container"]/p//text()').extract()).replace(' # ',
                                                                                                        '#').replace(
                    ' @ ', '@')
                if tweet['text'] == '':
                    # If there is not text, we ignore the tweet
                    continue

                ### get meta data
                tweet['url'] = item.xpath('.//@data-permalink-path').extract()[0]

                nbr_retweet = item.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_retweet:
                    tweet['nbr_retweet'] = int(nbr_retweet[0])
                else:
                    tweet['nbr_retweet'] = 0

                nbr_favorite = item.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_favorite:
                    tweet['nbr_favorite'] = int(nbr_favorite[0])
                else:
                    tweet['nbr_favorite'] = 0

                nbr_reply = item.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_reply:
                    tweet['nbr_reply'] = int(nbr_reply[0])
                else:
                    tweet['nbr_reply'] = 0

                # original tweet the response is reply to fix
                def conversation():
                    is_reply_to = item.xpath('.//@data-conversation-id').extract()
                    IDz = item.xpath('.//@data-tweet-id').extract()
                    if is_reply_to[0] == IDz[0]:
                        tweet['is_reply_to'] = "No"
                    else:
                        tweet['is_reply_to'] = is_reply_to[0]

                conversation()

                tweet['datetime'] = datetime.fromtimestamp(int(
                    item.xpath('.//div[@class="stream-item-header"]/small[@class="time"]/a/span/@data-time').extract()[
                        0])).strftime('%Y-%m-%d %H:%M:%S')

                # DISABLED
                ### get photo
                # has_cards = item.xpath('.//@data-card-type').extract()
                # if has_cards and has_cards[0] == 'photo':
                #     tweet['has_image'] = True
                #     tweet['images'] = item.xpath('.//*/div/@data-image-url').extract()
                # elif has_cards:
                #     logger.debug('Not handle "data-card-type":\n%s' % item.xpath('.').extract()[0])
                #
                # ### get animated_gif
                # has_cards = item.xpath('.//@data-card2-type').extract()
                # if has_cards:
                #     if has_cards[0] == 'animated_gif':
                #         tweet['has_video'] = True
                #         tweet['videos'] = item.xpath('.//*/source/@video-src').extract()
                #     elif has_cards[0] == 'player':
                #         tweet['has_media'] = True
                #         tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                #     elif has_cards[0] == 'summary_large_image':
                #         tweet['has_media'] = True
                #         tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                #     elif has_cards[0] == 'amplify':
                #         tweet['has_media'] = True
                #         tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                #     elif has_cards[0] == 'summary':
                #         tweet['has_media'] = True
                #         tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                #     elif has_cards[0] == '__entity_video':
                #         pass  # TODO
                #         # tweet['has_media'] = True
                #         # tweet['medias'] = item.xpath('.//*/div/@data-src').extract()
                #     else:  # there are many other types of card2 !!!!
                #         logger.debug('Not handle "data-card2-type":\n%s' % item.xpath('.').extract()[0])

                is_reply = item.xpath('.//div[@class="ReplyingToContextBelowAuthor"]').extract()
                tweet['is_reply'] = is_reply != []

                is_retweet = item.xpath('.//span[@class="js-retweet-text"]').extract()
                tweet['is_retweet'] = is_retweet != []

                tweet['user_id'] = item.xpath('.//@data-user-id').extract()[0]
                yield tweet

                if self.crawl_user:
                    ### get user info
                    user = User()
                    user['ID'] = tweet['user_id']
                    user['name'] = item.xpath('.//@data-name').extract()[0]
                    user['screen_name'] = item.xpath('.//@data-screen-name').extract()[0]
                    user['avatar'] = \
                        item.xpath('.//div[@class="content"]/div[@class="stream-item-header"]/a/img/@src').extract()[0]
                    yield user
            except:
                logger.error("Error tweet:\n%s" % item.xpath('.').extract()[0])
                # raise

    def extract_one(self, selector, xpath, default=None):
        extracted = selector.xpath(xpath).extract()
        if extracted:
            return extracted[0]
        return default

    def closed(self, reason):
        # Total Tweets Count
        print("##################################################################################################################")
        print("STATISTICS")

        ppl.cursor.execute("SELECT count(url) AS total FROM " + ppl.tabname)
        ppl.connection.commit()
        total_tweets = (str(ppl.cursor.fetchall()).replace("L,","").replace("(","").replace(")","").replace("[","").replace("]","").replace(" ",""))
        print "Tweets: " + str(total_tweets)

        # Retweets Count
        ppl.cursor.execute("SELECT SUM(retweet_count) AS total_retweets FROM " + ppl.tabname)
        ppl.connection.commit()
        retweets = (str(ppl.cursor.fetchall()).replace("L,","").replace("(","").replace(")","").replace("[","").replace("]","").replace(" ",""))
        print "Retweets: " + str(retweets)

        # Favorites Count
        ppl.cursor.execute("SELECT SUM(favorite_count) AS fav FROM " + ppl.tabname)
        ppl.connection.commit()
        favorites = (str(ppl.cursor.fetchall()).replace("L,","").replace("(","").replace(")","").replace("[","").replace("]","").replace(" ",""))
        print "Favorites: " + str(favorites)

        # Replies Count
        ppl.cursor.execute("SELECT SUM(reply_count) AS replies FROM " + ppl.tabname)
        ppl.connection.commit()
        replies = (str(ppl.cursor.fetchall()).replace("L,","").replace("(","").replace(")","").replace("[","").replace("]","").replace(" ",""))
        print "Replies: " + str(replies)

        # Replies Count
        ## Implement method to count only DISTINCT users -- NO DOUBLE COUNTING
        ppl.cursor.execute("SELECT SUM(followers_count) AS fc FROM " + ppl.tabname)
        ppl.connection.commit()
        total_followers = (str(ppl.cursor.fetchall()).replace("L,","").replace("(","").replace(")","").replace("[","").replace("]","").replace(" ","").replace("Decimal","").replace("'","").replace(",",""))
        print "Total Followers: " + str(total_followers)

        ### Make Values Integers
        try:
            int_tt = int(str(total_tweets))
        except:
            int_tt = int(0)
        try:
            int_tf = int(str(total_followers))
        except:
            int_tf = int(0)
        try:
            int_f = int(str(favorites))
        except:
            int_f = int(0)

        reach = int_tt*int_tf
        print "Reach: " + str(reach)
        try:
            fav_follower_ratio_stager = (float(int_f))/(float(int_tf))
        except:
            fav_follower_ratio = int(0)
        try:
            fav_follower_ratio = ((fav_follower_ratio_stager))
        except:
            fav_follower_ratio = int(0)
        print "Fav/Follower Ratio " + str(fav_follower_ratio)

        print("##################################################################################################################")

        ppl.cursor.execute("CREATE TABLE IF NOT EXISTS " + ppl.tabname + "_Stats" + "(\
            total_tweets VARCHAR,\
            retweets VARCHAR,\
            favorites VARCHAR,\
            replies VARCHAR,\
            reach VARCHAR,\
            fav_follower_ratio VARCHAR\
        )")
        ppl.connection.commit()
        ppl.cursor.execute("INSERT INTO " + ppl.tabname + "_Stats" + " VALUES(%s, %s, %s, %s, %s, %s)", (total_tweets, retweets, favorites, replies, reach, fav_follower_ratio))
        ppl.connection.commit()
