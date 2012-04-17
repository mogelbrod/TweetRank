#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from xml.dom.minidom import parseString
from ProxiedRequester import ProxiedRequester
from TweetsWarehouse import TweetsWarehouse
from UsersWarehouse import UsersWarehouse
from UsersFrontier import UsersFrontier
from Tweet import Tweet
from time import sleep
from random import randint
from threading import Thread
from OsUtils import get_utc_time

class TwitterCrawler:            
    def __init__(self, datadir, crawl_period = 60, workers=2):
        self.frontier = UsersFrontier(datadir + '/frontier.txt')
        self.tweets = TweetsWarehouse(datadir + '/tweets/')
        self.users  = UsersWarehouse(datadir + '/users/')
        self.requester = ProxiedRequester(datadir + '/proxies.txt')
        self.crawl_period = crawl_period
        print('Master crawler ready!')

    def crawl(self, nworkers=1):
        workers = [self.CrawlerWorker(self.frontier, self.tweets, self.users, self.requester, self.crawl_period)
                   for i in range(nworkers)]
        for w in workers: w.start()
        for w in workers: w.join()

    class CrawlerWorker(Thread):
        def __init__(self, frontier, tweets, users, requester, crawl_period):
            Thread.__init__(self)
            self.daemon = True
            self.frontier = frontier
            self.tweets = tweets
            self.users  = users
            self.requester = requester
            self.crawl_period = crawl_period
            
        def run(self):
            print('Thread %d: starting...' % self.ident)
            self.crawl()
                

        def get_twitter_multipage_query(self, query, count = 200, maxpages=16):
            page = 1
            result = []
            while True:
                (status, xmldata, wait_time) = self.requester.request("%s&count=%d&page=%d" % (query, count, page))

                if status == -1 and wait_time == None:
                    return None         # No proxy is working, abort!
                elif status == -1:
                    sleep(wait_time)    # Wait until some server works
                elif status == -2:
                    return []           # Query error (for instance: user has protected tweets)
                elif status == 200:
                    try:
                        domdata = parseString(xmldata.decode('utf-8'))
                        tweets = domdata.getElementsByTagName('status')
                        for tweet in tweets:
                            result.append(Tweet(tweet))
                        if len(tweets) < count or page == maxpages: return result # OK
                        else: page = page + 1      # Next page    
                    except:
                        # In the case of XML parsing errors, ignore these tweets
                        if page == maxpages: return result
                        else: page = page + 1

        def get_twitter_singlepage_query(self, query):
            while True:
                result = []
                (status, xmldata, wait_time) = self.requester.request(query)

                if status == -1 and wait_time == None:
                    return None         # No proxy is working, abort!
                elif status == -1:
                    sleep(wait_time)    # Wait until some server works
                elif status == -2:
                    return []           # Query error (for instance: user has protected tweets)
                elif status == 200:
                    try:
                        domdata = parseString(xmldata.decode('utf-8'))
                        tweets = domdata.getElementsByTagName('status')
                        for tweet in tweets:
                            result.append(Tweet(tweet))
                    finally:
                        pass # Ignore parse exception
                    return result # OK

        def get_user_tweets(self, user_id, since_id = None):
            query = 'http://api.twitter.com/1/statuses/user_timeline.xml?id=%d&exclude_replies=false&include_rts=true&include_entities=true' % user_id
            if since_id != None:
                query = query + ('&since_id=%d' % since_id)
            return self.get_twitter_multipage_query(query)

        def get_user_friends(self, user_id):
            query = 'http://api.twitter.com/1/friends/ids.xml?id=%d' % user_id

            curr_cursor = -1
            prev_cursor = 0
            result = []
            while (curr_cursor != prev_cursor):
                (status, xmldata, wait_time) = self.requester.request("%s&cursor=%d" % (query, curr_cursor))
                if status == -1 and wait_time == None:
                    return None         # No proxy is working, abort!
                elif status == -1:
                    sleep(wait_time)    # Wait until some server works
                elif status == -2:
                    return set([])      # Query error (for instance: user has protected followers)
                elif status == 200:
                    data = parseString(xmldata.decode('utf-8'))
                    curr_cursor = int(data.getElementsByTagName('next_cursor')[0].firstChild.data)
                    prev_cursor = int(data.getElementsByTagName('previous_cursor')[0].firstChild.data)
                    for elem in data.getElementsByTagName('id'):
                        result.append(int(elem.firstChild.data))

            return set(result) # OK


        def crawl(self, maxusers = 500, maxtweets = None):
            while len(self.frontier) > 0 and (maxtweets == None or (maxtweets != None and len(self.tweets) < maxtweets)):
                next_crawl_time,last_tweet_id,user = self.frontier.pop()
                print('Thread %d: Crawling %d...' % (self.ident,user))

                time_diff = next_crawl_time - get_utc_time()
                if time_diff > 0 : # Suspend
                    sleep(time_diff)

                # Fetch tweets and new followers
                tweets_by_user = self.get_user_tweets(user, last_tweet_id)
                if tweets_by_user == None:
                    print ('Thread %d: ABORTED CRAWLING TWEETS OF %s' % (self.ident, user))
                    self.frontier.push(user, last_tweet_id, next_crawl_time)
                    break # Abort

                new_users = self.get_user_friends(user)
                if new_users == None:
                    print ('Thread %d: ABORTED CRAWLING TWEETS OF %s' % (self.ident, user))
                    self.frontier.push(user, last_tweet_id, next_crawl_time)
                    break # Abort
                    

                friends = [nu for nu in new_users]

                hashtags=[]
                for tweet in tweets_by_user:
                    # Save the fetched tweet
                    self.tweets.add(tweet)
                    hashtags.extend(tweet.get_hashtags())
                    if tweet.get_tweet_id() > last_tweet_id:
                        last_tweet_id = tweet.get_tweet_id()

                    # Save the original retweeted status, if any
                    retweeted = tweet.get_retweeted_status()
                    if retweeted != None:
                        self.tweets.add(retweeted)

                    # Extract the users mentioned
                    new_users.update(tweet.get_mentioned_ids())

                # Extend the frontier
                for nu in new_users:
                    if maxusers != None and len(self.frontier)+1 >= maxusers: break
                    if nu not in (self.frontier) and nu != user:
                        self.frontier.push(nu, 1, get_utc_time())

                # Add the current user again, to crawl new updates in the future
                self.frontier.push(user, last_tweet_id, get_utc_time() + self.crawl_period)
                self.users.add(user, friends, hashtags)
            

tc = TwitterCrawler('../data/')
tc.crawl()
    
