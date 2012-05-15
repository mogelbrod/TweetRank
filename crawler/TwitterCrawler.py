#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from ProxiedRequester import ProxiedRequester
from UsersFrontier import UsersFrontier
from ServicesNotifier import ServicesNotifier
from TweetArrayParser import TweetArrayParser
from Tweet import Tweet
from EscapeXMLIllegalCharEntities import EscapeXMLIllegalCharEntities
from OsUtils import get_utc_time
from Backup import Backup

import sys
import xml.sax
import xml.dom.minidom
from time import sleep
from random import randint
from threading import Thread

class TwitterCrawler:
    def __init__(self, datadir):
        self.frontier = UsersFrontier(datadir + '/frontier.txt')
        self.requester = ProxiedRequester(datadir + '/proxies.txt')
        self.notif = ServicesNotifier()
        self.backup = Backup(datadir)

    def crawl(self, nworkers=1, crawl_period = 3600, max_depth = None, max_tweets = None, max_users = None):
        workers = [self.CrawlerWorker(self.frontier, self.requester, self.notif, self.backup, \
                                      crawl_period, max_depth, max_tweets, max_users) for i in range(nworkers)]
        for w in workers: w.start()
        for w in workers: w.join()

    class CrawlerWorker(Thread):
        def __init__(self, frontier, requester, notifier, backup, \
                     crawl_period = 3600, max_depth = None, \
                     max_tweets = None, max_users = None):
            Thread.__init__(self)
            self.daemon = True
            self.frontier = frontier
            self.requester = requester
            self.notif = notifier
            self.backup = backup
            self.crawl_period = crawl_period
            self.max_depth = max_depth
            self.max_tweets = max_tweets
            self.max_users = max_users

        def run(self):
            print('Thread %d: starting...' % self.ident)
            self.crawl()

        def get_user_tweets(self, user_id, since_id = None, count=200, maxpages=16):
            def tweet_callback(tw, result, counter):
                self.notif.notify_tweet(tw)
                result.append(tw)
                counter[0] = counter[0] + 1

            query = 'http://api.twitter.com/1/statuses/user_timeline.xml?id=%d&exclude_replies=false&include_rts=true&include_entities=true' % user_id
            if since_id is not None:
                query = query + ('&since_id=%d' % since_id)

            page = 1
            result = []
            tw_counter = []
            tw_counter.append(0)
            while True:
                (status, xmldata, wait_time) = self.requester.request("%s&count=%d&page=%d" % (query, count, page))

                if status == 999 and wait_time is None:
                    return None         # No proxy is working, abort!
                elif status == 999:
                    time_diff = wait_time - get_utc_time()
                    print 'Sleeping %d seconds...' % time_diff
                    sleep(time_diff)    # Wait until some server works
                elif status != 200:
                    return []           # Query error (for instance: user has protected tweets)
                else:
                    tw_counter[0] = 0
                    escaped_xml = EscapeXMLIllegalCharEntities(xmldata)
                    self.backup.store_tweets(user_id, escaped_xml)
                    xml.sax.parseString(escaped_xml, TweetArrayParser(lambda tw: tweet_callback(tw, result, tw_counter)))
                    if tw_counter[0] == 0 or page == maxpages: return result # OK
                    else: page = page + 1      # Next page

        def get_user_friends(self, user_id):
            query = 'http://api.twitter.com/1/friends/ids.xml?id=%d' % user_id

            curr_cursor = -1
            prev_cursor = 0
            result = []
            while (curr_cursor != prev_cursor):
                (status, xmldata, wait_time) = self.requester.request("%s&cursor=%d" % (query, curr_cursor))
                if status == 999 and wait_time is None:
                    return None         # No proxy is working, abort!
                elif status == 999:
                    time_diff = wait_time - get_utc_time()
                    print 'Sleeping %d seconds...' % time_diff
                    sleep(wait_time)    # Wait until some server works
                elif status != 200:
                    return set([])      # Query error (for instance: user has protected followers)
                else:
                    data = xml.dom.minidom.parseString(xmldata.decode('utf-8'))
                    curr_cursor = int(data.getElementsByTagName('next_cursor')[0].firstChild.data)
                    prev_cursor = int(data.getElementsByTagName('previous_cursor')[0].firstChild.data)
                    for elem in data.getElementsByTagName('id'):
                        result.append(int(elem.firstChild.data))

            return set(result) # OK


        def crawl(self):
            while len(self.frontier) > 0:
                next_crawl_time,depth,last_tweet_id,user = self.frontier.pop()

                time_diff = next_crawl_time - get_utc_time()
                if time_diff > 0 : # Suspend
                    print 'Sleeping %d seconds...' % time_diff
                    sleep(time_diff)

                print ('Crawling user: %d' % user)

                # Fetch tweets and new followers
                tweets_by_user = self.get_user_tweets(user, last_tweet_id)
                if tweets_by_user is None:
                    print ('Thread %d: ABORTED CRAWLING TWEETS OF %s' % (self.ident, user))
                    break # Abort

                friends = self.get_user_friends(user)
                if friends is None:
                    print ('Thread %d: ABORTED CRAWLING TWEETS OF %s' % (self.ident, user))
                    break # Abort

                new_users = set([nu for nu in friends])
                hashtags  = []

                # Traverse the fetched tweets to get retweeted statuses,
                # mentioned users, hashtags, etc.
                for tweet in tweets_by_user:
                    # Add hashtags
                    hashtags.extend(tweet.hashtags)

                    # Update the largest tweet_id for current user
                    if tweet.id > last_tweet_id: last_tweet_id = tweet.id

                    # Add retweet owner to the queue
                    if tweet.retweeted_status is not None:
                        new_users.add(tweet.retweeted_status.user.id)

                    # Extract the users mentioned
                    new_users.update(tweet.mentions)

                # Store user friends & used hashtags
                self.notif.notify_user_friends(user,friends)
                self.notif.notify_user_hashtags(user,hashtags)

                # Add the current user again, to crawl new updates in the future
                self.frontier.push(user, last_tweet_id, get_utc_time() + self.crawl_period, depth)

                # Extend the frontier
                for nu in new_users:
                    if (self.max_users is not None and len(self.frontier) >= self.max_users) or \
                       (self.max_depth is not None and depth > self.max_depth): break
                    if nu not in self.frontier:
                        self.frontier.push(nu, 1, get_utc_time(), depth+1)


tc = TwitterCrawler('../data/')
tc.crawl(max_depth=0,max_users=1)
