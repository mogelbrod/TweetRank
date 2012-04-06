#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json
from ProxiedRequester import ProxiedRequester
from TweetsWarehouse import TweetsWarehouse
from UsersWarehouse import UsersWarehouse
from UsersFrontier import UsersFrontier
from TweetUtils import get_related_users

class TwitterCrawler:            
    def __init__(self, datadir):
        self.frontier = UsersFrontier(datadir)
        self.tweets = TweetsWarehouse(datadir + '/tweets/')
        self.users  = UsersWarehouse(datadir + '/users/')

        self.requester = ProxiedRequester()
        self.requester.load_proxies(datadir + '/proxies.txt')


    def get_twitter_multipage_query(self, query, count = 100):
        page = 1
        result = []
        while True:
            (status, jsondata) = self.requester.request("%s&count=%d&page=%d" % (query, count, page))
            if status == 999:
                # No proxies available!
                return result # Output the current result

            if status == 200:
                data = json.loads(jsondata.decode('utf-8'))
                result.extend(data)
                if len(data) < count: return result
                else: page = page + 1

    def get_twitter_singlepage_query(self, query):
        (status, jsondata) = self.requester.request(query)
        if status == 200:
            return json.loads(jsondata.decode('utf-8'))
        else:
            return []

    def post_twitter_singlepage_query(self, query, params):
        (status, jsondata) = self.requester.request(query, params)
        if status == 200:
            return json.loads(jsondata.decode('utf-8'))
        else:
            return []
		
    def get_user_tweets(self, user_id, since_id = None):
        query = 'http://api.twitter.com/1/statuses/user_timeline.json?id=%d&exclude_replies=false&include_rts=true&include_entities=true' % user_id
        if since_id != None:
            query = query + ('&since_id=%d' % since_id)
        return self.get_twitter_multipage_query(query, 200)

    def get_user_retweets(self, user_id, since_id = None):
        query = 'http://api.twitter.com/1/statuses/retweeted_by_user.json?id=%d&include_entities=true' % user_id
        if since_id != None:
            query = query + ('&since_id=%d' % since_id)
        return self.get_twitter_multipage_query(query, 100)

    def crawl(self, maxdepth = 3, maxusers = 400):
        # TODO
        #tweets_by_user = {}
        while len(self.frontier) > 0 and len(self.users) < maxusers:
            user,depth = self.frontier.pop()
            if user in self.users: continue
            tweets_by_user = self.get_user_tweets(user)
            """
            print (user)
            for tid in self.tweets:
                tw_user = tweets_by_user.get(self.tweets[tid]['user']['id'], [])
                tw_user.append(tid)
                tweets_by_user[self.tweets[tid]['user']['id']] = tw_user
            """    
            for tweet in tweets_by_user:
                self.tweets.add(tweet)
                if depth < maxdepth:
                    related_users = get_related_users(tweet)
                    for ru in related_users:
                        self.frontier.push(ru, depth+1)

            self.users.add(user, 0) # dummy, this will be used later
        print ("Crawled users: %d" % len(self.users))
        print ("Crawled tweets: %d" % len(self.tweets))
            

tc = TwitterCrawler('../data/')
tc.crawl()
