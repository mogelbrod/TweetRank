#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json
from OsUtils import safemkdir
from ProxiedRequester import ProxiedRequester

class TwitterCrawler:            
    def __init__(self, datadir):
        self.datadir   = datadir
        self.tweetsdir = datadir + '/tweets/'
        self.usersdir  = datadir + '/users/'

        self.frontier  = set()
        self.tweets    = set()
        self.users     = set()

        safemkdir(self.tweetsdir)
        safemkdir(self.usersdir)

        self.load_data(datadir)
        self.load_frontier()

        self.requester = ProxiedRequester()
        self.requester.load_proxies(datadir + '/proxies2.txt')

    def load_data(self, datadir):
        try:
            for f in os.listdir(datadir + '/tweets/'):
                self.tweets.add(f)
            for f in os.listdir(datadir + '/users/'):
                self.users.add(f)
            return True
        except Exception as e:
            print(e)
            return False

    def load_frontier(self):
        try:
            ff = open(self.datadir + '/frontier.txt')
            for f in ff:
                self.frontier.add(f[:-1])
        except Exception as e:
            print(e)

    def get_twitter_multipage_query(self, query):
        page = 1
        result = []
        while True:
            (status, jsondata) = self.requester.request("%s&page=%d" % (query, page))
            if status == 999:
                # No proxies available!
                return result

            if status == 200:
                data = json.loads(jsondata.decode('utf-8'))
                result.extend(data)
                if len(data) < 100: return result
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

    def get_retweets_by_user(self,user):
        query = 'http://api.twitter.com/1/statuses/retweeted_by_user.json?id=%s&count=100' % user
        return self.get_twitter_multipage_query(query)

    def get_user_status_by_id(self, users):
        # TODO: POST requests are not working correctly
        query = 'http://api.twitter.com/1/users/lookup.json'
        return self.post_twitter_singlepage_query(query, {'screen_name': users})


    def crawl(self):
        # TODO
        for user in self.frontier:
            pass


tc = TwitterCrawler('../data/')
#tc.crawl()
print (tc.get_user_status_by_id(['joapuipe','twitter']))
