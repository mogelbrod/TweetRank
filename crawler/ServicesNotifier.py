#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from RankerNotifier import RankerNotifier
from SolrNotifier import SolrNotifier

class ServicesNotifier:
    def __init__(self, logger = None, max_pending = 10):
        self.pending = []
        self.max_pending = max_pending
        self.snotif = SolrNotifier(logger=logger, host='176.9.149.66')
        self.rnotif = RankerNotifier(logger=logger, host='130.237.226.32')

    def __del__(self):
        if len(self.pending) > 0:
            self.flush()
        self.pending = None
        self.max_pending = None
        self.snotif = None
        self.rnotif = None

    def notify_tweet(self, tweet):
        self.pending.append(tweet)
        if tweet.retweeted_status is not None:
            self.pending.append(tweet.retweeted_status)
        if len(self.pending) >= self.max_pending:
            self.flush()

    def notify_user_friends(self, user_id, friends):
        self.rnotif.add_user_friends(user_id, friends)

    def notify_user_hashtags(self, user_id, hashtags):
        #self.rnotif.add_user_hashtags(user_id, hashtags)
        pass

    def flush(self):
        self.snotif.notify_tweets(self.pending)
        self.rnotif.notify_tweets(self.pending)
        self.pending = []
