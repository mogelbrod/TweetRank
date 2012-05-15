#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPError, HTTPRequest
from tornado.ioloop import IOLoop

class RankerNotifier:
    def __init__(self, host = 'localhost', max_clients=100, port = 4711, logger = None):
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        self.http_client = AsyncHTTPClient(max_clients=max_clients)
        self.host = host
        self.port = port
        self.logger = logger
        self.body = ''

    def _handleResponse(self, response):
        if response.error is not None and self.logger is not None:
            self.logger.error('Request %s provoked error:\n %s' % (response.request.body, str(response.error)))
        elif self.logger is not None:
            self.logger.debug('Request %s finished.' % response.request.body)
        IOLoop.instance().stop()

    def _sendRequest(self):
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=self.body)
        self.http_client.fetch(request, self._handleResponse)
        IOLoop.instance().start()
        self.body = ''

    def add_retweet(self, tweet_id, retweeted_id):
        """This method tells the ranker that `tweet_id' is a retweet of `retweeted_id'"""
        self.body += "TYPE=RT&ID=%d&RefID=%d\n" % (tweet_id, retweeted_id)

    def add_reply(self, tweet_id, replied_id):
        """This method tells the ranker that `tweet_id' is a reply to `replied_id'"""
        self.body += "TYPE=RP&ID=%d&RefID=%d\n" % (tweet_id, replied_id)

    def add_mentions(self, tweet_id, mentioned_user_ids):
        """This method tells the ranker that `tweet_id' mentioned users in `mentioned_user_ids'"""
        if len(mentioned_user_ids) == 0: return

        self.body += 'TYPE=MN&ID=%d' % tweet_id
        for uid in mentioned_user_ids:
            self.body += '&RefID=%d' % uid
        self.body += '\n'

    def add_user_friends(self, user_id, friend_ids):
        """This method tells the ranker that `user_id' is following users in `followed_user_ids'"""
        if friend_ids is None or len(friend_ids) == 0: return

        self.body += 'TYPE=FW&ID=%d' % user_id
        for uid in friend_ids:
            self.body += '&RefID=%d' % uid
        self.body += '\n'
        self._sendRequest()

    def add_user_tweets(self, user_id, tweet_ids):
        """This method tells the ranker that `user_id' is the author of tweets in `tweet_ids'"""
        if tweet_ids is None or len(tweet_ids) == 0: return

        self.body += 'TYPE=TW&ID=%d' % user_id
        for tid in tweet_ids:
            self.body += '&RefID=%d' % tid
        self.body += '\n'

    def add_tweet_hashtags(self, tweet_id, hashtags):
        """This method tells the ranker that `tweet_id' contained the hashtags in `hashtags'"""
        if hashtags is None or len(hashtags) == 0: return

        self.body += 'TYPE=HT&ID=%d' % tweet_id
        for ht in hashtags:
            self.body += '&RefID=%s' % ht
        self.body += '\n'

    def notify_tweet(self, tweet):
        self.notify_tweets([tweet])

    def notify_tweets(self, tweets):
        if self.logger is not None: self.logger.debug('Sending files to Ranker...')
        for tweet in tweets:
            if tweet.retweeted_status is not None:
                self.add_retweet(tweet.id, tweet.retweeted_status.id)

            if tweet.replied_id is not None:
                self.add_reply(tweet.id, tweet.replied_id)

            if len(tweet.mentions) > 0:
                self.add_mentions(tweet.id, tweet.mentions)

            if len(tweet.hashtags) > 0:
                self.add_tweet_hashtags(tweet.id, tweet.hashtags)

            self.add_user_tweets(tweet.user.id, [tweet.id])

        self._sendRequest()
        if self.logger is not None: self.logger.debug('Ranker received files!')
