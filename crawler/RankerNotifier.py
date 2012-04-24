#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPError, HTTPRequest

class RankerNotifier:
    def __init__(self, host = "localhost", port = 4711):
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        self.http_client = HTTPClient()
        self.host = host
        self.port = port

    def _sendRequest(self, request):
        try:
            response = self.http_client.fetch(request)
            response.rethrow()
            return response.code
        except HTTPError as e:
            return e.code
        except Exception:
            return 999

    def add_retweet(self, tweet_id, retweeted_id):
        """This method tells the ranker that `tweet_id' is a retweet of `retweeted_id'"""
        body = "ID: %d\nRefID: %d\n" % (tweet_id, retweeted_id)
        request = HTTPRequest('http://%s:%d/RT'%(self.host, self.port),method='POST', body=body)
        return self._sendRequest(request)

    def add_reply(self, tweet_id, replied_id):
        """This method tells the ranker that `tweet_id' is a reply to `replied_id'"""
        body = "ID: %d\nRefID: %d\n" % (tweet_id, replied_id)
        request = HTTPRequest('http://%s:%d/RP'%(self.host, self.port),method='POST', body=body)        
        return self._sendRequest(request)

    def add_mentions(self, tweet_id, mentioned_user_ids):
        """This method tells the ranker that `tweet_id' mentioned users in `mentioned_user_ids'"""
        body = 'ID: %d\n' % tweet_id
        for uid in mentioned_user_ids:
            body = body + ('RefID: %d\n' % uid)
        request = HTTPRequest('http://%s:%d/MN'%(self.host, self.port),method='POST', body=body)
        return self._sendRequest(request)

    def add_following(self, user_id, followed_user_ids):
        """This method tells the ranker that `user_id' is following users in `followed_user_ids'"""
        body = 'ID: %d\n' % user_id
        for uid in followed_user_ids:
            body = body + ('RefID: %d\n' % uid)
        request = HTTPRequest('http://%s:%d/FW'%(self.host, self.port),method='POST', body=body)
        return self._sendRequest(request)

    def add_user_tweets(self, user_id, tweet_ids):
        """This method tells the ranker that `user_id' is the author of tweets in `tweet_ids'"""
        body = 'ID: %d\n' % user_id
        for tid in tweet_ids:
            body = body + ('RefID: %d\n' % tid)
        request = HTTPRequest('http://%s:%d/TW'%(self.host, self.port),method='POST', body=body)
        return self._sendRequest(request)

    def add_tweet_hashtags(self, tweet_id, hashtags):
        """This method tells the ranker that `tweet_id' contained the hashtags in `hashtags'"""
        body = 'ID: %d\n' % tweet_id
        for ht in hashtags:
            body = body + ('RefID: %s\n' % ht)
        request = HTTPRequest('http://%s:%d/HT'%(self.host, self.port),method='POST', body=body)
        return self._sendRequest(request)

