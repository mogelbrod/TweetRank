#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPError, HTTPRequest

class RankerNotifier:
    def __init__(self, host = "176.9.149.66", port = 4711):
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
        except:
            return 999

    def add_retweet(self, tweet_id, retweeted_id):
        """This method tells the ranker that `tweet_id' is a retweet of `retweeted_id'"""
        body = "TYPE=RT&ID=%d&RefID=%d\n" % (tweet_id, retweeted_id)
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=body)
        return self._sendRequest(request)

    def add_reply(self, tweet_id, replied_id):
        """This method tells the ranker that `tweet_id' is a reply to `replied_id'"""
        body = "TYPE=RP&ID=%d&RefID=%d\n" % (tweet_id, replied_id)
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=body)
        return self._sendRequest(request)

    def add_mentions(self, tweet_id, mentioned_user_ids):
        """This method tells the ranker that `tweet_id' mentioned users in `mentioned_user_ids'"""
        if len(mentioned_user_ids) == 0:
            return 200

        body = 'TYPE=MN&ID=%d' % tweet_id
        for uid in mentioned_user_ids:
            body = body + ('&RefID=%d' % uid)
        body = body + '\n'
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=body)
        return self._sendRequest(request)

    def add_user_friends(self, user_id, followed_user_ids):
        """This method tells the ranker that `user_id' is following users in `followed_user_ids'"""
        if len(followed_user_ids) == 0:
            return 200

        body = 'TYPE=FW&ID=%d' % user_id
        for uid in followed_user_ids:
            body = body + ('&RefID=%d' % uid)
        body = body + '\n'
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=body)
        return self._sendRequest(request)

    def add_user_tweets(self, user_id, tweet_ids):
        """This method tells the ranker that `user_id' is the author of tweets in `tweet_ids'"""
        if len(tweet_ids) == 0:
            return 200

        body = 'TYPE=TW&ID=%d' % user_id
        for tid in tweet_ids:
            body = body + ('&RefID=%d' % tid)
        body = body + '\n'
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=body)
        return self._sendRequest(request)

    def add_tweet_hashtags(self, tweet_id, hashtags):
        """This method tells the ranker that `tweet_id' contained the hashtags in `hashtags'"""
        if len(hashtags) == 0:
            return 200

        body = 'TYPE=HT&ID=%d' % tweet_id
        for ht in hashtags:
            body = body + ('&RefID=%s' % ht)
        body = body + '\n'
        request = HTTPRequest('http://%s:%d/'%(self.host, self.port),method='POST',body=body)
        return self._sendRequest(request)


    def notify_tweet(self, tweet):
        if tweet is None: return

        if tweet.get_retweeted_status() is not None:
            self.add_retweet(tweet.get_tweet_id(), tweet.get_retweeted_status().get_tweet_id())

        if tweet.get_replied_id() is not None:
            self.add_reply(tweet.get_tweet_id(), tweet.get_replied_id())

        if len(tweet.get_mentioned_ids()) > 0:
            self.add_mentions(tweet.get_tweet_id(), tweet.get_mentioned_ids())

        if len(tweet.get_hashtags()) > 0:
            self.add_tweet_hashtags(tweet.get_tweet_id(), tweet.get_hashtags())

        self.add_user_tweets(tweet.get_user_id(), [tweet.get_tweet_id()])


    def notify_tweets(self, tweets):
        for tweet in tweets:
            self.notify_tweet(tweet)
