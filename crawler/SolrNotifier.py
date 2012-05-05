#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from Tweet import Tweet
import solr

class SolrNotifier:
    def __init__(self, host = '176.9.149.66', port = 8983, max_pending=5000):
        self.host = host
        self.port = port
        self.max_pending = max_pending
        self.pending_tweets = set()

    def add_tweets(self, tweets):
        self.pending_tweets.update(tweets)
        if len(self.pending_tweets) < self.max_pending: return
        self.flush()

    def flush(self):
        conn = solr.SolrConnection('http://%s:%d/solr' % (self.host,self.port))
        docs = []
        for tw in self.pending_tweets:
            docs.append( dict(id=tw.get_tweet_id(),
                        created_at=tw.get_date(),
                        text=tw.get_text(),
                        hashtag=tw.get_hashtags(),
                        user_nick=tw.get_user_nick(),
                        user_name=tw.get_user_name(),
                        user_followers=tw.get_followers_count(),
                        user_friends=tw.get_friends_count(),
                        user_statuses=tw.get_statuses_count(),
                        retweet_count=tw.get_retweeted_count()) )
        conn.add_many(docs)
        conn.commit()
        self.pending_tweets = set()

if __name__ == '__main__':
    from xml.dom.minidom import parse
    dom = parse("../data/tweets/755859.tweets.1335180104858318")
    tweets = set()
    for tweet in dom.getElementsByTagName('status'):
        tweets.add(Tweet(tweet))

    snotif = SolrNotifier()
    snotif.add_tweets(tweets)
    snotif.flush()
