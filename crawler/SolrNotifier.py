#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from Tweet import Tweet
import solr, time

class SolrNotifier:
    def __init__(self, host = '176.9.149.66', port = 8983, max_pending=1000):
        self.host = host
        self.port = port
        self.max_pending = max_pending
        self.pending_tweets = set()

    def add_tweet(self, tweet):
        self.pending_tweets.add(tweet)
        if len(self.pending_tweets) >= self.max_pending: self.flush()

    def add_tweets(self, tweets):
        self.pending_tweets.update(tweets)
        if len(self.pending_tweets) >= self.max_pending: self.flush()

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

        trycommit = True
        while trycommit:
            try:
                conn.commit()
                trycommit = False
            except solr.SolrException as ex:
                if ex.httpcode == 503:
                    trycommit = True
                    time.sleep(2)
                else: trycommit = False
            except:
                trycommit = False

        conn.close()
        self.pending_tweets = set()
