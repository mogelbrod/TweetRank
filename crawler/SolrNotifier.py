#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from Tweet import Tweet
import solr

class SolrNotifier:
    def __init__(self, host = 'localhost', port = 8983, max_pending=5000):
        self.host = host
        self.port = port
        self.max_pending = max_pending
        self.pending_tweets = {}

    def add_tweets(self, tweets):
        self.pending_tweets.update(tweets)
        if self.pending_tweets < self.max_pending: return
        
        conn = solr.SolrConnection('http://%s:%d/solr' % (self.host,self.port))
        for tw in self.pending_tweets:
            sdoc = dict(id=tw.get_tweet_id(),
                        created_at=tw.get_date(),
                        text=tw.get_text(), #TODO
                        hashtag=tw.get_hashtags(), #TODO
                        user_nick=tw.get_user_nick(), #TODO
                        user_name=tw.get_user_name(), #TODO
                        user_followers=tw.get_user_followers(), #TODO
                        user_friends=tw.get_user_friends(), # TODO
                        user_statuses=tw.get_user_statuses(),
                        retweet_count=tw.get_retweet_count())
        conn.commit()
        self.pending_tweets = {}
            
    def flush(self):
        conn = solr.SolrConnection('http://%s:%d/solr' % (self.host,self.port))
        for tw in tweets:
            sdoc = dict(id=tw.get_tweet_id(),
                        created_at=tw.get_date(),
                        text=tw.get_text(), #TODO
                        hashtag=tw.get_hashtags(), #TODO
                        user_nick=tw.get_user_nick(), #TODO
                        user_name=tw.get_user_name(), #TODO
                        user_followers=tw.get_user_followers(), #TODO
                        user_friends=tw.get_user_friends(), # TODO
                        user_statuses=tw.get_user_statuses(),
                        retweet_count=tw.get_retweet_count())
        conn.commit()

if __name__ == '__main__':
    from xml.dom.minidom import parse
    dom = parse("../data/tweets/755859.tweets.1335180104858318")
    for tweet in dom.getElementsByTagName('status'):
        tweet = Tweet(tweet)
        print tweet.get_user_nick(), tweet.get_user_name(), tweet.get_followers_count(), tweet.get_friends_count(), tweet.get_retweeted_count(), tweet.get_text()
