#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import solr

class SolrNotifier:
    def __init__(self, host = 'localhost', port =, max_pending=5000):
        self.host = host
        self.port = port
        self.max_pending = max_pending
        self.pending_tweets = {}

    def add_tweets(self, tweets):
        self.pending_tweets.update(tweets)
        if self.pending_tweets < self.max_pending: return
        
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
            
        
