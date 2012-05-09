#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from Tweet import Tweet
import solr, time

class SolrNotifier:
    def __init__(self, host = 'localhost', port = 8983, logger = None):
        self.host = host
        self.port = port
        self.logger = logger

    def notify_tweet(self, tweet):
        self.notify_tweets([tweet])

    def notify_tweets(self, tweets):
        conn = solr.SolrConnection('http://%s:%d/solr' % (self.host,self.port))
        docs = []
        for tw in tweets:
            # Notify about tw
            try:
                conn.add(id=tw.id,
                         created_at=tw.date,
                         text=tw.text,
                         hashtag=tw.hashtags,
                         retweet_count=tw.retweet_count,
                         user_nick=tw.user.nick,
                         user_name=tw.user.name,
                         user_followers=tw.user.followers_count,
                         user_friends=tw.user.friends_count,
                         user_statuses=tw.user.statuses_count)
            except Exception as ex:
                if self.logger is not None:
                    self.logger.exception('Adding tweet failed: %s' % str(tw))

            # Notify about the retweeted status
            if tw.retweeted_status is not None:
                tw = tw.retweeted_status
                try:
                    conn.add(id=tw.id,
                             created_at=tw.date,
                             text=tw.text,
                             hashtag=tw.hashtags,
                             retweet_count=tw.retweet_count,
                             user_nick=tw.user.nick,
                             user_name=tw.user.name,
                             user_followers=tw.user.followers_count,
                             user_friends=tw.user.friends_count,
                             user_statuses=tw.user.statuses_count)
                except Exception as ex:
                    if self.logger is not None:
                    self.logger.exception('Adding tweet failed: %s' % str(tw))

        trycommit = True
        while trycommit:
            try:
                conn.commit()
                trycommit = False
            except solr.SolrException as ex:
                if ex.httpcode == 503:
                    trycommit = True
                    time.sleep(2)
                else:
                    if self.logger is not None: self.logger.exception('')
                    trycommit = False
            except Exception as ex:
                if self.logger is not None: self.logger.exception('')
                trycommit = False

        conn.close()
