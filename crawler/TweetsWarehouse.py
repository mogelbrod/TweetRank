#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from OsUtils import safemkdir, safemv, generate_tmp_fname
from threading import Lock
from os import listdir
from xml.dom.minidom import parse

class TweetsWarehouse:
    def __init__(self, tweets_dir):
        self.lock = Lock()
        self.tweetsdir = tweets_dir
        self.tweets = set()
        self.__load()
        
    def __load(self):
        safemkdir(self.tweetsdir)
        self.lock.acquire()
        try:
            for f in listdir(self.tweetsdir):
                self.tweets.add(int(f))
        except Exception as ex:
            raise ex
        finally:
            self.lock.release()
    
    def __len__(self):
        return len(self.tweets)
        
    def __contains__(self, elem): 
        return elem in self.tweets

    def __iter__(self):
        """
        Caution: Not thread-safe!
        """
        for tid in self.tweets:
            yield tid

    def __getitem__(self, tweet_id):
        return self.get(tweet_id)        
        
    def get(self, tweet_id):
        if tweet_id not in self.tweets: return None
        f = open(self.tweetsdir + str(tweet_id), 'r')
        tweet_dom = parse(f)
        f.close()
        return Tweet(tweet_dom)
    
    def add(self, tweet):
        tweet_id = tweet.get_tweet_id()
        self.lock.acquire()
        try:
            fname = self.tweetsdir + str(tweet_id)
            fname_tmp = generate_tmp_fname(fname)
            f = open(fname_tmp, 'wb')
            f.write(tweet.get_xml())
            f.close()
            safemv(fname_tmp, fname)
            self.tweets.add(tweet_id)
        except Exception as ex:
            raise ex
        finally:
            self.lock.release()
