#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from OsUtils import safemkdir
from threading import Lock
from os import listdir
import json

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
        tweet_data = json.load(f)
        f.close()
        return tweet_data
    
    def add(self, tweet_data):
        tweet_id = tweet_data['id']
        self.lock.acquire()
        try:
            f = open(self.tweetsdir + str(tweet_id), 'w')
            json.dump(tweet_data, f)
            f.close()
            self.tweets.add(tweet_id)
        except Exception as ex:
            raise ex
        finally:
            self.lock.release()
