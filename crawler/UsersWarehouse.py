#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from OsUtils import safemkdir, safemv, generate_tmp_fname
from threading import Lock
from os import listdir
from xml.dom.minidom import parse
from Tweet import Tweet
from errno import ENOENT

class UsersWarehouse:
    def __init__(self, users_dir, tweets_dir):
        self.lock = Lock()
        self.usersdir  = users_dir
        self.tweetsdir = tweets_dir
        self.users = set()
        self._load()

    def _load(self):
        safemkdir(self.usersdir)
        safemkdir(self.tweetsdir)
        self.lock.acquire()
        try:
            for f in listdir(self.usersdir):
                f = f.split('.')
                self.users.add(int(f[0]))
        finally:
            self.lock.release()        

    def __len__(self):
        return len(self.users)

    def __contains__(self, user_id): 
        return user_id in self.users

    def __iter__(self):
        """
        Caution: Not thread-safe!
        """
        for uid in self.users:
            yield uid

    def _load_user_data(self, user_id):
        friends, hashtags = set(), dict()        
        try:
            # Load friends
            f = open(self.usersdir + ('/%d.friends' % user_id), 'r')
            for l in f:
                friends.add(int(l))
            f.close()

            # Load hashtags
            f = open(self.usersdir + ('/%d.hashtags' % user_id), 'r')
            for l in f:
                l = l.split()
                if len(l) < 2: continue
                hashtags[l[0]] = int(l[1])
            f.close()
        except IOError as e:
            if e.errno != ENOENT:
                raise (e)
        return friends, hashtags

    def _save_user_data(self, user_id, friends, hashtags):
        # Save friends
        if len(friends) > 0:
            friends_file = self.usersdir + ('/%s.friends' % user_id)
            ftmp = generate_tmp_fname(friends_file)
            f = open(ftmp, 'w')
            for friend_id in friends:
                f.write('%d\n' % friend_id)
            f.close()
            safemv(ftmp, friends_file)
            
        # Save hashtags
        if len(hashtags) > 0:
            hashtags_file = self.usersdir + ('/%s.hashtags' % user_id)
            htmp = generate_tmp_fname(hashtags_file)
            f = open(htmp, 'w')
            for ht in hashtags.items():
                f.write('%s %d\n' % ht)
            f.close()
            safemv(htmp, hashtags_file)

    def add_user_tweets(self, user_id, set_of_tweets):
        print len(set_of_tweets) # TODO
        if len(set_of_tweets) == 0: return
        tweets_file = generate_tmp_fname('%s/%d.tweets' % (self.tweetsdir, user_id))
        f = open(tweets_file, 'w')
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<statuses type="array">\n')
        for tweet in set_of_tweets:
            f.write(tweet.get_xml() + '\n')
        f.write('</statuses>')
        f.close()
        
    def get(self, user_id):
        friends,hashtags=None,None
        self.lock.acquire()
        try:
            friends,hashtags=self._load_user_data(user_id)
        finally:
            self.lock.release()
        return friends,hashtags

    def add(self, user_id, friends_ids, hashtags):
        self.lock.acquire()
        try:
            stored_friends, stored_hashtags = self._load_user_data(user_id)
            stored_friends.update(friends_ids)
            for ht in hashtags:
                c = stored_hashtags.get(ht, 0)
                c = c+1
                stored_hashtags[ht] = c
            self._save_user_data(user_id, stored_friends, stored_hashtags)
        finally:
            self.lock.release()
