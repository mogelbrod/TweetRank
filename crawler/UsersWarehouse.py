#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from OsUtils import safemkdir, safemv, generate_tmp_fname
from threading import Lock
from os import listdir
from xml.dom.minidom import parse
from Tweet import Tweet

class UsersWarehouse:
    def __init__(self, users_dir, tweets_dir):
        self.lock = Lock()
        self.usersdir  = users_dir
        self.tweetsdir = tweets_dir
        self.users = set()
        self.__load()

    def __load(self):
        safemkdir(self.usersdir)
        self.lock.acquire()
        try:
            for f in listdir(self.usersdir):
                f = f.split('.')
                self.users.add(int(f[0]))
        except Exception as ex:
            raise ex
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

    def __load_user_data(self, user_id):
        try:
            friends, hashtags = set(), dict()
            files_in_dir = listdir(self.usersdir)
            if ('%d.friends' % user_id) not in files_in_dir or ('%d.hashtags' % user_id) not in files_in_dir:
                return friends, hashtags

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
            
            return friends, hashtags
        except Exception as ex:
            raise ex

    def __save_user_data(self, user_id, friends, hashtags):
        try:
            # Save friends
            friends_file = self.usersdir + ('/%s.friends' % user_id)
            hashtags_file = self.usersdir + ('/%s.hashtags' % user_id)
            ftmp = generate_tmp_fname(friends_file)
            htmp = generate_tmp_fname(hashtags_file)
            
            f = open(ftmp, 'w')
            for friend_id in friends:
                f.write('%d\n' % friend_id)
            f.close()

            # Save hashtags
            f = open(htmp, 'w')
            for ht in hashtags.items():
                f.write('%s %d\n' % (ht[0], ht[1]))
            f.close()

            safemv(ftmp, friends_file)
            safemv(htmp, hashtags_file)
        except Exception as ex:
            raise ex

    def __load_user_tweets(self, user_id):
        result = set()
        try:
            tweets_file = '%s/%d.tweets' % (self.tweetsdir, user_id)
            f = open(tweets_file, 'r')
            domdata = parse(f)
            tweets = domdata.getElementsByTagName('status')
            for tw in tweets:
                result.add(Tweet(tw))              
            f.close()
        except IOError:
            pass
        return result

    def __save_user_tweets(self, user_id, set_of_tweets):
        tweets_file = '%s/%d.tweets' % (self.tweetsdir, user_id)
        f = open(tweets_file, 'wb')
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(b'<statuses type="array">\n')
        for tweet in set_of_tweets:
            f.write(tweet.get_xml() + b'\n')
        f.write(b'</statuses>')
        f.close()

    def add_tweets_to_user(self, user_id, set_of_tweets):
        existing_tweets = self.__load_user_tweets(user_id)
        existing_tweets.update(set_of_tweets)
        self.__save_user_tweets(user_id, set_of_tweets)
        
    def get(self, user_id):
        friends,hashtags=None,None
        self.lock.acquire()
        try:
            friends,hashtags=self.__load_user_data(user_id)
        except Exception as ex:
            raise ex
        finally:
            self.lock.release()
        return friends,hashtags

    def add(self, user_id, friends_ids, hashtags):
        self.lock.acquire()
        try:
            stored_friends, stored_hashtags = self.__load_user_data(user_id)
            stored_friends.update(friends_ids)
            for ht in hashtags:
                c = stored_hashtags.get(ht, 0)
                c = c+1
                stored_hashtags[ht] = c
            self.__save_user_data(user_id, stored_friends, stored_hashtags)
        except Exception as ex:
            raise ex
        finally:
            self.lock.release()
