#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from OsUtils import safemkdir
from threading import Lock
from os import listdir, SEEK_END, stat
from struct import pack, unpack

class UsersWarehouse:
    def __init__(self, users_dir):
        self.lock = Lock()
        self.usersdir = users_dir
        self.users = dict() # Stores for each user, the last retrieved tweet
        self.__load()

    def __load(self):
        safemkdir(self.usersdir)
        self.lock.acquire()
        try:
            if 'last_user_tweet.dat' in listdir(self.usersdir):
                f = open(self.usersdir + '/last_user_tweet.dat', 'rb')
                fpos  = 0
                fdata = f.read(16)
                while fdata:
                    user_id, tweet_id = unpack('!QQ', fdata)
                    self.users[user_id] = (tweet_id, fpos)
                    fpos = f.tell()
                    fdata = f.read(16)
            else:
                f = open(self.usersdir + '/last_user_tweet.dat', 'wb')
                f.close()
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

    def __set_last_tweet(self, user_id, tweet_id):
        f = open(self.usersdir + '/last_user_tweet.dat', 'r+b')
        fdata = pack('!QQ', user_id, tweet_id)
        user_entry = self.users.get(user_id, None)
        if user_entry == None:
            # append fdata to the end of file
            f.seek(0, SEEK_END)
            user_entry = (tweet_id, f.tell())
            f.write(fdata)
        else:
            # overwrite file
            f.seek(user_entry[1], 0)
            f.write(fdata)
        self.users[user_id] = user_entry
        f.close()
        
    def get(self, user_id):
        user_entry = self.users.get(user_id, None)
        if user_entry == None: return None
        else: return user_entry[0]

    def add(self, user_id, tweet_id):
        self.lock.acquire()
        try:
            self.__set_last_tweet(user_id, tweet_id)
        except Exception as ex:
            raise ex
        finally:
            self.lock.release()
