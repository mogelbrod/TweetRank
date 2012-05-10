#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

class User:
    def __init__(self):
        self.id = None
        self.nick = None
        self.name = None
        self.date = None
        self.followers_count = None
        self.friends_count = None
        self.statuses_count = None

    def __str__(self):
        return '{ID: %d, Nick: %s, Name: %s, Date: %s, Followers: %d, Friends: %d, Statuses: %d}' % (self.id,self.nick,self.name,str(self.date),self.followers_count,self.friends_count,self.statuses_count)
