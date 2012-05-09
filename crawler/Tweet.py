#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import rfc822, datetime, pytz

class Tweet:
    def __init__(self):
        self.id = None
        self.text = None
        self.date = None
        self.retweet_count = None
        self.user = None
        self.mentions = []
        self.hashtags = []
        self.replied_id = None
        self.retweeted_status = None

    def __del__(self):
        self.user = None
        self.retweeted_status = None
        self.mentions = None
        self.hashtags = None

    def __eq__(self, other):
        return (other is not None and self.id == other.id)

    def __ne__(self, other):
        return (other is not None and self.id != other.id)

    def __lt__(self, other):
        return (other is not None and self.id < other.id)

    def __gt__(self, other):
        return (other is not None and self.id > other.id)

    def __hash__(self):
        return self.id

    def __str__(self):
        tstr = '{ID: %d, Date: %s, Text: %s, RT count: %d, User: %s, Hashtags: %s, Mentions: %s' % (self.id, str(self.date), self.text, self.retweet_count, str(self.user), str(self.hashtags), str(self.mentions))
        if self.replied_id is not None:
            tstr += ', RP: %d' % self.replied_id
        if self.retweeted_status is not None:
            tstr += ', RT: %s' % repr(self.retweeted_status)
        tstr += '}'
        return tstr
