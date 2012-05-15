#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from xml.sax.handler import ContentHandler
import rfc822, datetime, pytz
from Tweet import Tweet
from User import User
from EscapeXMLIllegalCharEntities import EscapeXMLIllegalCharEntities

class TweetArrayParser(ContentHandler):
    class Hashtag:
        def __init__(self):
            self.text = None

    class Mention:
        def __init__(self):
            self.id = None
            self.name = None
            self.screen_Name = None

    def __init__(self, tweet_callback):
        self.tweet_callback = tweet_callback
        self.stack = []
        self.ignore_object = None
        pass

    def startElement(self, name, attrs):
        self.pcharacters = ''
        if name == 'status' or name == 'retweeted_status':
            self.stack.append(Tweet())
        elif name == 'user':
            self.stack.append(User())
        elif name == 'hashtag':
            self.stack.append(self.Hashtag())
        elif name == 'user_mention':
            self.stack.append(self.Mention())
        elif name == 'place':
            self.ignore_object = 'place'

    def endElement(self, name):
        if self.ignore_object == name:
            self.ignore_object = None
        elif self.ignore_object is not None:
            return

        if name == 'id':
            self.stack[-1].id = int(self.pcharacters)
        elif name == 'text':
            self.stack[-1].text = EscapeXMLIllegalCharEntities(self.pcharacters)
        elif name == 'in_reply_to_status_id':
            if len(self.pcharacters) > 0: self.stack[-1].replied_id = int(self.pcharacters)
        elif name == 'retweet_count':
            self.stack[-1].retweet_count = int(self.pcharacters)
        elif name == 'created_at':
            date = rfc822.parsedate_tz(self.pcharacters)
            date = datetime.datetime(date[0], date[1], date[2],
                                    date[3], date[4], date[5], 0, pytz.utc)
            self.stack[-1].date = date
        elif name == 'name':
            self.stack[-1].name = EscapeXMLIllegalCharEntities(self.pcharacters)
        elif name == 'screen_name':
            self.stack[-1].nick = EscapeXMLIllegalCharEntities(self.pcharacters)
        elif name == 'followers_count':
            self.stack[-1].followers_count = int(self.pcharacters)
        elif name == 'friends_count':
            self.stack[-1].friends_count = int(self.pcharacters)
        elif name == 'statuses_count':
            self.stack[-1].statuses_count = int(self.pcharacters)
        elif name == 'user':
            self.stack[-2].user = self.stack[-1]
            self.stack.pop() # Removes user from the object stack
        elif name == 'retweeted_status':
            self.stack[-2].retweeted_status = self.stack[-1]
            self.stack.pop() # Removes tweet from the object stack
        elif name == 'status':
            self.tweet_callback(self.stack[-1])
            self.stack.pop() # Removes tweet from the object stack
        elif name == 'hashtag':
            self.stack[-2].hashtags.append(EscapeXMLIllegalCharEntities(self.stack[-1].text))
            self.stack.pop()
        elif name == 'user_mention':
            self.stack[-2].mentions.append(self.stack[-1].id)
            self.stack.pop()

    def characters(self, content):
        self.pcharacters += content

