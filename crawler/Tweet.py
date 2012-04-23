#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

class Tweet:
    def __init__(self, dom):
        self.xml = ('<status>' + ''.join(node.toxml('utf-8') for node in dom.childNodes) + '</status>')
        self._init_tweet_id(dom)
        self._init_user_id(dom)
        self._init_retweeted_id(dom)
        self._init_replied_id(dom)
        self._init_mentioned_ids(dom)
        self._init_hashtags(dom)
        self._init_retweeted_status(dom)       

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

    def _init_tweet_id(self, dom):
        for elem_id in dom.getElementsByTagName('id'):
            if elem_id.parentNode.tagName == 'status':
                self.id = int(elem_id.firstChild.data)

    def _init_user_id(self, dom):
        self.user_id = None
        for elem_id in dom.getElementsByTagName('id'):
            if elem_id.parentNode.tagName == 'user' and elem_id.parentNode.parentNode.tagName == 'status':
                self.user_id = int(elem_id.firstChild.data)

    def _init_retweeted_id(self, dom):
        self.retweeted_id = None
        for elem_id in dom.getElementsByTagName('id'):
            if elem_id.parentNode.tagName == 'retweeted_status':
                self.retweeted_id = int(elem_id.firstChild.data)

    def _init_replied_id(self, dom):
        self.replied_id = None
        for elem_id in dom.getElementsByTagName('in_reply_to_status_id'):
            if elem_id.parentNode.tagName == 'status' and elem_id.firstChild is not None:
                self.replied_id = int(elem_id.firstChild.data)

    def _init_mentioned_ids(self, dom):
        self.mentioned_ids = []
        for userment_id in dom.getElementsByTagName('user_mention'):
            if userment_id.parentNode.parentNode.parentNode.tagName == 'status':
                self.mentioned_ids.append(int(userment_id.getElementsByTagName('id')[0].firstChild.data))
        self.mentioned_ids = set(self.mentioned_ids)

    def _init_hashtags(self, dom):
        self.hashtags = []
        for elem in dom.getElementsByTagName('hashtag'):
            if elem.parentNode.parentNode.parentNode.tagName == 'status':
                self.hashtags.append(elem.getElementsByTagName('text')[0].firstChild.data.encode('utf-8'))

    def _init_retweeted_status(self, dom):
        self.retweeted_status = None
        rt_elem = dom.getElementsByTagName('retweeted_status')
        if len(rt_elem) == 0: return
        self.retweeted_status = rt_elem[0].cloneNode(True)
        self.retweeted_status.tagName = 'status'
        self.retweeted_status = Tweet(self.retweeted_status)

    def get_tweet_id(self):
        return self.id
        
    def get_user_id(self):
        return self.user_id

    def get_retweeted_id(self):
        return self.retweeted_id

    def get_replied_id(self):
        return self.replied_id

    def get_mentioned_ids(self):
        return self.mentioned_ids

    def get_hashtags(self):
        return self.hashtags

    def get_retweeted_status(self):
        return self.retweeted_status

    def get_xml(self):
        return self.xml
