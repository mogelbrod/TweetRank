#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def get_related_users(tweet):
    related = []
    if tweet['in_reply_to_user_id'] != None:
        related.append( tweet['in_reply_to_user_id'] )
    
    rts = tweet.get('retweeted_status', None)
    if rts != None and rts['user']['id'] != None:
        related.append( rts['user']['id'] )

    return related
