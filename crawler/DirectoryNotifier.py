#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from ServicesNotifier import ServicesNotifier
from TweetArrayParser import TweetArrayParser

from xml.sax import make_parser
from os import listdir

import sys, logging

def usage():
    print 'python DirectoryNotifier.py data_dir [ignore_list]'

def tweet_callback(notif, tweet):
    notif.notify_tweet(tweet) # Notify tweet

def main(argv):
    if len(argv) != 2 and len(argv) != 3:
        usage()
        return -1

    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(module)s:%(funcName)s:%(lineno)d -> %(message)s')
    logger = logging.getLogger('DirectoryNotifier')
    logger.setLevel(logging.INFO)

    notif = ServicesNotifier(logger) # Ranker & Solr notifier


    ignored = set()
    if len(argv) == 3:
        f = open(argv[2])
        for fname in f:
            ignored.add(fname.strip())
        f.close()

    data_dir = argv[1]
    tweets_dir = data_dir + '/tweets/'
    users_dir  = data_dir + '/users/'

    fulist = listdir(users_dir)
    ftlist = listdir(tweets_dir)

    fulist.sort()
    ftlist.sort()

    for fname in fulist:
        user_id = int(fname.split('.')[0])
        ftype   = fname.split('.')[1]
        fname = users_dir + fname
        if fname in ignored: continue

        logger.info('User file: %s' % fname)
        if ftype == 'friends':
            f = open(fname, 'r')
            friends = []
            for l in f:
                friends.append(int(l))
            notif.notify_user_friends(user_id, friends)
            f.close()
        else:
            pass # Add hashtags used by the user

    for fname in ftlist:
        fname = tweets_dir + fname
        if fname in ignored: continue

        logger.info('Tweets file: %s' % fname)
        parser = make_parser()
        parser.setContentHandler(TweetArrayParser(lambda tw: tweet_callback(notif, tw)))
        parser.parse(fname)

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
