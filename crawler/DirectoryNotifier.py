#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from ServicesNotifier import ServicesNotifier
from TweetArrayParser import TweetArrayParser
from EscapeXMLIllegalCharEntities import EscapeXMLIllegalCharEntities

from xml.sax import make_parser
from os import listdir

import sys, logging

def usage():
    print 'python DirectoryNotifier.py data_dir [ignore_list]'

def main(argv):
    if len(argv) != 2 and len(argv) != 3:
        usage()
        return -1

    # Logger configuration
    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(module)s:%(funcName)s:%(lineno)d -> %(message)s')
    logger = logging.getLogger('DirectoryNotifier')
    logger.setLevel(logging.INFO)

    # Create Services Notifier (Ranker & Solr notifier)
    notif = ServicesNotifier(logger)

    # Load ignored files
    ignored = set()
    if len(argv) == 3:
        f = open(argv[2])
        for fname in f:
            ignored.add(fname.strip())
        f.close()

    # Prepare data to process
    data_dir = argv[1]
    tweets_dir = data_dir + '/tweets/'
    users_dir  = data_dir + '/users/'

    fulist = listdir(users_dir)
    ftlist = listdir(tweets_dir)

    fulist.sort()
    ftlist.sort()

    # Process users data
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
            pass # TODO: Add hashtags used by the user

    # Process tweet data
    for fname in ftlist:
        fname = tweets_dir + fname
        if fname in ignored: continue

        logger.info('Tweets file: %s' % fname)
        parser = make_parser()
        parser.setContentHandler(TweetArrayParser(lambda tw: notif.notify_tweet(tw)))
        f = open(fname)
        fdata = EscapeXMLIllegalCharEntities(f.read())
        f.close()
        parser.parse(fdata)

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
