#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from ServicesNotifier import ServicesNotifier
from TweetArrayParser import TweetArrayParser

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
    logger.setLevel(logging.DEBUG)

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

    files = listdir(data_dir)

    # Process users data
    for fname in files:
        user_id = int(fname.split('.')[0])
        ftype   = fname.split('.')[1]
        if fname in ignored: continue
        fname = data_dir + fname

        if ftype == 'friends':
            logger.info('Friends file: %s' % fname)
            f = open(fname, 'r')
            friends = []
            for l in f: friends.append(int(l))
            notif.notify_user_friends(user_id, friends)
            f.close()
        elif ftype == 'tweets':
            logger.info('Tweets file: %s' % fname)
            parser = make_parser()
            parser.setContentHandler(TweetArrayParser(lambda tw: notif.notify_tweet(tw)))
            parser.parse(fname)
        else:
            pass # TODO: Add hashtags used by the user

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
