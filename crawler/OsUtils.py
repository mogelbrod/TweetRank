#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
import os, errno
import random
from time import mktime, gmtime, time

def safemkdir(fname):
    try:
        os.makedirs(fname)
        return True
    except OSError as e:
        if e.errno == errno.EEXIST:
            return True
        else:
            print(e)
            return False

def safemv(src,dst):
    try:
        os.renames(src,dst)
        return True
    except OSError as e:
        return False

def get_utc_time():
    return int(mktime(gmtime()))

def generate_tmp_fname(fname):
    return (fname + '.%d%d' % (time(), random.randint(0,1000000)))
