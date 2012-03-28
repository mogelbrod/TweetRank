#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, errno
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
