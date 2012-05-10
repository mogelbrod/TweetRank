#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import re

def EscapeXMLIllegalCharEntities(s):
    sc = ''
    lst_idx = 0
    for m in re.finditer('&#([0-9]+|x[0-9A-Za-z]+);', s):
        if m.group(1)[0] == 'x': code = eval('0x00' + m.group(1)[1:])
        else: code = int(m.group(1))
        if code != 0x09 and code != 0x0A and code != 0x0D and (code < 0x020 or code > 0x0D7FF) and \
           (code < 0x0E000 or code > 0x0FFFD) and (code < 0x10000 or code > 0x010FFFF):
            sc = sc + s[lst_idx:m.start()] + ('&amp;#x%x;'%code)
            lst_idx = m.end()
    sc = sc + s[lst_idx:]
    return sc

