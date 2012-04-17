#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import http.client
from socket import setdefaulttimeout

def http_request(url, server, port=80, body=None, timeout=20.0):
    method = "GET"
    if body != None: method = "POST"

    status = 999
    data  = None
    rtime = None
    rhits = None
    
    try:
        setdefaulttimeout(timeout)
        conn = http.client.HTTPConnection(server, port, timeout=timeout)        
        conn.request(method, url)
        conn.sock.settimeout(timeout)
        resp = conn.getresponse()
        
        if resp.getheader('Server', 'error') != 'tfe':
            # Response not from Twitter
            return 999, None, None, None
        
        status = resp.status
        rtime  = int(resp.getheader('X-RateLimit-Reset', 0))
        rhits  = int(resp.getheader('X-RateLimit-Remaining', 0))

        if resp.status == http.client.OK:
            data  = resp.read()

    except KeyboardInterrupt as e:
        raise (e)
    except Exception as e:
        print(e)
        return 999, None, None, None

    return status, data, rhits, rtime
