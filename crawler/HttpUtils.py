#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from urllib.request import FancyURLopener
from socket import setdefaulttimeout

def http_request(url, server, port = 80, data = None, timeout = 20):
    """Request a URL to a HTPP server.

    `url' must be the Absolute URL in the case that `server' is the address of a proxy server.
    
    `data` is used on POST requests. `data' must be a dictionary containing the parameters that will be embedded in the
    body of the HTTP request. If `data' = None, a GET request is assumed.

    This function returns an HTTPResponse instance.
    """
    try:
        opener  = FancyURLopener(proxies={'http': 'http://%s:%d/' % (server, port)})
        setdefaulttimeout(timeout)
        
        resp = opener.open(url, data)
        if resp == None:
            return 999, None, None, None
        
        headers = resp.info()
        if headers == None or headers['Status'] == None:
            return 999, None, None, None
        
        status  = int(headers['Status'].split()[0])
        rhits   = headers['X-RateLimit-Remaining']
        if rhits != None: rhits = int(rhits)
        rtime   = headers['X-RateLimit-Reset']
        if rtime != None: rtime = int(rtime)
        data    = None
        
        if status == 200:
            data = resp.read()       
            
        return status, data, rhits, rtime
    except KeyboardInterrupt as e:
        raise (e)
    except Exception as e:
        return 999, None, None, None
    
