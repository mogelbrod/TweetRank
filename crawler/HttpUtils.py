#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from tornado.httpclient import HTTPClient, HTTPError

def http_request(url, server, port=80, timeout=20.0):
    http_client = HTTPClient()
    code, data, rtime, rhits = None, None, None, None
    try:
        response = http_client.fetch(url, proxy_host=server, proxy_port=port, connect_timeout=timeout, request_timeout=timeout)
        response.rethrow()
        
        srv = response.headers.get_list('Server')
        if len(srv) == 0 or srv[0] != 'tfe':
            # Response does not come from Twitter
            return 999, None, None, None
        
        data = response.body
        code = response.code
        rtime = response.headers.get_list('X-RateLimit-Reset')
        rhits = response.headers.get_list('X-RateLimit-Remaining')
        if len(rtime) > 0: rtime = int(rtime[0])
        else: rtime = None
        if len(rhits) > 0: rhits = int(rhits[0])
        else: rhits = None
    except HTTPError as e:
        code, data, rhits, rtime = e.code, None, None, None

    return code, data, rhits, rtime
