#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPError

def http_request(url, server, port=80, timeout=20.0):
    def check_twitter_response(response):
        return (response is not None and len(response.headers.get_list('Server')) > 0 \
                and response.headers.get_list('Server')[0] == 'tfe')

    def get_rate_limits(headers):
        rtime = headers.get_list('X-RateLimit-Reset')
        rhits = headers.get_list('X-RateLimit-Remaining')
        if len(rtime) > 0: rtime = int(rtime[0])
        else: rtime = None
        if len(rhits) > 0: rhits = int(rhits[0])
        else: rhits = None
        return rhits, rtime

    AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
    http_client = HTTPClient()
    code, data, rtime, rhits = 999, None, None, None
    try:
        response = http_client.fetch(url, proxy_host=server, proxy_port=port, connect_timeout=timeout, request_timeout=timeout)
        response.rethrow()

        if check_twitter_response(response):
            code, data = response.code, response.body
            rhits, rtime = get_rate_limits(response.headers)

    except HTTPError as e:
        if check_twitter_response(e.response):
            code, data = e.code, None
            rhits, rtime = get_rate_limits(e.response.headers)

    return code, data, rhits, rtime
