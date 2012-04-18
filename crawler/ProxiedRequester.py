#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import http.client
from HttpUtils import http_request
from OsUtils import get_utc_time, safemv, generate_tmp_fname
from threading import Lock
from time import time
from random import randint

class ProxiedRequester:
    class ProxyEntry:
        def __init__(self, address, port, ok_reqs=0, tot_reqs=0, rhits=0, rtime=0):
            self.trlock   = Lock()
            self.orlock   = Lock()            
            self.tllock   = Lock()
            self.address  = address
            self.port     = port
            self.rhits    = rhits
            self.rtime    = rtime
            self.ok_reqs  = ok_reqs
            self.tot_reqs = tot_reqs
        
        def __eq__(self, other):
            return (self.address == other.address and self.port == other.port)
        
        def __str__(self):
            return '%s %d %d %d %d %d' % (self.address, self.port, self.ok_reqs, self.tot_reqs, self.rhits, self.rtime)

        def __increment_total_requests(self):
            self.trlock.acquire()
            self.tot_reqs = self.tot_reqs + 1
            self.trlock.release()

        def __increment_ok_requests(self):
            self.orlock.acquire()
            self.ok_reqs = self.ok_reqs + 1
            self.orlock.release()

        def __set_rate_limits(self, rhits, rtime):
            self.tllock.acquire()
            self.rtime = rtime
            self.rhits = rhits
            self.tllock.release()

        def check_rate_limits(self):
            self.tllock.acquire()
            valid = True
            if self.rhits == 0 and get_utc_time() < self.rtime:
                valid = False
            self.tllock.release()
            return valid

        def request(self, url, body = None):
            if not self.check_rate_limits():
                return 400, None

            self.__increment_total_requests()

            status, data, rhits, rtime = http_request(url, self.address, self.port, body)
            if rhits != None and rtime != None:
                self.__set_rate_limits(rhits, rtime)

            if status == 200:
                self.__increment_ok_requests()
            
            return status, data
            
    def __init__(self, proxy_file):
        self.pflock = Lock()    # Lock for I/O operations on proxy_file
        self.cplock = Lock()    # Lock for current_proxy pointer
        self.proxies  = []      # list of proxies
        self.proxy_file = proxy_file
        self.__load_proxies()

    def __del__(self):
        self.__save_proxies()

    def __load_proxies(self):
        """Load the proxies list from disk.
        """
        self.pflock.acquire()
        try:
            f = open(self.proxy_file, 'r')
            
            for l in f:
                l = l.strip()
                if l[0] == '#': continue   # skip comments

                l = l.split()
                if len(l) < 2: continue
                server, port, ok_reqs, tot_reqs, rhits, rtime = l[0], int(l[1]), 0, 0, 0, 0

                if len(l) >= 4: 
                    ok_reqs, tot_reqs = int(l[2]), int(l[3])

                if len(l) >= 6:
                    rhits, rtime = int(l[4]), int(l[5])

                self.proxies.append(self.ProxyEntry(server, port, ok_reqs, tot_reqs, rhits, rtime))

            self.curr_proxy = randint(0, len(self.proxies)-1)

            f.close()
        finally:
            self.pflock.release()

    def __save_proxies(self):
        """Save the proxies list to disk.
        """
        self.pflock.acquire()
        try:
            tmp_fname = generate_tmp_fname(self.proxy_file)
            f = open(tmp_fname, 'w')
            for proxy in self.proxies:
                f.write(str(proxy) + '\n')
            f.close()
            safemv(tmp_fname, self.proxy_file)
        finally:
            self.pflock.release()

    def __get_next_proxy(self, cp):
        self.cplock.acquire()
        if cp == self.curr_proxy:
            self.curr_proxy = (self.curr_proxy + 1) % len(self.proxies)
        cp = int(self.curr_proxy)
        self.cplock.release()
        return cp

    def __get_current_proxy(self):
        self.cplock.acquire()
        cp = int(self.curr_proxy)
        self.cplock.release()
        return cp
        
    def request(self, url, body = None):
        min_rtime = None
        tried_proxies = 0
        cp = self.__get_current_proxy()
        while tried_proxies < len(self.proxies):
            # Selects the proxy to use.
            # Checks that the proxy has not reached the rate limit.
            proxy = self.proxies[cp]
            if not proxy.check_rate_limits():
                tried_proxies = tried_proxies + 1
                cp = self.__get_next_proxy(cp)
                continue
            
            print('Requesting %s on %s:%d...' % (url, proxy.address, proxy.port))

            # Send query
            t1 = time()
            status, data = 999, None
            status, data = proxy.request(url, body)
            t2 = time()
            if status != 999 and (min_rtime == None or proxy.rtime < min_rtime):
                min_rtime = proxy.rtime

            print('status=%d, time=%d, rhits=%d, rtime=%d' % (status, t2-t1, proxy.rhits, proxy.rtime))

            if status == http.client.OK:
                # Everything OK. Save statistics and continue
                self.__save_proxies()
                return (status, data, None)
            elif status == http.client.UNAUTHORIZED or status == http.client.NOT_FOUND:
                # Bad query, do not try again
                self.__save_proxies()
                return (-2, None, None)
            elif status == http.client.INTERNAL_SERVER_ERROR or \
            status == http.client.BAD_GATEWAY or \
            status == http.client.SERVICE_UNAVAILABLE:
                # Twitter temporaly unavailable, try again
                pass 
            else:
                # Something went wrong. Try a new proxy!
                cp = self.__get_next_proxy(cp)
                tried_proxies = tried_proxies + 1
                
            
        # No valid proxies to send the request
        # Check if we can wait, or we should abourt
        difft=None
        if min_rtime != None:
            difft = min_rtime - get_utc_time()
            if difft < 0: difft = None

        self.__save_proxies()
        return (-1, None, difft)
