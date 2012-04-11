#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from HttpUtils import http_request
from OsUtils import get_utc_time, safemv, generate_tmp_fname
from threading import Lock
from time import time
from random import randint

class ProxiedRequester:
    class ProxyEntry:
        def __init__(self, address, port, ok_reqs=0, tot_reqs=0, rhits=0, rtime=0):
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

        def request(self, url, body = None):
            if self.rhits == 0 and get_utc_time() < self.rtime:
                return 400, None

            self.tot_reqs = self.tot_reqs + 1

            status, data, rhits, rtime = http_request(url, self.address, self.port, body)            
            if rhits != None and rtime != None:
                self.rhits, self.rtime = rhits, rtime
                
            if status == 200:
                self.ok_reqs = self.ok_reqs + 1
            
            return status, data
            
    def __init__(self, proxy_file):
        self.lock = Lock()
        self.proxies  = []      # list of proxies
        self.proxy_file = proxy_file
        self.__load_proxies()

    def __del__(self):
        self.__save_proxies()

    def __load_proxies(self):
        """Load the proxies list from disk.
        """        
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

    def __save_proxies(self):
        """Save the proxies list to disk.
        """
        tmp_fname = generate_tmp_fname(self.proxy_file)
        f = open(tmp_fname, 'w')
        for proxy in self.proxies:
            f.write(str(proxy) + '\n')    
        f.close()
        safemv(tmp_fname, self.proxy_file)
        
    def request(self, url, body = None):
        min_rtime = None
        tried_proxies = 0
        while tried_proxies < len(self.proxies):
            # Selects the proxy to use.
            # Checks that the proxy has not reached the rate limit.
            self.lock.acquire()
            proxy = self.proxies[self.curr_proxy]
            if proxy.rhits == 0 and proxy.rtime > get_utc_time():
                tried_proxies = tried_proxies + 1
                self.curr_proxy = (self.curr_proxy + 1) % len(self.proxies)
                self.lock.release()
                continue
            self.lock.release()
            
            print('Requesting %s on %s:%d...' % (url, proxy.address, proxy.port))

            # Send query
            t1 = time()
            status, data = proxy.request(url, body)
            t2 = time()
            if status != 999 and (min_rtime == None or proxy.rtime < min_rtime):
                min_rtime = proxy.rtime

            print('status=%d, time=%d, rhits=%d, rtime=%d' % (status, t2-t1, proxy.rhits, proxy.rtime))

            if status == 200:
                # Everything OK. Save statistics and continue
                self.__save_proxies()
                return (status, data, None)
            elif status == 999 or proxy.rhits == 0:
                # Something wrong. Try a new proxy!
                self.lock.acquire()
                tried_proxies = tried_proxies + 1
                self.curr_proxy = (self.curr_proxy + 1) % len(self.proxies)
                self.lock.release()
            
        # No valid proxies to send the request
        # Check if we can wait, or we should abourt
        difft=None
        if min_rtime != None:
            difft = min_rtime - get_utc_time()
            if difft < 0: difft = None

        self.__save_proxies()
        return (-1, None, difft)
