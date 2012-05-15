#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

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
            if self.rhits is not None and self.rtime is not None:
                return '%s %d %d %d %d %d' % (self.address, self.port, self.ok_reqs, self.tot_reqs, self.rhits, self.rtime)
            elif self.rhits is not None and self.rtime is None:
                return '%s %d %d %d %d None' % (self.address, self.port, self.ok_reqs, self.tot_reqs, self.rhits)
            elif self.rhits is None and self.rtime is not None:
                return '%s %d %d %d None %d' % (self.address, self.port, self.ok_reqs, self.tot_reqs, self.rtime)
            else:
                return '%s %d %d %d None None' % (self.address, self.port, self.ok_reqs, self.tot_reqs)

        def _increment_total_requests(self):
            self.trlock.acquire()
            self.tot_reqs = self.tot_reqs + 1
            self.trlock.release()

        def _increment_ok_requests(self):
            self.orlock.acquire()
            self.ok_reqs = self.ok_reqs + 1
            self.orlock.release()

        def _set_rate_limits(self, rhits, rtime):
            self.tllock.acquire()
            self.rtime = rtime
            self.rhits = rhits
            self.tllock.release()

        def _check_rate_limits(self):
            self.tllock.acquire()
            valid = True
            if self.rhits == 0 and get_utc_time() < self.rtime:
                valid = False
            self.tllock.release()
            return valid

        def request(self, url):
            if not self._check_rate_limits():
                return 400, None

            self._increment_total_requests()

            status, data, rhits, rtime = http_request(url, self.address, self.port)
            self._set_rate_limits(rhits, rtime)

            if status == 200:
                self._increment_ok_requests()
            return status, data

    def __init__(self, proxy_file):
        self.pflock = Lock()    # Lock for I/O operations on proxy_file
        self.cplock = Lock()    # Lock for current_proxy pointer
        self.proxies  = []      # list of proxies
        self.proxy_file = proxy_file
        self._load_proxies()

    def __del__(self):
        self._save_proxies()

    def _load_proxies(self):
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
                server, port, ok_reqs, tot_reqs, rhits, rtime = l[0], int(l[1]), 0, 0, None, None

                if len(l) >= 4:
                    ok_reqs, tot_reqs = int(l[2]), int(l[3])

                if len(l) >= 6:
                    rhits, rtime = eval(l[4]), eval(l[5])

                self.proxies.append(self.ProxyEntry(server, port, ok_reqs, tot_reqs, rhits, rtime))

            self.curr_proxy = randint(0, len(self.proxies)-1)
            f.close()
        finally:
            self.pflock.release()

    def _save_proxies(self):
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

    def _get_next_proxy(self, cp):
        self.cplock.acquire()
        if cp == self.curr_proxy:
            self.curr_proxy = (self.curr_proxy + 1) % len(self.proxies)
        cp = int(self.curr_proxy)
        self.cplock.release()
        return cp

    def _get_current_proxy(self):
        self.cplock.acquire()
        cp = int(self.curr_proxy)
        self.cplock.release()
        return cp

    def request(self, url):
        tried_proxies = 0
        cp = self._get_current_proxy()
        while tried_proxies < len(self.proxies):
            # Selects the proxy to use.
            # Checks that the proxy has not reached the rate limit.
            proxy = self.proxies[cp]
            if not proxy._check_rate_limits():
                tried_proxies = tried_proxies + 1
                cp = self._get_next_proxy(cp)
                continue

            print('Requesting %s on %s:%d...' % (url, proxy.address, proxy.port))

            # Send query
            status, data = proxy.request(url)

            if proxy.rhits is not None and proxy.rtime is not None:
                print('status=%d, rhits=%d, rtime=%d' % (status, proxy.rhits, proxy.rtime))
            elif proxy.rhits is None and proxy.rtime is not None:
                print('status=%d, rhits=None, rtime=%d' % (status, proxy.rtime))
            elif proxy.rhits is not None and proxy.rtime is None:
                print('status=%d, rhits=%d, rtime=None' % (status, proxy.rhits))
            else:
                print('status=%d, rhits=None, rtime=None' % (status))

            if status == 200:
                # Everything OK. Save statistics and continue
                self._save_proxies()
                return (status, data, None)
            elif status == 401 or status == 404:
                # Bad query, do not try again
                self._save_proxies()
                return (status, None, None)
            elif status == 500 or status == 502 or status == 503:
                # Twitter temporaly unavailable, try again
                pass
            else:
                # Something went wrong. Try a new proxy!
                cp = self._get_next_proxy(cp)
                tried_proxies = tried_proxies + 1

        # Save proxies
        self._save_proxies()

        # No valid proxies to send the request
        # Check if we can wait, or we should abort
        rtimes = [p.rtime for p in self.proxies if p.rtime is not None]
        if len(rtimes) == 0:
            return (999, None, None)
        else:
            return (999, None, min(rtimes))
