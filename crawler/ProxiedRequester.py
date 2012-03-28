#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from HttpUtils import http_request
from time import time

class ProxiedRequester:
    def __init__(self):
        self.proxies  = []      # list of proxies
        self.banperiod = 3600   # seconds
        self.curr_proxy_id = 0  # proxy used currently

    def load_proxies(self, f):
        """Load the proxies list from disk.
        `f' can be both a file or a string containing the filename.
        """        
        fclose = False
        if isinstance(f, str):
            f = open(f, 'r')
            fclose = True    # Force to close
            
        for l in f:
            l = l.strip()
            if l[0] == '#': continue   # skip comments
            
            l = l.split()
            if len(l) == 2:
                # Address and Port
                self.proxies.append((l[0], int(l[1]), 0.0, 0, 200, 0.0))
            elif len(l) == 6:
                # Address, Port, Avg. Response Time, Last HTTP Response and Time since last Rate Limit
                self.proxies.append((l[0], int(l[1]), float(l[2]), int(l[3]), int(l[4]), float(l[5])))
            
        if fclose: f.close()

    def save_proxies(self, f):
        """Save the proxies list to disk.
        `f' can be both a file or a string containing the filename.
        """
        fclose = False
        if isinstance(f, str):
            f = open(f, 'w')
            fclose = True    # Force to close

        f.write('# Address     Port     Avg. Response Time     N. Requests     LastStatus     LastRateExceeded\n')
        for proxy in self.proxies:
            line = "%s %d %f %d %d %f\n" % proxy
            f.write(line)

        if fclose: f.close()

    def request(self, url, body = None):
        tried_proxies = 0
        while tried_proxies < len(self.proxies):
            (server, port, avgt, reqn, status, lastban) = self.proxies[self.curr_proxy_id]
            if status != 200 and time() - lastban < self.banperiod:
                # If the status of the last HTTP request was not correct, and the ban period
                # has not expired, we must try a different proxy.
                self.curr_proxy_id = (self.curr_proxy_id + 1) % len(self.proxies)
                tried_proxies = tried_proxies+1
                continue

            try:
                t1 = time()
                (status,data) = http_request(url, server, port, body)
                t2 = time()
                if status == 400: lastban = t2
                avgt = (avgt*reqn + (t2-t1))/(reqn+1)
                reqn = reqn + 1
            except KeyboardInterrupt as e:
                raise (e)
            except Exception as e:
                print(e)
                status = 999 # Some weird exception
                
            self.proxies[self.curr_proxy_id] = (server, port, avgt, reqn, status, lastban)
            if status == 200:
                return (status,data)
            
        # No valid proxies to send the request
        return (999, None)
        
