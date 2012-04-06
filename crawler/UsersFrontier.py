#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from threading import Lock
from heapq import heappush, heappop, heapify

class UsersFrontier:
    def __init__(self, datadir, save_ops=1000):
        self.lock = Lock()
        self.datadir = datadir
        self.frontier = []
        self.users_in_frontier = set()
        self.save_ops = save_ops
        self.ops = 0
        self.__load()

    def __del__(self):
        self.__save()

    def __load(self):
        self.lock.acquire()
        try:
            f = open(self.datadir + '/frontier.txt', 'r')
            for l in f:
                l = l.split()
                self.frontier.append( (int(l[1]), int(l[0])) )
                self.users_in_frontier.add( int(l[0]) )
            heapify(self.frontier)
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def __save(self):
        self.lock.acquire()
        try:
            f = open(self.datadir + '/frontier.txt', 'w')
            for u in self.frontier:
                f.write("%d\t%d\n" % (u[1], u[0]))
            f.close()
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def __contains__(self, user):
        return user in self.users_in_frontier

    def __len__(self):
        return len(self.users_in_frontier)

    def push(self, user, depth):
        self.lock.acquire()
        try:
            if user not in self.users_in_frontier:
                heappush(self.frontier, (depth,user))
                self.users_in_frontier.add( user )
                self.ops = self.ops + 1
        except Exception as e:
            raise e
        finally:
            self.lock.release()
        if self.ops % self.save_ops == 0: self.__save()    

    def pop(self):
        self.lock.acquire()
        elem = None
        try:
            elem = heappop(self.frontier)
            self.ops = self.ops + 1
        except Exception as e:
            raise e
        finally:
            self.lock.release()
        if self.ops % self.save_ops == 0: self.__save()    
        return elem[1], elem[0]
