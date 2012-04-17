#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from threading import Lock
from heapq import heappush, heappop, heapify
from OsUtils import generate_tmp_fname, safemv

class UsersFrontier:
    def __init__(self, frontierfile, save_ops=1):
        self.lock = Lock()
        self.frontierfile = frontierfile
        self.frontier = []
        self.users_in_frontier = set()
        self.save_ops = save_ops
        self.ops = 0
        self.size = 0
        self.__load()

    def __load(self):
        self.lock.acquire()
        try:
            f = open(self.frontierfile, 'r')
            for l in f:
                l = l.strip()
                if l[0] == '#': continue
                
                l= l.split() 
                if int(l[0]) not in self.users_in_frontier:
                    self.frontier.append( (int(l[2]), int(l[1]), int(l[0])) ) # Next_Query_Time, Last_Tweet_ID, UserID
                    self.users_in_frontier.add( int(l[0]) )
            heapify(self.frontier)
            self.size = len(self.frontier)
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def __save(self):
        self.lock.acquire()
        try:
            tmp_fname = generate_tmp_fname(self.frontierfile)
            f = open(tmp_fname, 'w')
            f.write('# UserID\tLastTweetID\tNextQuery\n')
            for u in self.frontier:
                f.write("%d\t%d\t%d\n" % (u[2], u[1], u[0]))
            f.close()
            safemv(tmp_fname, self.frontierfile)
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def __contains__(self, user):
        self.lock.acquire()
        is_in = False
        try:
            is_in = (user in self.users_in_frontier)
        finally:
            self.lock.release()
        return is_in

    def __len__(self):
        self.lock.acquire()
        size = self.size
        self.lock.release()
        return size

    def push(self, user, tweet, time):
        self.lock.acquire()
        try:
            if not user in self.users_in_frontier: # safety
                heappush(self.frontier, (time, tweet, user))
                self.users_in_frontier.add( user )
                self.ops = self.ops + 1
                self.size = self.size + 1
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
            self.users_in_frontier.remove(elem[2])
        except Exception as e:
            raise e
        finally:
            self.lock.release()
        return elem
