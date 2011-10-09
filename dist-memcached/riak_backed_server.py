#!/usr/bin/env python

import os
import sys
import struct
from txriak import riak
from twisted.python import log
log.startLogging(sys.stdout)


sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

from memcached import binary, constants


class RiakStorage(object):

    def __init__(self):				
				self.doStart()
				
    def doStart(self):
				self.BUCKET = 'distmemcached'
				self.RIAK_CLIENT_ID = 'distmemcached_node'
				self.client = riak.RiakClient(client_id = self.RIAK_CLIENT_ID)
				self.bucket = self.client.bucket(self.BUCKET)
				self.d = {} # temp local dict
				log.msg('Riak Client initialized ', self.client)
				log.msg('Bucket selected: ', self.bucket)

    @defer.inlineCallbacks	
    def doGet(self, req, data):
        log.msg('get: ', req)
        c = yield self.bucket.get(req.key)
        exp, flags, cas, val = c.get_data()
        res = binary.GetResponse(req, flags, cas, data=val)
        # If the magic 'slow' is requested, slow down.
        if req.key == 'slow':
            rv = defer.Deferred()
            reactor.callLater(5, rv.callback, res)
            deer.returnValue(rv)
        else:
            defer.returnValue(res)

    def doGetQ(self, req, data):
        try:
            return self.doGet(req, data)
        except binary.MemcachedNotFound:
            return binary.EmptyResponse()

    def doSet(self, req, data):
        log.msg('e')
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        o = self.bucket.new(req_key, (exp, flags, 0, data))
        log.msg('storing: ', o.store())
				
    def doAdd(self, req, data):
        if req.key in self.d:
            raise binary.MemcachedExists()
        else:
            flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
            self.d[req.key] = (exp, flags, 0, data)

    def doAppend(self, req, newdata):
        exp, flags, cas, olddata = self.d[req.key]
        self.d[req.key] = (exp, flags, 0, olddata + newdata)

    def doPrepend(self, req, newdata):
        exp, flags, cas, olddata = self.d[req.key]
        self.d[req.key] = (exp, flags, 0, newdata + olddata)

    def doDelete(self, req, data):
        del self.d[req.key]

    def doFlush(self, req, data):
        self.d = {}

    def doStats(self, req, data):
        r = binary.MultiResponse()
        r.add(binary.Response(req, key='version', data='blah'))
        r.add(binary.Response(req, key='', data=''))
        return r

storage = RiakStorage()

def ex(*a):
    print "Shutting down a client."
    raise binary.MemcachedDisconnect()
    # this also works, but apparently confuses people.
    # sys.exit(0)

def doNoop(req, data):
    return binary.Response(req)

class DistributedBinaryServer(binary.BinaryServerProtocol):

    handlers = {
        constants.CMD_GET: storage.doGet,
        constants.CMD_GETQ: storage.doGetQ,
        constants.CMD_SET: storage.doSet,
        constants.CMD_ADD: storage.doAdd,
        constants.CMD_APPEND: storage.doAppend,
        constants.CMD_PREPEND: storage.doPrepend,
        constants.CMD_DELETE: storage.doDelete,
        constants.CMD_STAT: storage.doStats,
        constants.CMD_FLUSH: storage.doFlush,
        constants.CMD_NOOP: doNoop,
        constants.CMD_QUIT: ex
        }

factory = protocol.Factory()
factory.protocol = DistributedBinaryServer

reactor.listenTCP(11212, factory)
reactor.run()
