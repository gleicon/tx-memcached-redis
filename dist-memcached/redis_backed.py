#!/usr/bin/env python
# redis backed memcached server

import os
import sys
import struct
import txredisapi
import yaml
import pickle

from twisted.python import log
log.startLogging(sys.stdout)


sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task
from memcached import binary, constants


class RedisBackedStorage(object):
    def __init__(self):
		self.doStart()

    def doStart(self):
		self.client = txredisapi.lazyRedisConnectionPool()
		log.msg('Redis Client initialized ', self.client)

    @defer.inlineCallbacks
    def doGet(self, req, data):
        log.msg("doGet")
        x = None
        v = yield self.client.hgetall(req.key)
        log.msg("get ret %s" % v)
        if len(v) > 0:
            x = binary.GetResponse(req, v["flags"], v["cas"], data = str(v["data"]))
            log.msg("get: %s" % req.key)
            #if req.key == 'slow':
            #    rv = defer.Deferred()
            #    reactor.callLater(5, rv.callback, x)
            #    return rv
            log.msg(repr(x))
            log.msg("bytes: %s" % x.toSequence())
            defer.returnValue(x)
        else:
            raise binary.MemcachedNotFound()

    def doGetQ(self, req, data):
        log.msg("doGetQ")
        try:
            o = self.doGet(req, data)
            return o
        except binary.MemcachedNotFound:
            return binary.EmptyResponse()

    def doSet(self, req, data):
        log.msg("cas: %s" % req.cas)
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        o = self.client.hmset(req.key, {"exp": exp, "flags": flags, "cas": 0, "data": data})
        log.msg("set: %s: %s" % (req.key, data))
        return binary.EmptyResponse()

	# unsafe (non atomic) operations

    def doAdd(self, req, data):
        log.msg("add")
        r = self.client.exists(req.key)
        res = None
        def p(v):
            if v is 0:
                flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
                o = self.client.hmset(req.key, {"exp": exp, "flags": flags, "cas": 0, "data": data})
                res = binary.Response(req, cas=req.cas)
                log.msg("add: %s" % data)
            else:
                res = None
        r.addCallback(p)
        if res is None: raise binary.MemcachedExists()
        return res

    def doReplace(self, req, data):
        log.msg("replace")
        r = self.client.exists(req.key)
        res = None
        def p(v):
            if v > 0:
                flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
                o = self.client.hmset(req.key, {"exp": exp, "flags": flags, "cas": 0, "data": data})
                res = binary.Response(req, cas=req.cas)
            else:
                res = None
        r.addCallback(p)
        if res is None: raise binary.MemcachedNotFound()
        return res

    @defer.inlineCallbacks		
    def doIncr(self, req, data):
        log.msg("incr")
        log.msg("i cas: %s" % req.cas)
        r = yield self.client.exists(req.key)
        if r:
            v = yield self.client.hgetall(req.key)
            log.msg("incr v -> %s" % v)
            o = self.client.hincr(req.key, "data")
            res = binary.Response(req, cas=v["cas"], data=str(v["data"] + 1))
            defer.returnValue(res)
        else:
            raise binary.MemcachedNotFound()

    @defer.inlineCallbacks		
    def doDecr(self, req, data):
		log.msg("decr")
		v = yield self.client.exists(req.key)	
		if v:
			v = yield self.client.hgetall(req.key)
			log.msg("old val: %s" % v["data"])
			o = self.client.hdecr(req.key, "data")
			res = binary.Response(req, cas=v["cas"], data=str(v["data"] - 1))
			defer.returnValue(res)
		else:
			raise binary.MemcachedNotFound()

    def doAppend(self, req, newdata):
        log.msg("append")
        r = self.client.hgetall(req.key)		
        def p(v):
            if len(v) > 1:
                n = list(v["data"] + newdata)
                o = self.client.hset(req.key, "data", "".join(n))
                res = binary.Response(req, cas=v["cas"], data="".join(n))
                return res
            else:
                raise binary.MemcachedNotFound()
        r.addCallback(p)

    def doPrepend(self, req, newdata):
        log.msg("prepend")		
        r = self.client.hgetall(req.key)		
        def p(v):
            if len(v) > 1:
                n = list(newdata + v["data"])
                o = self.client.hset(req.key, "data", "".join(n))
                res = binary.Response(req, cas=v["cas"], data="".join(n))
                return res
            else:
                raise binary.MemcachedNotFound()
        r.addCallback(p)

    def doDelete(self, req, data):
        log.msg("delete")
        o = self.client.delete(req.key)

    def doFlush(self, req, data):
        log.msg("flush")
        o = self.client.flushdb()

    def doVersion(self, req, data):
        r = binary.MultiResponse()
        r.add(binary.Response(req, key='version', data='blah'))
        return r

    def doStats(self, req, data):
        r = binary.MultiResponse()
        r.add(binary.Response(req, key='version', data='blah'))
        r.add(binary.Response(req, key='', data=''))
        return r

    def doNoop(self, req, data):
        log.msg("noop")

storage = RedisBackedStorage()

def ex(*a):
    print "Shutting down a client."
    raise binary.MemcachedDisconnect()


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
        constants.CMD_NOOP: storage.doNoop,
        constants.CMD_QUIT: ex,
        constants.CMD_VERSION: storage.doVersion,
		constants.CMD_INCR: storage.doIncr,
		constants.CMD_DECR: storage.doDecr,
		constants.CMD_REPLACE: storage.doReplace
        }

factory = protocol.Factory()
factory.protocol = DistributedBinaryServer

reactor.listenTCP(11212, factory)
reactor.run()

