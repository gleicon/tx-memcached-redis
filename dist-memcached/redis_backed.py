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
		self.d = {} # temp local dict
		log.msg('Redis Client initialized ', self.client)

	@defer.inlineCallbacks	
	def doGet(self, req, data):
		v = yield self.client.get(req.key)
		(exp, flags, cas, val) = pickle.loads(v.encode('utf-8'))
		res = binary.GetResponse(req, flags, cas, data=val)
		
		# If the magic 'slow' is requested, slow down.
		if req.key == 'slow':
			rv = defer.Deferred()
			reactor.callLater(5, rv.callback, res)
			defer.returnValue(rv)
		else:
			defer.returnValue(res)
	
	@defer.inlineCallbacks
	def doGetQ(self, req, data):
		try:
			self.doGet(req, data)
		except binary.MemcachedNotFound:
			defer.returnValue(binary.EmptyResponse())
	
	@defer.inlineCallbacks	
	def doSet(self, req, data):
		flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
		o = yield self.client.set(req.key, pickle.dumps((exp, flags, 0, data)))
				
	def doAdd(self, req, data):
		# work on add + packed data for redis
		if req.key in self.d:
			raise binary.MemcachedExists()
		else:
			flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
			self.d[req.key] = (exp, flags, 0, data)

	# unsafe (non atomic) operations
	@defer.inlineCallbacks
	def doIncr(self, req, data):
		pass

	@defer.inlineCallbacks	
	def doDecr(self, req, data):
		pass

	@defer.inlineCallbacks	
	def doAppend(self, req, newdata):
		v = yield self.client.get(req.key)
		(exp, flags, cas, olddata) = pickle.loads(v.encode('utf-8'))
		n = list(olddata + newdata)
		n[3] = chr(len(n) + 1)
		o = yield self.client.set(req.key, pickle.dumps((exp, flags, 0, "".join(n))))
	
	@defer.inlineCallbacks
	def doPrepend(self, req, newdata):
		v = yield self.client.get(req.key)
		(exp, flags, cas, olddata) = pickle.loads(v.encode('utf-8'))
		hdr = list(olddata[:4])
		body = olddata[4:]
		c = ord(hdr[3])
		hdr[3] = chr(c + len(newdata))
		n = "".join(hdr) + newdata + body
		o = yield self.client.set(req.key, pickle.dumps((exp, flags, 0, n)))

	@defer.inlineCallbacks	
	def doDelete(self, req, data):
		o = yield self.client.delete(req.key)

	@defer.inlineCallbacks
	def doFlush(self, req, data):
		self.d = {}
    
	def doVersion(self, req, data):
		r = binary.MultiResponse()
		r.add(binary.Response(req, key='version', data='blah'))
		return r
		
	def doStats(self, req, data):
		r = binary.MultiResponse()
		r.add(binary.Response(req, key='version', data='blah'))
		r.add(binary.Response(req, key='', data=''))
 		return r

storage = RedisBackedStorage()

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
        constants.CMD_QUIT: ex,
        constants.CMD_VERSION: storage.doVersion,
		constants.CMD_INCR: storage.doIncr,
		constants.CMD_DECR: storage.doDecr
        }

factory = protocol.Factory()
factory.protocol = DistributedBinaryServer

reactor.listenTCP(11212, factory)
reactor.run()

