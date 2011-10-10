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
		v = yield self.client.get(req.key)
		if v is not None:
			(exp, flags, cas, val) = pickle.loads(v.encode('utf-8'))
			res = binary.GetResponse(req, flags, cas, data=val)
		
			# If the magic 'slow' is requested, slow down.
			if req.key == 'slow':
				rv = defer.Deferred()
				reactor.callLater(5, rv.callback, res)
				defer.returnValue(rv)
			else:
				defer.returnValue(res)
	
	def doGetQ(self, req, data):
		try:
			o = self.doGet(req, data)
			return o
		except binary.MemcachedNotFound:
			defer.returnValue(binary.EmptyResponse())
	
	def doSet(self, req, data):
		flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
		o = self.client.set(req.key, pickle.dumps((exp, flags, 0, data)))

	# unsafe (non atomic) operations
	def doAdd(self, req, data):
		r = self.client.exists(req.key)
		def p(v):
			if v is None:
				flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
				o = self.client.set(req.key, pickle.dumps((exp, flags, 0, data)))
		r.addCallback(p)

	def doReplace(self, req, data):
		r = self.client.exists(req.key)
		def p(v):
			if v is not None:
				flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
				o = self.client.set(req.key, pickle.dumps((exp, flags, 0, data)))
		r.addCallback(p)
		

	def doIncr(self, req, data):
		pass

	def doDecr(self, req, data):
		pass

	def doAppend(self, req, newdata):
		r = self.client.get(req.key)
		def p(v):
			(exp, flags, cas, olddata) = pickle.loads(v.encode('utf-8'))
			n = list(olddata + newdata)
			n[3] = chr(len(n) + 1)
			o = self.client.set(req.key, pickle.dumps((exp, flags, 0, "".join(n))))
		r.addCallback(p)
		
	def doPrepend(self, req, newdata):
		r = self.client.get(req.key)
		def p(v):
			(exp, flags, cas, olddata) = pickle.loads(v.encode('utf-8'))
			hdr = list(olddata[:4])
			body = olddata[4:]
			c = ord(hdr[3])
			hdr[3] = chr(c + len(newdata))
			n = "".join(hdr) + newdata + body
			o = self.client.set(req.key, pickle.dumps((exp, flags, 0, n)))
		r.addCallback(p)
		
	def doDelete(self, req, data):
		r = self.client.exists(req.key)
		def p(v):
			if v is None:
				raise binary.MemcachedNotFound()
			else:
				o = self.client.delete(req.key)
		r.addCallback(p)

	def doFlush(self, req, data):
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
    # this also works, but apparently confuses people.
    # sys.exit(0)
	
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

