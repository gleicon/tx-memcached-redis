Twisted based memcached server using redis as backend, with a lazy connection pool

Based on the twisted implementation of memcached (binary protocol) by Dustin Sallings: https://github.com/dustin/twisted-memcached

Depends on https://github.com/fiorix/txredisapi/

GET and SET are ok, other commands are tentative. Each key is stored as a redis hash with the protocol parts mapped to keys along with values. It wont pass all tests on https://github.com/dustin/memcached-test (probably wrong responses)

Note that not all clients implements the binary protocol

TODO: fix responses, port back text protocol, make the backend plugabble. I did this to plug into legacy systems that already have data around on redis and mysql, so having support for handlersocket  and probably a database as cassandra would be a good idea.

gm 2011

