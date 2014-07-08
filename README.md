lua-resty-cassandra
===================

Cassandra client for Lua Nginx module using CQL binary protocol v2

This is a work in progress and is definitly not ready for production use.

Only simple queries are working.

API
---

You can check some examples on the [tests](https://github.com/jbochi/lua-resty-cassandra/blob/master/spec/functional_spec.lua).


Running tests
-------------

We use busted and require luasocket to mock ngx.socket.tcp(). To run the tests, just run:

    $ busted

