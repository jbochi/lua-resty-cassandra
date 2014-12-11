lua-resty-cassandra
===================

[![Build Status](https://travis-ci.org/jbochi/lua-resty-cassandra.svg?branch=master)](https://travis-ci.org/jbochi/lua-resty-cassandra)

Pure Lua Cassandra client using CQL binary protocol v2.

It is 100% non-blocking if used in Nginx/Openresty but can also be used with luasocket.


Installation
------------

For usage inside nginx, just copy the `src/cassandra.lua` file.

Otherwise, run:

    $ luarocks install cassandra


API
---

Overview:
```lua
cassandra = require("cassandra")
session = cassandra.new()
session:set_timeout(1000)  -- 1000ms timeout
connected, err = session:connect("127.0.0.1", 9042)

session:set_keyspace("lua_tests")

-- simple query
local table_created, err = session:execute([[
    CREATE TABLE users (
      user_id uuid PRIMARY KEY,
      name varchar,
      age int
    )
]])

-- query with arguments
local ok, err = session:execute([[
  INSERT INTO users (name, age, user_id)
  VALUES (?, ?, ?)
]], {"John O'Reilly", 42, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})


-- select statement
local users, err = session:execute("SELECT name, age, user_id from users")

assert(1 == #users)
local user = users[1]
ngx.say(user.name) -- "John O'Reilly"
ngx.say(user.user_id) -- "1144bada-852c-11e3-89fb-e0b9a54a6d11"
ngx.say(user.age) -- 42
```

You can check more examples on the [tests](https://github.com/jbochi/lua-resty-cassandra/blob/master/spec/functional_spec.lua).


### Methods

#### session, err = cassandra.new()

Creates a new session.

#### ok, err = session:set_timeout(timeout)

Sets timeout (in miliseconds).

#### ok, err = session:connect({contact_points}, port)

Connect to a single host or to a handle host in an array of contact points at the given port.

#### ok, err = session:setkeepalive(max_idle_timeout, pool_size)  -- Nginx only

Puts the current Cassandra connection immediately into the ngx_lua cosocket connection pool.

You can specify the max idle timeout (in ms) when the connection is in the pool and the maximal size of the pool every nginx worker process.

In case of success, returns `1`. In case of errors, returns `nil` with a string describing the error.

Only call this method in the place you would have called the close method instead. Calling this method will immediately turn the current cassandra session object into the closed state. Any subsequent operations other than connect() on the current objet will return the closed error.

#### times, err = session:get_reused_times() -- Nginx only

This method returns the (successfully) reused times for the current connection. In case of error, it returns `nil` and a string describing the error.

If the current connection does not come from the built-in connection pool, then this method always returns `0`, that is, the connection has never been reused (yet). If the connection comes from the connection pool, then the return value is always non-zero. So this method can also be used to determine if the current connection comes from the pool.

#### ok, err = session:close()

Closes the current connection and returns the status.

In case of success, returns `1`. In case of errors, returns nil with a string describing the error.

#### stmt, err = session:prepare(query, options)

Prepare a statement for later execution. `option` are the same options available on `execute`

#### result, err = session:execute(query, args, options)

Execute a query or previously prepared statement. `args` is an array of arguments that can be optionally
type annoted. For example, with `cassandra.bigint(4)`. If there is no annotation, the driver will try to
infer a type. Since integer numbers are serialized as `int` with 4 bytes, Cassandra would return an error 
if we tried to insert it in a `bigint` column. `options` is a table that can contain two types of keys:

* `consistency_level`: for example `cassandra.consistency.ONE`
* `tracing`: enables tracing for this query. In this case, the result table will contain a key named `tracing_id` with an uuid of the tracing session.


#### ok, err = session:set_keyspace(keyspace_name)

Sets session keyspace to the given `keyspace_name`.


#### trace, err = session:get_trace(result)

Return the trace of a given result, if possible. The trace is a table with the following keys (from `system_traces.sessions` and `system_traces.events` [system tracing tables](http://www.datastax.com/dev/blog/advanced-request-tracing-in-cassandra-1-2):

* coordinator
* duration
* parameters
* request
* started_at
* events: an array of tables with the following keys:
    * event_id
    * activity
    * source
    * source_elapsed
    * thread


Running tests
-------------

We use `busted` and require `luasocket` to mock `ngx.socket.tcp()`. To run the tests, start a local cassandra instance and run:

    $ busted
