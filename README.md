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

Running tests
-------------

We use `busted` and require `luasocket` to mock `ngx.socket.tcp()`. To run the tests, start a local cassandra instance and run:

    $ busted
