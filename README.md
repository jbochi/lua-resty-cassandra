lua-resty-cassandra
===================

Cassandra client for Lua Nginx module using CQL binary protocol v2

This is a work in progress and is definitly not ready for production use.

Only simple queries are working.

API
---
```lua
session = cassandra.new()
session:set_timeout(1000)
connected, err = session:connect("127.0.0.1", 9042)

session:set_keyspace("lua_tests")

local table_created, err = session:execute([[
    CREATE TABLE users (
      user_id uuid PRIMARY KEY,
      name varchar,
      age int
    )
]])

local ok, err = session:execute([[
  INSERT INTO users (name, age, user_id)
  VALUES (?, ?, ?)
]], {"Juarez S' Bochi", 31, {type="uuid", value="1144bada-852c-11e3-89fb-e0b9a54a6d11"}})
local users, err = session:execute("SELECT name, age, user_id from users")
assert(1 == #users)
local user = users[1]
ngx.say(user.name) -- "Juarez S' Bochi"
ngx.say(user.user_id) -- "1144bada-852c-11e3-89fb-e0b9a54a6d11"
ngx.say(user.age) -- 31
```

You can check more examples on the [tests](https://github.com/jbochi/lua-resty-cassandra/blob/master/spec/functional_spec.lua).

Running tests
-------------

We use `busted` and require `luasocket` to mock `ngx.socket.tcp()`. To run the tests, start a local cassandra instance and run:

    $ busted
