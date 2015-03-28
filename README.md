# lua-resty-cassandra

[![Build Status][badge-travis-image]][badge-travis-url]
[![Coverage Status][badge-coveralls-image]][badge-coveralls-url]
![Module Version][badge-version-image]

Pure Lua Cassandra client using CQL binary protocol v2.

It is 100% non-blocking if used in Nginx/Openresty but can also be used with luasocket.

## Installation

#### Luarocks

Installation through [luarocks][luarocks-url] is recommended:

```bash
$ luarocks install cassandra
```

#### Manual

Copy the `src/` folder and require `cassandra.lua`.

## Usage

Overview:

```lua
local cassandra = require "cassandra"

local session = cassandra.new()
session:set_timeout(1000) -- 1000ms timeout

local connected, err = session:connect("127.0.0.1", 9042)

session:set_keyspace("lua_tests")

-- simple query
local table_created, err = session:execute [[
  CREATE TABLE users(
    user_id uuid PRIMARY KEY,
    name varchar,
    age int
  )
]]

-- query with arguments
local ok, err = session:execute([[
  INSERT INTO users(name, age, user_id) VALUES(?, ?, ?)
]], {"John O'Reilly", 42, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})


-- select statement
local users, err = session:execute("SELECT name, age, user_id from users")

assert(1 == #users)
local user = users[1]
ngx.say(user.name) -- "John O'Reilly"
ngx.say(user.user_id) -- "1144bada-852c-11e3-89fb-e0b9a54a6d11"
ngx.say(user.age) -- 42
```

You can check more examples in the [tests](https://github.com/jbochi/lua-resty-cassandra/blob/master/spec/functional_spec.lua) or [here][anchor-examples].

## Socket methods

### session, err = cassandra.new()

Creates a new session. Create a socket with the cosocket API if available, fallback on luasocket otherwise.

> **Return values:**
>
> * `session`: A lua-resty-cassandra session.
> * `err`: Any error encountered during the socket creation.

### session:set_timeout(timeout)

Sets timeout (in miliseconds). Uses Nginx [tcpsock:settimeout](http://wiki.nginx.org/HttpLuaModule#tcpsock:settimeout).

> **Parameters:**
>
> * `timeout`: A number being the timeout in miliseconds

### ok, err = session:connect(contact_points, port)

Connects to a single or multiple hosts at the given port.

> **Parameters:**
>
> * `contact_points`: A string or an array of strings (hosts) to connect to.
> * `port`: The port number

> **Return values:**
>
> * `ok`: true if connected, false otherwise. Nil of the session doesn't have a socket.
> * `err`: Any encountered error.

### ok, err = session:set_keepalive(max_idle_timeout, pool_size)  -- Nginx only

Puts the current Cassandra connection immediately into the ngx_lua cosocket connection pool.

**Note**: Only call this method in the place you would have called the close method instead. Calling this method will immediately turn the current cassandra session object into the closed state. Any subsequent operations other than connect() on the current objet will return the closed error.

> **Parameters:**
>
> * `max_idle_timeout`: Max idle timeout (in ms) when the connection is in the pool
> * `pool_size`: Maximal size of the pool every nginx worker process.

> **Return values:**
>
> * `ok`: `1` if success, nil otherwise.
> * `err`: Encountered error if any

### times, err = session:get_reused_times() -- Nginx only

This method returns the (successfully) reused times for the current connection. In case of error, it returns `nil` and a string describing the error.

**Note:** If the current connection does not come from the built-in connection pool, then this method always returns `0`, that is, the connection has never been reused (yet). If the connection comes from the connection pool, then the return value is always non-zero. So this method can also be used to determine if the current connection comes from the pool.

> **Return values:**
>
> * `times`: Number of times the current connection was successfully reused, nil if error
> * `err`: Encountered error if any

### ok, err = session:close()

Closes the current connection and returns the status.

> **Return values:**
>
> * `ok`: `1` if success, nil otherwise.
> * `err`: Encountered error if any

## Client methods

All errors returned by functions in this section are tables with the following properties:

> * `code`: A string from one of the `error_codes` in `cassandra.contants`.
> * `raw_message`: The error message being returned by Cassandra.
> * `message`: A constructed error message with `code` + `raw_message`.

Error tables implement the `__tostring` method and are thus printable. A stringified error table will outputs its `message` property.

### ok, err = session:set_keyspace(keyspace_name)

Sets session keyspace to the given `keyspace_name`.

> **Parameters:**
>
> * `keyspace_name`: Name of the keyspace to use.

> **Return values:**
>
> See `:execute()`

### stmt, err = session:prepare(query, options)

Prepare a statement for later execution.

> **Parameters:**
>
> * `query`: A string representing a query to prepare.
> * `options`: The same options available on `:execute()`.

> **Return values:**
>
> * `stmt`: A prepareed statement to be used by `:execute()`, nil if the preparation failed.
> * `err`: Encountered error if any.

### result, err = session:execute(query, args, options)

Execute a query or previously prepared statement.

> **Parameters:**
>
> * `query`: A string representing a query or a previously prepared statement.
> * `args`: An array of arguments to bind to the query. Those arguments can be type annotated (example: `cassandra.bigint(4)`. If there is no annotation, the driver will try to infer a type. Since integer numbers are serialized as int with 4 bytes, Cassandra would return an error if we tried to insert it in a bigint column.
> * `options` is a table of options:
>   * `consistency_level`: for example `cassandra.consistency.ONE`
>   * `tracing`: if set to true, enables tracing for this query. In this case, the result table will contain a key named `tracing_id` with an uuid of the tracing session.
>   * `page_size`: Maximum size of the page to fetch (default: 5000).
>   * `auto_paging`: If set to true, `execute` will return an iterator. See the [example below][anchor-examples] on how to use auto pagination.

> **Return values:**
>
> * `result`: A table containing the result of this query if successful, ni otherwise. The table can contain additional keys:
>   * `type`: Type of the result set, can either be "VOID", "ROWS", "SET_KEYSPACE" or "SCHEMA_CHANGE".
>   * `meta`: If the result type is "ROWS" and the result has more pages that haven't been returned, this property will contain 2 values: `has_more_pages` and `paging_state`. See the [example below][anchor-examples] on how to use pagination.
> * `err`: Encountered error if any.

### batch, err = cassandra.BatchStatement(type)

Initialized a batch statement. See the [example below][anchor-examples] on how to use batch statements and [this](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/batch_r.html) for informations about the type of batch to use.

> **Parameters:**
>
> * `type`: The type of batch statement. Can be ony of those:
>   * `cassandra.batch_types.LOGGED` (default)
>   * `cassandra.batch_types.UNLOGGED`
>   * `cassandra.batch_types.COUNTER`

> **Return values:**
>
> * `batch`: An empty batch statement on which to add operations.
> * `err`: Encountered error if any.

### batch:add(query, args)

Add an operation to a batch statement. See the [example below][anchor-examples] on how to use batch statements.

> **Parameters:**
>
> * `query`: A string representing a query or a previously prepared statement.
> * `args`: An array of arguments to bind to the query, similar to `:execute()`.

### trace, err = session:get_trace(result)

Return the trace of a given result, if possible.

> **Parameters:**
>
> * `result`: A previous query result.

> **Return values:**
>
> `trace`: is a table with the following keys (from `system_traces.sessions` and `system_traces.events` [system tracing tables](http://www.datastax.com/dev/blog/advanced-request-tracing-in-cassandra-1-2):
>
> * coordinator
> * duration
> * parameters
> * request
> * started_at
> * events: an array of tables with the following keys:
>    * event_id
>    * activity
>    * source
>    * source_elapsed
>    * thread
>
> `err`: Encountered error if any.

## Examples

Batches:

```lua
-- Create a batch statement
local batch = cassandra.BatchStatement()

-- Add a query
batch:add("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)",
          {"James", 32, cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")})

-- Add a prepared statement
local stmt, err = session:prepare("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)")
batch:add(stmt, {"John", 45, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})

-- Execute the batch
local result, err = session:execute(batch)
```

Pagination might be very useful to build web services:

```lua
-- Assuming our users table contains 1000 rows

local query = "SELECT * FROM users"
local rows, err = session:execute(query, nil, {page_size = 500}) -- default page_size is 5000

assert.same(500, #rows) -- rows contains the 500 first rows

if rows.meta.has_more_pages then
  local next_rows, err = session:execute(query, nil, {paging_state = rows.meta.paging_state})

  assert.same(500, #next_rows) -- next_rows contains the next (and last) 500 rows
end
```

Automated pagination:

```lua
-- Assuming our users table now contains 10.000 rows

local query = "SELECT * FROM users"

for _, rows, page, err in session:execute(query, nil, {auto_paging=true}) do
  assert.same(5000, #rows) -- rows contains 5000 rows on each iteration in this case
  -- page: will be 1 on the first iteration, 2 on the second
  -- err: in case any fetch returns an error
  -- _: (the first for argument) is the current paging_state used to fetch the rows
end
```

## Running unit tests

We use `busted` and require `luasocket` to mock `ngx.socket.tcp()`. To run the tests, start a local cassandra instance and run:

```bash
$ luarocks install busted
$ make test
```

## Running coverage

```bash
$ luarocks install luacov
$ make coverage
```

Report will be in `./luacov.report.out`.

## Running linting

```bash
$ luarocks install luacheck
$ make lint
```

## Contributors

Juarez Bochi (@jbochi)

Thibault Charbonnier (@thibaultCha) -> Several contributions, including paging support, improved batch statements, better documentation, specs and code style.

Leandro Moreira (@leandromoreira) -> Added support for doubles

Marco Palladino (@thefosk)

[badge-travis-url]: https://travis-ci.org/jbochi/lua-resty-cassandra
[badge-travis-image]: https://img.shields.io/travis/jbochi/lua-resty-cassandra.svg?style=flat

[badge-coveralls-url]: https://coveralls.io/r/jbochi/lua-resty-cassandra?branch=master
[badge-coveralls-image]: https://coveralls.io/repos/jbochi/lua-resty-cassandra/badge.svg?branch=master

[badge-version-image]: https://img.shields.io/badge/version-0.5--5-green.svg?style=flat

[luarocks-url]: https://luarocks.org

[anchor-examples]: #examples
