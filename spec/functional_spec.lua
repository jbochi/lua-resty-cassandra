package.path = "src/?.lua;spec/?.lua;" .. package.path

_G.ngx = require("fake_ngx")
local cassandra = require("cassandra")

describe("cassandra", function()

  before_each(function()
    session = cassandra.new()
    session:set_timeout(1000)

    connected, err = session:connect("127.0.0.1", 9042)
    if err then
      error(err)
    end

    local res, err = session:execute [[
      CREATE KEYSPACE IF NOT EXISTS lua_tests
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
    ]]
    if err then
      error(err)
    end

    session:set_keyspace("lua_tests")
  end)

  teardown(function()
    local res, err = session:execute("DROP KEYSPACE lua_tests")
    if err then
      error(err)
    end
  end)

  describe("Connection #socket", function()
    it("should be possible to connect", function()
      assert.truthy(connected)
    end)

    it("should return an error if attempting to connect without an initialized session", function()
      local connected, err = cassandra.connect({}, "127.0.0.1")
      assert.falsy(connected)
      assert.same("session does not have a socket, create a new session first.", err)
    end)

    it("should not require port for connection", function()
      local new_session = cassandra.new()
      new_session:set_timeout(1000)
      local connected, err = new_session:connect("127.0.0.1")
      assert.truthy(connected)
    end)

    it("should be possible to send a list of hosts for connection", function()
      local new_session = cassandra.new()
      new_session:set_timeout(1000)
      local connected, err = new_session:connect({"localhost", "127.0.0.1"})
      assert.truthy(connected)
    end)

    it("should try another host if it fails to connect", function()
      local new_session = cassandra.new()
      new_session:set_timeout(1000)
      local connected, err = new_session:connect({"0.0.0.1", "0.0.0.2", "0.0.0.3", "127.0.0.1"})
      assert.truthy(connected)
      assert.falsy(err)
    end)

    it("should return error if it fails to connect to all hosts", function()
      local new_session = cassandra.new()
      new_session:set_timeout(1000)
      local connected, err = new_session:connect({"0.0.0.1", "0.0.0.2", "0.0.0.3"})
      assert.falsy(connected)
      assert.truthy(err)
    end)

    it("should fallback to luasocket when creating a session", function()
      local ngx_mock = _G.ngx
      _G.ngx = nil
      local new_session = cassandra.new()
      new_session:set_timeout(1000)
      local connected, err = new_session:connect("127.0.0.1")
      assert.truthy(connected)
      _G.ngx = ngx_mock
    end)
  end)

  describe("Query results", function()
    local rows, err

    before_each(function()
      rows, err = session:execute [[
        SELECT cql_version, native_protocol_version, release_version FROM system.local
      ]]
    end)

    it("should have a length", function()
      assert.same(1, #rows)
    end)

    describe("Result row", function()
      local row = rows[1]

      it("should access a row column by column name", function()
        assert.truthy(row.native_protocol_version == "2" or row.native_protocol_version == "3")
      end)

      it("should access a row column by index position", function()
        assert.same(row[1], row.cql_version)
        assert.same(row[2], row.native_protocol_version)
        assert.same(row[3], row.release_version)
      end)

      if (_VERSION >= "Lua 5.2") then
        it("should have the correct number of columns", function()
          assert.same(#row, 3)
        end)
      end

      it("should be iterable by key and value", function()
        local columns = {cql_version="cql_version",
                         native_protocol_version="native_protocol_version",
                         release_version="release_version"}
        local n_columns = 0
        for key, _ in pairs(row) do
          assert.same(columns[key], key)
          n_columns = n_columns + 1
        end
        assert.same(n_columns, 3)
      end)
    end)
  end)

  describe("Query tracing", function()
    it("should be queryable with tracing", function()
      local rows, err = session:execute([[
        SELECT cql_version, native_protocol_version, release_version FROM system.local
      ]], nil, {tracing=true})

      assert.truthy(rows.tracing_id)
    end)
  end)

  describe("Prepared statements", function()
    it("should support prepared statements", function()
      local stmt, err = session:prepare("SELECT native_protocol_version FROM system.local")
      assert.truthy(stmt)
      local rows = session:execute(stmt)
      assert.same(1, #rows)
      assert.truthy(rows[1].native_protocol_version == "2" or rows[1].native_protocol_version == "3")
    end)

    it("should support variadic arguments in prepared statements", function()
      local stmt, err = session:prepare("SELECT * FROM system.local WHERE key IN ?")
      assert.same(nil, err)
      assert.truthy(stmt)
      local rows = session:execute(stmt, {cassandra.list({"local", "not local"})})
      assert.same(1, #rows)
      assert.truthy(rows[1].key == "local")
    end)

    it("should support tracing for prepared statements", function()
      local stmt, err = session:prepare("SELECT native_protocol_version FROM system.local", {tracing=true})
      assert.truthy(stmt)
      assert.truthy(stmt.tracing_id)
    end)
  end)

  describe("Keyspace", function()
    it("should catch errors", function()
      local ok, err = session:set_keyspace("invalid_keyspace")
      assert.same(err, [[Cassandra returned error (Invalid): "Keyspace 'invalid_keyspace' does not exist"]])
    end)

    it("should be possible to use a namespace", function()
      local ok, err = session:set_keyspace("lua_tests")
      assert.truthy(ok)
    end)
  end)

  describe("Real use-case", function()

    setup(function()
      table_created, err = session:execute [[
        CREATE TABLE IF NOT EXISTS users (
          user_id uuid PRIMARY KEY,
          name varchar,
          age int
        )
      ]]
    end)

    teardown(function()
      session:execute("DROP TABLE users")
    end)

    describe("DDL statements", function()
      it("should be possible to create a table", function()
        assert.same("users", table_created.table)
      end)

      it("should not be possible to create a table twice", function()
        local table_created, err = session:execute [[
          CREATE TABLE users (
            user_id uuid PRIMARY KEY,
            name varchar,
            age int
          )
        ]]
        assert.is_not_true(table_created)
        assert.same(err, 'Cassandra returned error (Already_exists): "Cannot add already existing column family "users" to keyspace "lua_tests""')
      end)
    end)

    describe("DML statements", function()
      it("should be possible to insert a row", function()
        local ok, err = session:execute([[
          INSERT INTO users (name, age, user_id)
          VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
        ]])
        assert.truthy(ok)
      end)

      it("should be possible to insert a row with tracing", function()
        local query = [[
          INSERT INTO users (name, age, user_id)
          VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
        ]]
        local result, err = session:execute(query, {}, {tracing=true})
        assert.truthy(result)
        assert.truthy(result.tracing_id)
        local tracing, err = session:get_trace(result)
        assert.falsy(err)
        assert.truthy(query, tracing.query)
        assert.truthy(tracing.started_at > 0)
        assert.truthy(#tracing.events > 0)
      end)

      it("should be possible to set consistency level", function()
        local ok, err = session:execute([[
          INSERT INTO users (name, age, user_id)
          VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
        ]], {}, {consistency_level=cassandra.consistency.TWO})
        assert.same(err, 'Cassandra returned error (Unavailable exception): "Cannot achieve consistency level TWO"')
      end)

      it("should support queries with arguments", function()
        local ok, err = session:execute([[
          INSERT INTO users (name, age, user_id)
          VALUES (?, ?, ?)
        ]], {"John O'Reilly", 42, cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")})
        local users, err = session:execute("SELECT name, age, user_id from users")
        assert.same(1, #users)
        local user = users[1]
        assert.same("John O'Reilly", user.name)
        assert.same("2644bada-852c-11e3-89fb-e0b9a54a6d93", user.user_id)
        assert.same(42, user.age)
        assert.truthy(ok)
      end)
    end)
  end)

  describe("Types #types", function()
    local types = require "spec.type_fixtures"

    for _, type in ipairs(types) do
      describe("Type " .. type.name, function()

        setup(function()
          session:execute([[
            CREATE TABLE type_test_table (
              key varchar PRIMARY KEY,
              value ]] .. type.name .. [[
            )
          ]])
        end)

        teardown(function()
          session:execute("DROP TABLE type_test_table")
        end)

        it("should be possible to insert and get value back", function()
          local ok, err = session:execute([[
            INSERT INTO type_test_table (key, value)
            VALUES (?, ?)
          ]], {"key", type.insert_value ~= nil and type.insert_value or type.value})
          assert.same(nil, err)
          local rows, err = session:execute("SELECT value FROM type_test_table WHERE key = 'key'")
          assert.same(1, #rows)
          if type.read_test then
            assert.truthy(type.read_test(rows[1].value))
          else
            assert.same(type.read_value ~= nil and type.read_value or type.value, rows[1].value)
          end
        end)
      end)
    end
  end)

  describe("Pagination #pagination", function()

    setup(function()
      session:execute [[
        CREATE TABLE IF NOT EXISTS pagination_test_table(
          key int PRIMARY KEY,
          value varchar
        )
      ]]
      for i = 1, 200 do
        session:execute([[ INSERT INTO pagination_test_table(key, value)
                            VALUES(?,?) ]], { i, "test" })
      end
    end)

    teardown(function()
      session:execute("DROP TABLE pagination_test_table")
    end)

    it("should have a high default value and signal that everything is fetched", function()
      local rows, err = session:execute("SELECT * FROM pagination_test_table")
      assert.falsy(err)
      assert.same(200, #rows)
      assert.is_not_true(rows.has_more_pages)
    end)

    it("should support a page_size option", function()
      local rows, err = session:execute("SELECT * FROM pagination_test_table", nil, {page_size=200})
      assert.falsy(err)
      assert.same(200, #rows)
    end)

    it("should return metadata flags", function()
      -- Incomplete page
      local rows, err = session:execute("SELECT * FROM pagination_test_table", nil, {page_size=100})
      assert.falsy(err)
      assert.is_true(rows.meta.has_more_pages)
      assert.truthy(rows.meta.paging_state)

      -- Complete page
      local rows, err = session:execute("SELECT * FROM pagination_test_table", nil, {page_size=500})
      assert.falsy(err)
      assert.is_not_true(rows.meta.has_more_pages)
      assert.falsy(rows.meta.paging_state)
    end)

    it("should fetch the next page by passing a paging_state option", function()
      local rows_1, err = session:execute("SELECT * FROM pagination_test_table", nil, {page_size=100})
      assert.falsy(err)
      assert.same(100, #rows_1)

      local rows_2, err = session:execute("SELECT * FROM pagination_test_table", nil, {
        page_size=500,
        paging_state=rows_1.meta.paging_state
      })
      assert.falsy(err)
      assert.same(100, #rows_2)
      assert.are_not.same(rows_1, rows_2)
    end)

    it("should return an iterator if given an auto_paging option", function()
      local page_tracker = 0
      local expected_number_of_pages = 20

      for _, rows, page, err in session:execute("SELECT * FROM pagination_test_table", nil, {page_size=10, auto_paging=true}) do
        assert.falsy(err)
        page_tracker = page_tracker + 1
        assert.are.same(page_tracker, page)
        assert.are.same(10, #rows)
      end

      assert.are.same(expected_number_of_pages, page_tracker)
    end)
  end)

  describe("Counters #counters", function()

    setup(function()
      session:execute [[
        CREATE TABLE counter_test_table (
          key varchar PRIMARY KEY,
          value counter
        )
      ]]
    end)

    teardown(function()
      session:execute("DROP TABLE counter_test_table")
    end)

    after_each(function()
      session:execute("TRUNCATE counter_test_table")
    end)

    it("should be possible to increment and get value back", function()
      session:execute([[
        UPDATE counter_test_table SET value = value + ? WHERE key = ?
      ]], {{type="counter", value=10}, "key"})
      local rows, err = session:execute("SELECT value FROM counter_test_table WHERE key = 'key'")
      assert.same(1, #rows)
      assert.same(10, rows[1].value)
    end)

    it("should be possible to decrement and get value back", function()
      session:execute([[
        UPDATE counter_test_table SET value = value + ? WHERE key = ?
      ]], {{type="counter", value=-10}, "key"})
      local rows, err = session:execute("SELECT value FROM counter_test_table WHERE key = 'key'")
      assert.same(1, #rows)
      assert.same(-10, rows[1].value)
    end)
  end)

  describe("Batch statements #batch", function()

    setup(function()
      table_created, err = session:execute [[
        CREATE TABLE IF NOT EXISTS users (
          user_id uuid PRIMARY KEY,
          name varchar,
          age int
        )
      ]]
    end)

    teardown(function()
      session:execute("DROP TABLE users")
    end)

    it("should support batch statements", function()
      local batch = cassandra.BatchStatement()

      batch:add("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)",
        {"James", 32, cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")})

      local stmt, err = session:prepare("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)")
      batch:add(stmt, {"John", 45, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})

      local result, err = session:execute(batch)
      assert.falsy(err)
      assert.truthy(result)

      local users, err = session:execute("SELECT name, age, user_id from users")
      assert.same(2, #users)
      assert.same("James", users[1].name)
      assert.same("John", users[2].name)
    end)
  end)
end)
