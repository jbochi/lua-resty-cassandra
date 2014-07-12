package.path = package.path .. ";spec/?.lua"
ngx = require("fake_ngx")
local cassandra = require("cassandra")

describe("cassandra", function()
  before_each(function()
    session = cassandra.new()
    session:set_timeout(1000)
    connected, err = session:connect("127.0.0.1", 9042)
    local res, err = pcall(session.execute, session, [[
      CREATE KEYSPACE lua_tests
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
    ]])
  end)

  it("should be possible to connect", function()
    assert.truthy(connected)
  end)

  it("should be queryable", function()
    local rows, err = session:execute("SELECT cql_version, native_protocol_version, release_version FROM system.local");
    assert.same(1, #rows)
    assert.same(rows[1].native_protocol_version, "2")
  end)

  it("should support prepared statements", function()
    local stmt, err = session:prepare("SELECT native_protocol_version FROM system.local");
    assert.truthy(stmt)
    local rows = session:execute(stmt)
    assert.same(1, #rows)
    assert.same(rows[1].native_protocol_version, "2")
  end)

  it("should catch errors", function()
    local ok, err = session:set_keyspace("invalid_keyspace")
    assert.truthy(string.find(err, "Keyspace 'invalid_keyspace' does not exist"))
  end)

  it("should be possible to use a namespace", function()
    local ok, err = session:set_keyspace("lua_tests")
    assert.truthy(ok)
  end)

  describe("a table", function()
    before_each(function()
      session:set_keyspace("lua_tests")
      local res, err = session:execute("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='lua_tests' and columnfamily_name='users'")
      if #res > 0 then
        session:execute("DROP TABLE users")
      end
      table_created, err = session:execute([[
          CREATE TABLE users (
            user_id uuid PRIMARY KEY,
            name varchar,
            age int
          )
      ]])
    end)

    it("should be possible to be created", function()
      assert.same("lua_tests.users CREATED", table_created)
    end)

    it("should be possible to insert a row", function()
      local ok, err = session:execute([[
        INSERT INTO users (name, age, user_id)
        VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
      ]])
      assert.truthy(ok)
    end)

    it("should support queries with arguments", function()
      local ok, err = session:execute([[
        INSERT INTO users (name, age, user_id)
        VALUES (?, ?, ?)
      ]], {"Juarez S' Bochi", 31, {type="uuid", value="1144bada-852c-11e3-89fb-e0b9a54a6d11"}})
      local users, err = session:execute("SELECT name, age, user_id from users")
      assert.same(1, #users)
      local user = users[1]
      assert.same("Juarez S' Bochi", user.name)
      assert.same("1144bada-852c-11e3-89fb-e0b9a54a6d11", user.user_id)
      assert.same(31, user.age)
      assert.truthy(ok)
    end)
  end)

  local types = {
    {name='ascii', insert_value='juarez', read_value='juarez'},
    {name='bigint', insert_value={type='bigint', value=42000000000}, read_value=42000000000},
    -- todo: signed bigint,
    {name='blob', insert_value="\005\042", read_value="\005\042"},
    {name='blob', insert_value=string.rep("blob", 10000), read_value=string.rep("blob", 10000)},
    {name='boolean', insert_value=true, read_value=true},
    {name='boolean', insert_value=false, read_value=false},
    -- counters are not here because they are used with UPDATE instead of INSERT
    -- todo: decimal,
    -- todo: double,
    -- todo: float,
    {name='int', insert_value=4200, read_value=4200},
    {name='int', insert_value=-42, read_value=-42},
    {name='text', insert_value='juarez', read_value='juarez'},
    -- todo: timestamp,
    {name='uuid', insert_value={type='uuid', value="1144bada-852c-11e3-89fb-e0b9a54a6d11"}, read_value="1144bada-852c-11e3-89fb-e0b9a54a6d11"},
    {name='varchar', insert_value='juarez', read_value='juarez'},
    {name='blob', insert_value=string.rep("juarez", 10000), read_value=string.rep("juarez", 10000)},
    -- todo: varint,
    -- todo: timeuuid,
    -- todo: inet,
    -- todo: list,
    -- todo: map,
    -- todo: set
  }

  for _, type in ipairs(types) do
    describe("the type " .. type.name, function()
      before_each(function()
        session:set_keyspace("lua_tests")
        session:execute([[
          CREATE TABLE type_test_table (
            key varchar PRIMARY KEY,
            value ]] .. type.name .. [[
          )
        ]])
      end)
      it("should be possible to insert and get value back", function()
        session:execute([[
          INSERT INTO type_test_table (key, value)
          VALUES (?, ?)
        ]], {"key", type.insert_value})
        local rows, err = session:execute("SELECT value FROM type_test_table WHERE key = 'key'")
        assert.same(1, #rows)
        assert.same(type.read_value, rows[1].value)
      end)
      after_each(function()
        session:execute("DROP TABLE type_test_table")
      end)
    end)
  end

  describe("counters", function()
    before_each(function()
      session:set_keyspace("lua_tests")
      session:execute([[
        CREATE TABLE type_test_table (
          key varchar PRIMARY KEY,
          value counter
        )
      ]])
    end)
    it("should be possible to increment and get value back", function()
      session:execute([[
        UPDATE type_test_table
        SET value = value + ?
        WHERE key = ?
      ]], {{type="counter", value=10}, "key"})
      local rows, err = session:execute("SELECT value FROM type_test_table WHERE key = 'key'")
      assert.same(1, #rows)
      assert.same(10, rows[1].value)
    end)
    after_each(function()
      session:execute("DROP TABLE type_test_table")
    end)
  end)
end)
