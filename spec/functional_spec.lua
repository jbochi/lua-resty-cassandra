package.path = package.path .. ";spec/?.lua"
ngx = require("fake_ngx")
local cassandra = require("cassandra")

describe("cassandra", function()
  before_each(function()
    session = cassandra.new()
    session:set_timeout(1000)
    connected, err = session:connect("127.0.0.1", 9042)
    local res, err = session:execute([[
      CREATE KEYSPACE lua_tests
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
    ]])
  end)

  it("should be possible to connect", function()
    assert.truthy(connected)
  end)

  it("should not require port for connection", function()
    local new_session = cassandra.new()
    new_session:set_timeout(1000)
    local connected, err = new_session:connect("127.0.0.1")
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
    assert.same(err, [[Cassandra returned error (Invalid): "Keyspace 'invalid_keyspace' does not exist"]])
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

    it("should not be possible to be created again", function()
      table_created, err = session:execute([[
          CREATE TABLE users (
            user_id uuid PRIMARY KEY,
            name varchar,
            age int
          )
      ]])
      assert.same(err, 'Cassandra returned error (Already_exists): "Cannot add already existing column family "users" to keyspace "lua_tests""')
    end)

    it("should be possible to insert a row", function()
      local ok, err = session:execute([[
        INSERT INTO users (name, age, user_id)
        VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
      ]])
      assert.truthy(ok)
    end)

    it("should be possible to insert a row", function()
      local ok, err = session:execute([[
        INSERT INTO users (name, age, user_id)
        VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
      ]])
      assert.truthy(ok)
    end)

    it("should be possible to set consistency level", function()
      local ok, err = session:execute([[
        INSERT INTO users (name, age, user_id)
        VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)
      ]], {}, cassandra.consistency.TWO)
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

  local types = {
    {name='ascii', insert_value='string', read_value='string'},
    {name='ascii', insert_value=cassandra.null, read_value=nil},
    {name='bigint', insert_value=cassandra.bigint(42000000000), read_value=42000000000},
    {name='bigint', insert_value=cassandra.bigint(-42000000000), read_value=-42000000000},
    {name='bigint', insert_value=cassandra.bigint(-42), read_value=-42},
    {name='blob', insert_value="\005\042", read_value="\005\042"},
    {name='blob', insert_value=string.rep("blob", 10000), read_value=string.rep("blob", 10000)},
    {name='boolean', insert_value=true, read_value=true},
    {name='boolean', insert_value=false, read_value=false},
    -- counters are not here because they are used with UPDATE instead of INSERT
    -- todo: decimal,
    {name='double', insert_value=cassandra.double(1.0000000000000004), read_test=function(value) return math.abs(value - 1.0000000000000004) < 0.000000000000001 end},
    {name='double', insert_value=cassandra.double(-1.0000000000000004), read_value=-1.0000000000000004},
    {name='double', insert_value=cassandra.double(0), read_test=function(value) return math.abs(value - 0) < 0.000000000000001 end},
    {name='double', insert_value=cassandra.double(314151), read_test=function(value) return math.abs(value - 314151) < 0.000000000000001 end},
    {name='float', insert_value=3.14151, read_test=function(value) return math.abs(value - 3.14151) < 0.0000001 end},
    {name='float', insert_value=cassandra.float(3.14151), read_test=function(value) return math.abs(value - 3.14151) < 0.0000001 end},
    {name='float', insert_value=cassandra.float(0), read_test=function(value) return math.abs(value - 0) < 0.0000001 end},
    {name='float', insert_value=-3.14151, read_test=function(value) return math.abs(value + 3.14151) < 0.0000001 end},
    {name='float', insert_value=cassandra.float(314151), read_test=function(value) return math.abs(value - 314151) < 0.0000001 end},
    {name='int', insert_value=4200, read_value=4200},
    {name='int', insert_value=-42, read_value=-42},
    {name='text', insert_value='string', read_value='string'},
    {name='timestamp', insert_value=cassandra.timestamp(1405356926), read_value=1405356926},
    {name='uuid', insert_value=cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"), read_value="1144bada-852c-11e3-89fb-e0b9a54a6d11"},
    {name='varchar', insert_value='string', read_value='string'},
    {name='blob', insert_value=string.rep("string", 10000), read_value=string.rep("string", 10000)},
    {name='varint', insert_value=4200, read_value=4200},
    {name='varint', insert_value=-42, read_value=-42},
    {name='timeuuid', insert_value=cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"), read_value="1144bada-852c-11e3-89fb-e0b9a54a6d11"},
    {name='inet', insert_value=cassandra.inet("127.0.0.1"), read_value="127.0.0.1"},
    {name='inet', insert_value=cassandra.inet("2001:0db8:85a3:0042:1000:8a2e:0370:7334"), read_value="2001:0db8:85a3:0042:1000:8a2e:0370:7334"},
    {name='list<text>', insert_value=cassandra.list({'abc', 'def'}), read_value={'abc', 'def'}},
    {name='list<int>', insert_value=cassandra.list({4, 2, 7}), read_value={4, 2, 7}},
    {name='map<text,text>', insert_value=cassandra.map({k1='v1', k2='v2'}), read_value={k1='v1', k2='v2'}},
    {name='map<text,int>', insert_value=cassandra.map({k1=3, k2=4}), read_value={k1=3, k2=4}},
    {name='map<text,text>', insert_value=cassandra.map({}), read_value=nil},
    {name='set<text>', insert_value=cassandra.set({'abc', 'def'}), read_value={'abc', 'def'}}
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
        local ok, err = session:execute([[
          INSERT INTO type_test_table (key, value)
          VALUES (?, ?)
        ]], {"key", type.insert_value})
        assert.same(nil, err)
        local rows, err = session:execute("SELECT value FROM type_test_table WHERE key = 'key'")
        assert.same(1, #rows)
        if type.read_test then
          assert.truthy(type.read_test(rows[1].value))
        else
          assert.same(type.read_value, rows[1].value)
        end
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
        CREATE TABLE counter_test_table (
          key varchar PRIMARY KEY,
          value counter
        )
      ]])
    end)
    it("should be possible to increment and get value back", function()
      session:execute([[
        UPDATE counter_test_table
        SET value = value + ?
        WHERE key = ?
      ]], {{type="counter", value=10}, "key"})
      local rows, err = session:execute("SELECT value FROM counter_test_table WHERE key = 'key'")
      assert.same(1, #rows)
      assert.same(10, rows[1].value)
    end)
    it("should be possible to decrement and get value back", function()
      session:execute([[
        UPDATE counter_test_table
        SET value = value + ?
        WHERE key = ?
      ]], {{type="counter", value=-10}, "key"})
      local rows, err = session:execute("SELECT value FROM counter_test_table WHERE key = 'key'")
      assert.same(1, #rows)
      assert.same(-10, rows[1].value)
    end)
    after_each(function()
      session:execute("DROP TABLE counter_test_table")
    end)
  end)
end)
