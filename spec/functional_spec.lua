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

  it("should be queryble", function()
    local rows, err = session:execute("SELECT cql_version, native_protocol_version, release_version FROM system.local");
    assert.same(1, #rows)
    assert.same(rows[1].native_protocol_version, "2")
  end)

  it("should catch errors", function()
    local ok, err = pcall(session.set_keyspace, session, "invalid_keyspace")
    assert.truthy(string.find(err, "Keyspace 'invalid_keyspace' does not exist"))
  end)

  it("should be possible to use a namespace", function()
    local ok, err = session:set_keyspace("lua_tests")
    assert.truthy(ok)
  end)

  describe("a table", function()
    before_each(function()
      session:set_keyspace("lua_tests")
      session:execute("DROP TABLE users")
    end)

    it("should be possible to be created", function()
      local res, err = session:execute([[
          CREATE TABLE users (
            user varchar PRIMARY KEY,
            age int,
            email varchar
          )
      ]])
      assert.same("lua_tests.users CREATED", res)
    end)
  end)
end)
