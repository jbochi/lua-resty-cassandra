package.path = package.path .. ";spec/?.lua"
ngx = require("fake_ngx")
local cassandra = require("cassandra")

describe("cassandra", function()
  before_each(function()
    session = cassandra.new()
    session:set_timeout(1000)
    connected, err = session:connect("127.0.0.1", 9042)
  end)

  it("should be possible to connect", function()
    assert.truthy(connected)
  end)

  it("should be queryble", function()
    local rows, err = session:execute("select cql_version, native_protocol_version, release_version from system.local");
    assert.same(1, #rows)
    assert.same(rows[1].native_protocol_version, "2")
  end)
end)
