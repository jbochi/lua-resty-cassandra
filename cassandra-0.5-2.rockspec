package = "cassandra"
version = "0.5-2"
source = {
   url = "git://github.com/jbochi/lua-resty-cassandra",
   tag = "v0.5-2"
}
description = {
   summary = "Pure Lua Cassandra - CQL client",
   detailed = [[
      Pure Cassandra driver for Lua supporting CQL 3,
      using binary protocol v2.
   ]],
   homepage = "https://github.com/jbochi/lua-resty-cassandra",
   license = "MIT/X11"
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "builtin",
   modules = {
      cassandra = "src/cassandra.lua",
      ["cassandra.constants"] = "src/constants.lua",
      ["cassandra.protocol"] = "src/protocol.lua",
      ["cassandra.decoding"] = "src/decoding.lua",
      ["cassandra.encoding"] = "src/encoding.lua"
   },
   copy_directories = { "spec" }
}
