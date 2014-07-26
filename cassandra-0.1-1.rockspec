package = "cassandra"
version = "0.1-1"
source = {
   url = "git://github.com/me/luafruits",
   tag = "v0.1"
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
   "lua ~> 5.1"
}
build = {
   type = "builtin",
   modules = {
      cassandra = "src/cassandra.lua", 
   },
   copy_directories = { "spec" }
}
