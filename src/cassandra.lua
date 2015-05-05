-- Implementation of CQL Binary protocol V2 available at:
-- https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec;hb=HEAD

local protocol = require("cassandra.protocol")
local encoding = require("cassandra.encoding")
local constants = require("cassandra.constants")

local CQL_VERSION = "3.0.0"

math.randomseed(ngx and ngx.time() or os.time())

local _M = {
  version="0.5-7",
  consistency=constants.consistency,
  batch_types=constants.batch_types
}

-- create functions for type annotations
for key, _ in pairs(constants.types) do
  _M[key] = function(value)
    return {type=key, value=value}
  end
end

_M.null = {type="null", value=nil}

---
--- SOCKET METHODS
---

local mt = {__index=_M}

function _M.new()
  local tcp
  if ngx and ngx.get_phase ~= nil and ngx.get_phase() ~= "init" then
    -- openresty
    tcp = ngx.socket.tcp
  else
    -- fallback to luasocket
    -- It's also a fallback for openresty in the
    -- "init" phase that doesn't support Cosockets
    tcp = require("socket").tcp
  end

  local sock, err = tcp()
  if not sock then
    return nil, err
  end

  return setmetatable({sock=sock}, mt)
end

local function shuffle(t)
  -- see: http://en.wikipedia.org/wiki/Fisher-Yates_shuffle
  local n = #t
  while n >= 2 do
    local k = math.random(n)
    t[n], t[k] = t[k], t[n]
    n = n - 1
  end
  return t
end

local function startup(self)
  local body = encoding.string_map_representation({["CQL_VERSION"]=CQL_VERSION})
  local response, err = protocol.send_frame_and_get_response(self,
    constants.op_codes.STARTUP, body)
  if not response then
    return nil, err
  end
  if response.op_code ~= constants.op_codes.READY then
    error("Server is not ready")
  end
  return true
end

local function split_by_port(str)
  local fields = {}
  str:gsub("([^:]+)", function(c) fields[#fields+1] = c end)
  return fields[1], fields[2]
end

function _M:connect(contact_points, port)
  if port == nil then port = 9042 end
  if contact_points == nil then
    return nil, "no contact points provided"
  elseif type(contact_points) == 'table' then
    -- shuffle the contact points so we don't try
    -- to connect always on the same order, avoiding
    -- pressure on the same node cordinator
    shuffle(contact_points)
  else
    contact_points = {contact_points}
  end
  local sock = self.sock
  if not sock then
    return nil, "session does not have a socket, create a new session first."
  end
  local ok, err
  for _, contact_point in ipairs(contact_points) do
    -- Extract port if string is of the form "host:port"
    local host, host_port = split_by_port(contact_point)
    -- Default port is the one given as parameter
    if not host_port then
      host_port = port
    end
    ok, err = sock:connect(host, host_port)
    if ok then
      self.host = host
      break
    end
  end
  if not ok then
    return false, err
  end
  if not self.initialized then
    --todo: not tested
    startup(self)
    self.initialized = true
  end
  return true
end

function _M:set_timeout(timeout)
  local sock = self.sock
  if not sock then
    return nil, "not initialized"
  end

  return sock:settimeout(timeout)
end

function _M:set_keepalive(...)
  local sock = self.sock
  if not sock then
    return nil, "not initialized"
  elseif sock.setkeepalive then
    return sock:setkeepalive(...)
  end
  return nil, "luasocket does not support reusable sockets"
end

function _M:get_reused_times()
  local sock = self.sock
  if not sock then
    return nil, "not initialized"
  elseif sock.getreusedtimes then
    return sock:getreusedtimes()
  end
  return nil, "luasocket does not support reusable sockets"
end

function _M:close()
  local sock = self.sock
  if not sock then
    return nil, "not initialized"
  end

  return sock:close()
end

---
--- CLIENT METHODS
---

local batch_statement_mt = {
  __index={
    add=function(self, query, args)
      table.insert(self.queries, {query=query, args=args})
    end,
    representation=function(self)
      return encoding.batch_representation(self.queries, self.type)
    end,
    is_batch_statement = true
  }
}

function _M.BatchStatement(batch_type)
  if not batch_type then
    batch_type = constants.batch_types.LOGGED
  end

  return setmetatable({type=batch_type, queries={}}, batch_statement_mt)
end

function _M:prepare(query, options)
  if not options then options = {} end
  local body = encoding.long_string_representation(query)
  local response, err = protocol.send_frame_and_get_response(self,
    constants.op_codes.PREPARE, body, options.tracing)
  if not response then
    return nil, err
  end
  if response.op_code ~= constants.op_codes.RESULT then
    error("Result expected")
  end
  return protocol.parse_prepared_response(response)
end

-- Default query options
local default_options = {
  consistency_level=constants.consistency.ONE,
  page_size=5000,
  auto_paging=false,
  tracing=false
}

function _M:execute(query, args, options)
  local op_code = protocol.query_op_code(query)
  if not options then options = {} end

  -- Default options
  for k, v in pairs(default_options) do
    if options[k] == nil then
      options[k] = v
    end
  end

  if options.auto_paging then
    local page = 0
    return function(query, paging_state)
      -- Latest fetched rows have been returned for sure, end the iteration
      if not paging_state and page > 0 then return nil end

      local rows, err = self:execute(query, args, {
        page_size=options.page_size,
        paging_state=paging_state
      })
      page = page + 1

      -- If we have some results, retrieve the paging_state
      local paging_state
      if rows ~= nil then
        paging_state = rows.meta.paging_state
      end

      -- Allow the iterator to return the latest page of rows or an error
      if err or (paging_state == nil and rows and #rows > 0) then
        paging_state = false
      end

      return paging_state, rows, page, err
    end, query, nil
  end

  local frame_body = protocol.frame_body(query, args, options)

  -- Send frame
  local response, err = protocol.send_frame_and_get_response(self, op_code, frame_body, options.tracing)

  -- Check response errors
  if not response then
    return nil, err
  elseif response.op_code ~= constants.op_codes.RESULT then
    error("Result expected")
  end

  return protocol.parse_response(response)
end

function _M:set_keyspace(keyspace_name)
  return self:execute("USE " .. keyspace_name)
end

function _M:get_trace(result)
  if not result.tracing_id then
    return nil, "No tracing available"
  end
  local rows, err = self:execute([[
    SELECT coordinator, duration, parameters, request, started_at
      FROM  system_traces.sessions WHERE session_id = ?]],
    {_M.uuid(result.tracing_id)})
  if not rows then
    return nil, "Unable to get trace: " .. err
  end
  if #rows == 0 then
    return nil, "Trace not found"
  end
  local trace = rows[1]
  trace.events, err = self:execute([[
    SELECT event_id, activity, source, source_elapsed, thread
      FROM system_traces.events WHERE session_id = ?]],
    {_M.uuid(result.tracing_id)})
  if not trace.events then
    return nil, "Unable to get trace events: " .. err
  end
  return trace
end

return _M
