local constants = require("cassandra.constants")
local encoding = require("cassandra.encoding")
local decoding = require("cassandra.decoding")

local error_mt = {}
error_mt = {
  __tostring = function(self)
    return self.message
  end,
  __concat = function (a, b)
    if getmetatable(a) == error_mt then
      return a.message .. b
    else
      return a .. b.message
    end
  end
}

local function cassandra_error(message, code, raw_message)
  return setmetatable({
    message=message,
    code=code,
    raw_message=raw_message
  }, error_mt)
end

local function read_error(buffer)
  local error_code = decoding.read_int(buffer)
  local error_code_translation = constants.error_codes_translation[error_code]
  local error_message = decoding.read_string(buffer)
  return cassandra_error(
    'Cassandra returned error (' .. error_code_translation .. '): "' .. error_message .. '"',
    error_code,
    error_message
  )
end

local function read_frame(self)
  local header, err = self.sock:receive(8)
  if not header then
    return nil, string.format("Failed to read frame header from %s: %s", self.host, err)
  end
  local header_buffer = decoding.create_buffer(header)
  local version = decoding.read_raw_byte(header_buffer)
  local flags = decoding.read_raw_byte(header_buffer)
  local stream = decoding.read_raw_byte(header_buffer)
  local op_code = decoding.read_raw_byte(header_buffer)
  local length = decoding.read_int(header_buffer)
  local body, tracing_id
  if length > 0 then
    body, err = self.sock:receive(length)
    if not body then
      return nil, string.format("Failed to read frame body from %s: %s", self.host, err)
    end
  else
    body = ""
  end
  if version ~= constants.version_codes.RESPONSE then
    error("Invalid response version")
  end
  local body_buffer = decoding.create_buffer(body)
  if flags == 0x02 then -- tracing
    tracing_id = decoding.read_uuid(string.sub(body, 1, 16))
    body_buffer.pos = 17
  end
  if op_code == constants.op_codes.ERROR then
    return nil, read_error(body_buffer)
  end
  return {
    flags=flags,
    stream=stream,
    op_code=op_code,
    buffer=body_buffer,
    tracing_id=tracing_id
  }
end

local function hasbit(x, p)
  return x % (p + p) >= p
end

local function setbit(x, p)
  return hasbit(x, p) and x or x + p
end

local function parse_metadata(buffer)
  -- Flags parsing
  local flags = decoding.read_int(buffer)
  local global_tables_spec = hasbit(flags, constants.rows_flags.GLOBAL_TABLES_SPEC)
  local has_more_pages = hasbit(flags, constants.rows_flags.HAS_MORE_PAGES)
  local columns_count = decoding.read_int(buffer)

  -- Paging metadata
  local paging_state
  if has_more_pages then
    paging_state = decoding.read_bytes(buffer)
  end

  -- global_tables_spec metadata
  local global_keyspace_name, global_table_name
  if global_tables_spec then
    global_keyspace_name = decoding.read_string(buffer)
    global_table_name = decoding.read_string(buffer)
  end

  -- Columns metadata
  local columns = {}
  for _ = 1, columns_count do
    local ksname = global_keyspace_name
    local tablename = global_table_name
    if not global_tables_spec then
      ksname = decoding.read_string(buffer)
      tablename = decoding.read_string(buffer)
    end
    local column_name = decoding.read_string(buffer)
    columns[#columns + 1] = {
      keyspace=ksname,
      table=tablename,
      name=column_name,
      type=decoding.read_option(buffer)
    }
  end

  return {
    columns_count=columns_count,
    columns=columns,
    has_more_pages=has_more_pages,
    paging_state=paging_state
  }
end

local function parse_rows(buffer, metadata)
  local columns = metadata.columns
  local columns_count = metadata.columns_count
  local rows_count = decoding.read_int(buffer)
  local values = {}
  local row_mt = {
    __index = function(t, i)
      -- allows field access by position/index, not column name only
      local column = columns[i]
      if column then
        return t[column.name]
      end
      return nil
    end,
    __len = function() return columns_count end
  }
  for _ = 1, rows_count do
    local row = {}
    setmetatable(row, row_mt)
    for j = 1, columns_count do
      local value = decoding.read_value(buffer, columns[j].type)
      row[columns[j].name] = value
    end
    values[#values + 1] = row
  end
  assert(buffer.pos == #(buffer.str) + 1)
  return values
end

-- Represent a <query_parameters>
-- <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
local function query_parameters_representation(args, options)
  -- <flags>
  local flags_repr = 0

  if args then
    flags_repr = setbit(flags_repr, constants.query_flags.VALUES)
  end

  local result_page_size = ""
  if options.page_size > 0 then
    flags_repr = setbit(flags_repr, constants.query_flags.PAGE_SIZE)
    result_page_size = encoding.int_representation(options.page_size)
  end

  local paging_state = ""
  if options.paging_state then
    flags_repr = setbit(flags_repr, constants.query_flags.PAGING_STATE)
    paging_state = encoding.bytes_representation(options.paging_state)
  end

  -- <query_parameters>
  return encoding.short_representation(options.consistency_level) ..
    string.char(flags_repr) .. encoding.values_representation(args) ..
    result_page_size .. paging_state
end

-- Represents <query><query_parameters>
local function query_representation(query, args, options)
  return encoding.long_string_representation(query) .. query_parameters_representation(args, options)
end

-- Represents <id><query_parameters>
local function execute_representation(id, args, options)
  return encoding.short_bytes_representation(id) .. query_parameters_representation(args, options)
end

-- Represents <type><n><query_1>...<query_n><consistency>
-- where <query_n> must be
--   <kind><string_or_id><n><value_1>...<value_n>
local function batch_representation(batch, options)
  local queries = batch.queries
  local b = {}
  -- <type>
  b[#b + 1] = string.char(batch.type)
  -- <n> (number of queries)
  b[#b + 1] = encoding.short_representation(#queries)
  -- <query_n> (operations)
  for _, query in ipairs(queries) do
    local kind
    local string_or_id
    if type(query.query) == "string" then
      kind = encoding.boolean_representation(false)
      string_or_id = encoding.long_string_representation(query.query)
    else
      kind = encoding.boolean_representation(true)
      string_or_id = encoding.short_bytes_representation(query.query.id)
    end

    -- The behaviour is sligthly different than from <query_parameters>
    -- for <query_parameters>:
    --   [<n><value_1>...<value_n>] (n cannot be 0), otherwise is being mixed up with page_size
    -- for batch <query_n>:
    --   <kind><string_or_id><n><value_1>...<value_n> (n can be 0, but is required)
    if query.args then
      b[#b + 1] = kind .. string_or_id .. encoding.values_representation(query.args)
    else
      b[#b + 1] = kind .. string_or_id .. encoding.short_representation(0)
    end
  end

  -- <type><n><query_1>...<query_n><consistency>
  return table.concat(b)..encoding.short_representation(options.consistency_level)
end

--
-- Protocol exposed methods
--

local _M = {}

function _M.op_code_and_frame_body(op, args, options)
  local op_code, representation
  -- Determine if op is a query, statement, or batch
  if type(op) == "string" then
    op_code = constants.op_codes.QUERY
    representation = query_representation(op, args, options)
  elseif op.is_batch_statement then
    op_code = constants.op_codes.BATCH
    representation = batch_representation(op, options)
  else
    op_code = constants.op_codes.EXECUTE
    representation = execute_representation(op.id, args, options)
  end

  return op_code, representation
end

function _M.parse_prepared_response(response)
  local buffer = response.buffer
  local kind = decoding.read_int(buffer)
  local result = {}
  if kind == constants.result_kinds.PREPARED then
    local id = decoding.read_short_bytes(buffer)
    local metadata = parse_metadata(buffer)
    local result_metadata = parse_metadata(buffer)
    assert(buffer.pos == #(buffer.str) + 1)
    result = {
      type="PREPARED",
      id=id,
      metadata=metadata,
      result_metadata=result_metadata
    }
  else
    error("Invalid result kind")
  end
  if response.tracing_id then result.tracing_id = response.tracing_id end
  return result
end

function _M.parse_response(response)
  local result
  local buffer = response.buffer
  local kind = decoding.read_int(buffer)
  if kind == constants.result_kinds.VOID then
    result = {
      type="VOID"
    }
  elseif kind == constants.result_kinds.ROWS then
    local metadata = parse_metadata(buffer)
    result = parse_rows(buffer, metadata)
    result.type = "ROWS"
    result.meta = {
      has_more_pages=metadata.has_more_pages,
      paging_state=metadata.paging_state
    }
  elseif kind == constants.result_kinds.SET_KEYSPACE then
    result = {
      type="SET_KEYSPACE",
      keyspace=decoding.read_string(buffer)
    }
  elseif kind == constants.result_kinds.SCHEMA_CHANGE then
    result = {
      type="SCHEMA_CHANGE",
      change=decoding.read_string(buffer),
      keyspace=decoding.read_string(buffer),
      table=decoding.read_string(buffer)
    }
  else
    error(string.format("Invalid result kind: %x", kind))
  end

  if response.tracing_id then
    result.tracing_id = response.tracing_id
  end
  return result
end

function _M.send_frame_and_get_response(self, op_code, body, tracing)
  local version = string.char(constants.version_codes.REQUEST)
  local flags = tracing and constants.flags.tracing or '\000'
  local stream_id = '\000'
  local length = encoding.int_representation(#body)
  local frame = version .. flags .. stream_id .. string.char(op_code) .. length .. body

  local bytes, err = self.sock:send(frame)
  if not bytes then
    return nil, string.format("Failed to read frame header from %s: %s", self.host, err)
  end
  return read_frame(self)
end

return _M
