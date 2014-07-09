-- Implementation of CQL Binary protocol V2 available at https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec;hb=HEAD

local tcp = ngx.socket.tcp


local _M = {}

_M.version = "0.0.1"
local CQL_VERSION = "3.0.0"

local version_codes = {
    REQUEST='\002',
    RESPONSE='\130', --\x82
}

local op_codes = {
    ERROR='\000',
    STARTUP='\001',
    READY='\002',
    AUTHENTICATE='\003',
    --\004
    OPTIONS='\005',
    SUPPORTED='\006',
    QUERY='\007',
    RESULT='\008',
    PREPARE='\009',
    EXECUTE='\010',
    REGISTER='\011',
    EVENT='\012',
    BATCH='\013',
    AUTH_CHALLENGE='\014',
    AUTH_RESPONSE='\015',
    AUTH_SUCCESS='\016',
}

local consistency = {
    ANY='\000\000',
    ONE='\000\001',
    TWO='\000\002',
    THREE='\000\003',
    QUORUM='\000\004',
    ALL='\000\005',
    LOCAL_QUORUM='\000\006',
    EACH_QUORUM='\000\007',
    SERIAL='\000\008',
    LOCAL_SERIAL='\000\009',
    LOCAL_ONE='\000\010'
}

local result_kinds = {
    VOID=1,
    ROWS=2,
    SET_KEYSPACE=3,
    PREPARED=4,
    SCHEMA_CHANGE=5
}

local types = {
    custom=0,
    ascii=1,
    bigint=2,
    blob=3,
    boolean=4,
    counter=5,
    decimal=6,
    double=7,
    float=8,
    int=9,
    text=10,
    timestamp=11,
    uuid=12,
    varchar=13,
    varint=14,
    timeuuid=15,
    inet=16,
    list=32,
    map=33,
    set=34
}

local mt = { __index = _M }

---
--- SOCKET METHODS
---

function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end

function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

function _M.connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local ok, err = sock:connect(...)
    if not ok then
        return false, err
    end
    return self:startup()
end

function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


local function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end
_M.close = close

---
--- ENCODE FUNCTIONS
---

local function big_endian_representation(num, bytes)
    local t = {}
    while num > 0 do
        local rest = math.fmod(num, 256)
        table.insert(t, 1, string.char(rest))
        num = (num-rest) / 256
    end
    local padding = string.rep(string.char(0), bytes - #t)
    return padding .. table.concat(t)
end

local function int_representation(num)
    return big_endian_representation(num, 4)
end

local function short_representation(num)
    return big_endian_representation(num, 2)
end

local function uuid_representation(value)
    local str = string.gsub(value, "-", "")
    local buffer = {}
    for i = 1, #str, 2 do
        local byte_str =  string.sub(str, i, i + 1)
        buffer[#buffer + 1] = string.char(tonumber(byte_str, 16))
    end
    return table.concat(buffer)
end

local function string_representation(str)
    return short_representation(#str) .. str
end

local function long_string_representation(str)
    return int_representation(#str) .. str
end

local function bytes_representation(bytes)
    return int_representation(#bytes) .. bytes
end

local function short_bytes_representation(bytes)
    return short_representation(#bytes) .. bytes
end

local function string_map_representation(map)
    local buffer = {}
    local n = 0
    for k, v in pairs(map) do
        buffer[#buffer + 1] = string_representation(k)
        buffer[#buffer + 1] = string_representation(v)
        n = n + 1
    end
    return short_representation(n) .. table.concat(buffer)
end

local function value_representation(value)
    local representation = value
    if type(value) == 'number' then
        representation = int_representation(value)
    elseif type(value) == 'table' and value.type == 'bigint' then
        representation = big_endian_representation(value.value, 8)
    elseif type(value) == 'table' and value.type == 'uuid' then
        representation = uuid_representation(value.value)
    else
        representation = value
    end
    return bytes_representation(representation)
end

---
--- DECODE FUNCTIONS
---

local function string_to_number(str)
    local number = 0
    local exponent = 1
    for i = #str, 1, -1 do
        number = number + string.byte(str, i) * exponent
        exponent = exponent * 256
    end
    return number
end

local function create_buffer(str)
    return {str=str, pos=1}
end

local function read_raw_bytes(buffer, n_bytes)
    local bytes = string.sub(buffer.str, buffer.pos, buffer.pos + n_bytes - 1)
    buffer.pos = buffer.pos + n_bytes
    return bytes
end

local function read_raw_byte(buffer)
    return read_raw_bytes(buffer, 1)
end

local function read_int(buffer)
    return string_to_number(read_raw_bytes(buffer, 4))
end

local function read_short(buffer)
    return string_to_number(read_raw_bytes(buffer, 2))
end

local function read_string(buffer)
    local str_size = read_short(buffer)
    return read_raw_bytes(buffer, str_size)
end

local function read_bytes(buffer)
    local size = read_int(buffer)
    return read_raw_bytes(buffer, size)
end

local function read_short_bytes(buffer)
    local size = read_short(buffer)
    return read_raw_bytes(buffer, size)
end

local function read_option(bufffer)
    local type_id = read_short(buffer)
    local type_value = nil
    if type_id == types.custom then
        type_value = read_string(buffer)
    elseif type_id == types.list then
        type_value = read_option(buffer)
    elseif type_id == types.map then
        type_value = {read_option(buffer), read_option(buffer)}
    elseif type_id == types.set then
        type_value = {read_option(buffer)}
    end
    return {id=type_id, value=type_value}
end

local function read_uuid(bytes)
    buffer = {}
    for i = 1, #bytes do
        buffer[i] = string.format("%02x", string.byte(bytes, i))
    end
    table.insert(buffer, 5, "-")
    table.insert(buffer, 8, "-")
    table.insert(buffer, 11, "-")
    table.insert(buffer, 14, "-")
    return table.concat(buffer)
end

local function read_value(buffer, type)
    bytes = read_bytes(buffer)
    if type.id == types.int or type.id == types.bigint then
        return string_to_number(bytes)
    elseif type.id == types.uuid then
        return read_uuid(bytes)
    end
    return bytes
end

local function read_frame(self)
    local header, err, partial = self.sock:receive(8)
    if not header then
        error("Failed to read frame:" .. err)
    end
    local header_buffer = create_buffer(header)
    local version = read_raw_byte(header_buffer)
    local flags = read_raw_byte(header_buffer)
    local stream = read_raw_byte(header_buffer)
    local op_code = read_raw_byte(header_buffer)
    local length = read_int(header_buffer)
    local body, err, partial
    if length > 0 then
        body, err, partial = self.sock:receive(length)
        if not body then
            error("Failed to read body:" .. err)
        end
    else
        body = ""
    end
    if version ~= version_codes.RESPONSE then
        error("Invalid response version")
    end
    local body_buffer = create_buffer(body)
    if op_code == op_codes.ERROR then
        local error_code = read_int(body_buffer)
        local hex_error_code = string.format("%x", error_code)
        local error_message = read_string(body_buffer)
        error('Cassandra returned error (' .. hex_error_code .. '): "' .. error_message .. '"')
    end
    return {
        flags=flags,
        stream=stream,
        op_code=op_code,
        buffer=body_buffer
    }
end

---
--- BITS methods
--- http://ricilake.blogspot.com.br/2007/10/iterating-bits-in-lua.html
---

function bit(p)
  return 2 ^ (p - 1)
end

function hasbit(x, p)
  return x % (p + p) >= p
end

---
--- CLIENT METHODS
---

local function send_reply_and_get_response(self, op_code, body)
    local version = version_codes.REQUEST
    local flags = '\000'
    local stream_id = '\000'
    local length = int_representation(#body)
    local frame = version .. flags .. stream_id .. op_code .. length .. body

    local bytes, err = self.sock:send(frame)
    if not bytes then
        error("Failed to send data to cassandra: " .. err)
    end
    return read_frame(self)
end

function _M.startup(self)
    local body = string_map_representation({["CQL_VERSION"]=CQL_VERSION})
    local response = send_reply_and_get_response(self, op_codes.STARTUP, body)
    if response.op_code ~= op_codes.READY then
        error("Server is not ready")
    end
    return true
end

local function parse_metadata(buffer)
    local flags = read_int(buffer)
    local global_tables_spec = hasbit(flags, bit(1))
    local has_more_pages = hasbit(flags, bit(2))
    local no_metadata = hasbit(flags, bit(3))
    local columns_count = read_int(buffer)
    local paging_state = nil
    if has_more_pages then
        paging_state = read_bytes(buffer)
    end
    local global_keyspace_name = nil
    local global_table_name = nil
    if global_tables_spec then
        global_keyspace_name = read_string(buffer)
        global_table_name = read_string(buffer)
    end
    local columns = {}
    for j = 1, columns_count do
        local ksname = global_ksname
        local tablename = global_tablename
        if not global_tables_spec then
            ksname = read_string(buffer)
            tablename = read_string(buffer)
        end
        local column_name = read_string(buffer)
        local type = read_option(buffer)
        columns[#columns + 1] = {
            keyspace = ksname,
            table = tablename,
            name = column_name,
            type = type
        }
    end
    return {columns_count=columns_count, columns=columns}
end

local function parse_rows(buffer, metadata)
    local columns = metadata.columns
    local columns_count = metadata.columns_count
    local rows_count = read_int(buffer)
    local values = {}
    for i = 1, rows_count do
        local row = {}
        for j = 1, columns_count do
            local value = read_value(buffer, columns[j].type)
            row[j] = value
            row[columns[j].name] = value
        end
        values[#values + 1] = row
    end
    assert(buffer.pos == #(buffer.str) + 1)
    return values
end

function _M.prepare(self, query)
    local body = long_string_representation(query)
    local response = send_reply_and_get_response(self, op_codes.PREPARE, body)
    if response.op_code ~= op_codes.RESULT then
        error("Result expected")
    end
    buffer = response.buffer
    local kind = read_int(buffer)
    if kind == result_kinds.PREPARED then
        local id = read_short_bytes(buffer)
        local metadata = parse_metadata(buffer)
        local result_metadata = parse_metadata(buffer)
        assert(buffer.pos == #(buffer.str) + 1)
        return {id=id, metadata=metadata, result_metadata=result_metadata}
    else
        error("Invalid result kind")
    end
end

function _M.execute(self, query, args)
    local op_code, query_repr
    if type(query) == "string" then
        op_code = op_codes.QUERY
        query_repr = long_string_representation(query)
    else
        op_code = op_codes.EXECUTE
        query_repr = short_bytes_representation(query.id)
    end

    local values = {}
    if not args then
        flags = string.char(0)
    else
        flags = string.char(1)
        values[#values + 1] = short_representation(#args)
        for _, value in ipairs(args) do
            values[#values + 1] = value_representation(value)
        end
    end

    local query_parameters = consistency.ONE .. flags
    body = query_repr .. query_parameters .. table.concat(values)
    local response = send_reply_and_get_response(self, op_code, body)

    if response.op_code ~= op_codes.RESULT then
        error("Result expected")
    end
    buffer = response.buffer
    local kind = read_int(buffer)
    if kind == result_kinds.VOID then
        return true
    elseif kind == result_kinds.ROWS then
        local metadata = parse_metadata(buffer)
        return parse_rows(buffer, metadata)
    elseif kind == result_kinds.SET_KEYSPACE then
        local keyspace = read_string(buffer)
        return keyspace
    elseif kind == result_kinds.SCHEMA_CHANGE then
        local change = read_string(buffer)
        local keyspace = read_string(buffer)
        local table = read_string(buffer)
        return keyspace .. "." .. table .. " " .. change
    else
        error("Invalid result kind")
    end
end

function _M.set_keyspace(self, keyspace_name)
    return self:execute("USE " .. keyspace_name)
end

return _M
