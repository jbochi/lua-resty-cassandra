-- Implementation of CQL Binary protocol V2 available at https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec;hb=HEAD

local tcp = ngx.socket.tcp


local _M = {}

_M.version = "0.0.1"
local CQL_VERSION = "3.0.0"

local version_codes = {
    REQUEST=0x02,
    RESPONSE=0x82
}

local op_codes = {
    ERROR=0x00,
    STARTUP=0x01,
    READY=0x02,
    AUTHENTICATE=0x03,
    -- 0x04
    OPTIONS=0x05,
    SUPPORTED=0x06,
    QUERY=0x07,
    RESULT=0x08,
    PREPARE=0x09,
    EXECUTE=0x0A,
    REGISTER=0x0B,
    EVENT=0x0C,
    BATCH=0x0D,
    AUTH_CHALLENGE=0x0E,
    AUTH_RESPONSE=0x0F,
    AUTH_SUCCESS=0x10,
}

local consistency = {
    ANY=0x0000,
    ONE=0x0001,
    TWO=0x0002,
    THREE=0x0003,
    QUORUM=0x0004,
    ALL=0x0005,
    LOCAL_QUORUM=0x0006,
    EACH_QUORUM=0x0007,
    SERIAL=0x0008,
    LOCAL_SERIAL=0x0009,
    LOCAL_ONE=0x000A
}
_M.consistency = consistency

local result_kinds = {
    VOID=0x01,
    ROWS=0x02,
    SET_KEYSPACE=0x03,
    PREPARED=0x04,
    SCHEMA_CHANGE=0x05
}

local types = {
    custom=0x00,
    ascii=0x01,
    bigint=0x02,
    blob=0x03,
    boolean=0x04,
    counter=0x05,
    decimal=0x06,
    double=0x07,
    float=0x08,
    int=0x09,
    text=0x0A,
    timestamp=0x0B,
    uuid=0x0C,
    varchar=0x0D,
    varint=0x0E,
    timeuuid=0x0F,
    inet=0x10,
    list=0x20,
    map=0x21,
    set=0x22
}

local error_codes = {
    [0x0000]= "Server error",
    [0x000A]= "Protocol error",
    [0x0100]= "Bad credentials",
    [0x1000]= "Unavailable exception",
    [0x1001]= "Overloaded",
    [0x1002]= "Is_bootstrapping",
    [0x1003]= "Truncate_error",
    [0x1100]= "Write_timeout",
    [0x1200]= "Read_timeout",
    [0x2000]= "Syntax_error",
    [0x2100]= "Unauthorized",
    [0x2200]= "Invalid",
    [0x2300]= "Config_error",
    [0x2400]= "Already_exists",
    [0x2500]= "Unprepared"
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
    if num < 0 then
        -- 2's complement
        num = math.pow(256, bytes) + num
    end
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

local function boolean_representation(value)
    if value then return "\001" else return "\000" end
end

local function float_representation(number)
    if number == 0 then
        return string.char(0x00, 0x00, 0x00, 0x00)
    elseif number ~= number then
        return string.char(0xFF, 0xFF, 0xFF, 0xFF)
    else
        local sign = 0x00
        if number < 0 then
            sign = 0x80
            number = -number
        end
        local mantissa, exponent = math.frexp(number)
        exponent = exponent + 0x7F
        if exponent <= 0 then
            mantissa = math.ldexp(mantissa, exponent - 1)
            exponent = 0
        elseif exponent > 0 then
            if exponent >= 0xFF then
                return string.char(sign + 0x7F, 0x80, 0x00, 0x00)
            elseif exponent == 1 then
                exponent = 0
            else
                mantissa = mantissa * 2 - 1
                exponent = exponent - 1
            end
        end
        mantissa = math.floor(math.ldexp(mantissa, 23) + 0.5)
        return string.char(
                sign + math.floor(exponent / 2),
                (exponent % 2) * 0x80 + math.floor(mantissa / 0x10000),
                math.floor(mantissa / 0x100) % 0x100,
                mantissa % 0x100)
    end
end

local function inet_representation(value)
    local digits = {}
    -- ipv6
    for d in string.gfind(value, "([^:]+)") do
        if #d == 4 then
            for i = 1, #d, 2 do
                digits[#digits + 1] = string.char(tonumber(string.sub(d, i, i + 1), 16))
            end
        end
    end
    -- ipv4
    if #digits == 0 then
        for d in string.gfind(value, "(%d+)") do
            table.insert(digits, string.char(d))
        end
    end
    return table.concat(digits)
end

local function list_representation(elements)
    local buffer = {short_representation(#elements)}
    for _, value in ipairs(elements) do
        buffer[#buffer + 1] = _M._value_representation(value, true)
    end
    return table.concat(buffer)
end

local function set_representation(elements)
    return list_representation(elements)
end

local function map_representation(map)
    local buffer = {}
    local size = 0
    for key, value in pairs(map) do
        buffer[#buffer + 1] = _M._value_representation(key, true)
        buffer[#buffer + 1] = _M._value_representation(value, true)
        size = size + 1
    end
    table.insert(buffer, 1, short_representation(size))
    return table.concat(buffer)
end

local function value_representation(value, short)
    local representation = value
    if type(value) == 'number' and math.floor(value) == value then
        representation = int_representation(value)
    elseif type(value) == 'number' then
        representation = float_representation(value)
    elseif type(value) == 'table' and value.type == 'float' then
        representation = float_representation(value.value)
    elseif type(value) == 'table' and value.type == 'bigint' then
        representation = big_endian_representation(value.value, 8)
    elseif type(value) == 'boolean' then
        representation = boolean_representation(value)
    elseif type(value) == 'table' and value.type == 'counter' then
        representation = big_endian_representation(value.value, 8)
    elseif type(value) == 'table' and value.type == 'timestamp' then
        representation = big_endian_representation(value.value, 8)
    elseif type(value) == 'table' and value.type == 'uuid' then
        representation = uuid_representation(value.value)
    elseif type(value) == 'table' and value.type == 'inet' then
        representation = inet_representation(value.value)
    elseif type(value) == 'table' and value.type == 'list' then
        representation = list_representation(value.value)
    elseif type(value) == 'table' and value.type == 'map' then
        representation = map_representation(value.value)
    elseif type(value) == 'table' and value.type == 'set' then
        representation = set_representation(value.value)
    else
        representation = value
    end
    if short then
        return short_bytes_representation(representation)
    end
    return bytes_representation(representation)
end
_M._value_representation = value_representation

---
--- DECODE FUNCTIONS
---

local function string_to_number(str, signed)
    local number = 0
    local exponent = 1
    for i = #str, 1, -1 do
        number = number + string.byte(str, i) * exponent
        exponent = exponent * 256
    end
    if signed and number > exponent / 2 then
        -- 2's complement
        number = number - exponent
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
    return string.byte(read_raw_bytes(buffer, 1))
end

local function read_int(buffer)
    return string_to_number(read_raw_bytes(buffer, 4), true)
end

local function read_short(buffer)
    return string_to_number(read_raw_bytes(buffer, 2), false)
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
        type_value = read_option(buffer)
    end
    return {id=type_id, value=type_value}
end

local function read_boolean(bytes)
    return string.byte(bytes) == 1
end

local function read_float(bytes)
    local b1, b2, b3, b4 = string.byte(bytes, 1, 4)
    local exponent = (b1 % 0x80) * 0x02 + math.floor(b2 / 0x80)
    local mantissa = math.ldexp(((b2 % 0x80) * 0x100 + b3) * 0x100 + b4, -23)
    if exponent == 0xFF then
        if mantissa > 0 then
            return 0 / 0
        else
            mantissa = math.huge
            exponent = 0x7F
        end
    elseif exponent > 0 then
        mantissa = mantissa + 1
    else
        exponent = exponent + 1
    end
    if b1 >= 0x80 then
        mantissa = -mantissa
    end
    return math.ldexp(mantissa, exponent - 0x7F)
end

local function read_uuid(bytes)
    local buffer = {}
    for i = 1, #bytes do
        buffer[i] = string.format("%02x", string.byte(bytes, i))
    end
    table.insert(buffer, 5, "-")
    table.insert(buffer, 8, "-")
    table.insert(buffer, 11, "-")
    table.insert(buffer, 14, "-")
    return table.concat(buffer)
end

local function read_inet(bytes)
    local buffer = {}
    if #bytes == 16 then
        -- ipv6
        for i = 1, #bytes, 2 do
            buffer[#buffer + 1] = string.format("%02x", string.byte(bytes, i)) ..
                                  string.format("%02x", string.byte(bytes, i + 1))
        end
        return table.concat(buffer, ":")
    end
    for i = 1, #bytes do
        buffer[#buffer + 1] = string.format("%d", string.byte(bytes, i))
    end
    return table.concat(buffer, ".")
end

local function read_list(buffer, type)
    local element_type = type.value
    local n = read_short(buffer)
    local elements = {}
    for i = 1, n do
        elements[#elements + 1] = _M._read_value(buffer, element_type, true)
    end
    return elements
end

local read_set = read_list

local function read_map(buffer, type)
    local element_type = type.value
    local n = read_short(buffer)
    local map = {}
    for i = 1, n do
        local key = _M._read_value(buffer, element_type, true)
        local value = _M._read_value(buffer, element_type, true)
        map[key] = value
    end
    return map
end

local function read_value(buffer, type, short)
    local bytes
    if short then
        bytes = read_short_bytes(buffer)
    else
        bytes = read_bytes(buffer)
    end
    if type.id == types.int or
       type.id == types.bigint or
       type.id == types.counter or
       type.id == types.varint or
       type.id == types.timestamp then
        return string_to_number(bytes, true)
    elseif type.id == types.boolean then
        return read_boolean(bytes)
    elseif type.id == types.float then
        return read_float(bytes)
    elseif type.id == types.uuid then
        return read_uuid(bytes)
    elseif type.id == types.inet then
        return read_inet(bytes)
    elseif type.id == types.list then
        return read_list(create_buffer(bytes), type)
    elseif type.id == types.set then
        return read_set(create_buffer(bytes), type)
    elseif type.id == types.map then
        return read_map(create_buffer(bytes), type)
    end
    return bytes
end
_M._read_value = read_value

local function read_error(buffer)
    local error_code = error_codes[read_int(buffer)]
    local error_message = read_string(buffer)
    return 'Cassandra returned error (' .. error_code .. '): "' .. error_message .. '"'
end

local function read_frame(self)
    local header, err, partial = self.sock:receive(8)
    if not header then
        return nil, "Failed to read frame header: " .. err
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
            return nil, "Failed to read frame body: " .. err
        end
    else
        body = ""
    end
    if version ~= version_codes.RESPONSE then
        error("Invalid response version")
    end
    local body_buffer = create_buffer(body)
    if op_code == op_codes.ERROR then
        return nil, read_error(body_buffer)
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
    local version = string.char(version_codes.REQUEST)
    local flags = '\000'
    local stream_id = '\000'
    local length = int_representation(#body)
    local frame = version .. flags .. stream_id .. string.char(op_code) .. length .. body

    local bytes, err = self.sock:send(frame)
    if not bytes then
        return nil, "Failed to send data to cassandra: " .. err
    end
    return read_frame(self)
end

function _M.startup(self)
    local body = string_map_representation({["CQL_VERSION"]=CQL_VERSION})
    local response, err = send_reply_and_get_response(self, op_codes.STARTUP, body)
    if not response then
        return nil, err
    end
    if response.op_code ~= op_codes.READY then
        return nil, "Server is not ready"
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
    local response, err = send_reply_and_get_response(self, op_codes.PREPARE, body)
    if not response then
        return nil, err
    end
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

function _M.execute(self, query, args, consistency_level)
    if not consistency_level then
        consistency_level = consistency.ONE
    end

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

    local query_parameters = short_representation(consistency_level) .. flags
    body = query_repr .. query_parameters .. table.concat(values)
    local response, err = send_reply_and_get_response(self, op_code, body)
    if not response then
        return nil, err
    end

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
