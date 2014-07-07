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

local function string_representation(str)
    return short_representation(#str) .. str
end

local function long_string_representation(str)
    return int_representation(#str) .. str
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

---
--- DECODE FUNCTIONS
---

local function string_to_number(str)
    local number = 0
    local exponent = 1
    for i = #str, 1, -1 do
        number = number + string.byte(str, i) * exponent
        exponent = exponent * 16
    end
    return number
end

local function create_buffer(str)
    return {str=str, pos=1}
end

local function read_bytes(buffer, n_bytes)
    local bytes = string.sub(buffer.str, buffer.pos, buffer.pos + n_bytes - 1)
    buffer.pos = buffer.pos + n_bytes
    return bytes
end

local function read_byte(buffer)
    return read_bytes(buffer, 1)
end

local function read_int(buffer)
    return string_to_number(read_bytes(buffer, 4))
end

local function read_short(buffer)
    return string_to_number(read_bytes(buffer, 2))
end

local function read_string(buffer)
    local str_size = read_short(buffer)
    return read_bytes(buffer, str_size)
end

local function debug_hex_string(str)
    buffer = {}
    for i = 1, #str do
        buffer[i] = string.byte(str, i)
    end
    return table.concat(buffer, " ")
end

local function read_frame(self)
    local header, err, partial = self.sock:receive(8)
    if not header then
        error("Failed to read frame:" .. err)
    end
    local header_buffer = create_buffer(header)
    local version = read_byte(header_buffer)
    local flags = read_byte(header_buffer)
    local stream = read_byte(header_buffer)
    local op_code = read_byte(header_buffer)
    local length = read_int(header_buffer)
    local body, err, partial = self.sock:receive(length)
    if not body then
        error("Failed to read body:" .. err)
    end
    if version ~= version_codes.RESPONSE then
        error("Invalid response version")
    end
    local body_buffer = create_buffer(body)
    if op_code == op_codes.ERROR then
        local error_code = read_int(body_buffer)
        local hex_error_code = string.format("%x", error_code)
        local error_message = read_string(body_buffer)
        error("Cassandra returned error (" .. hex_error_code .. "): '" .. error_message .. "'")
    end
    return {
        flags=flags,
        stream=stream,
        op_code=op_code,
        buffer=body_buffer
    }
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

function _M.execute(self, query)
    local query_repr = long_string_representation(query)
    local flags = '\000'
    local query_params = consistency.ONE .. flags
    local body = query_repr .. query_params
    local response = send_reply_and_get_response(self, op_codes.QUERY, body)
    if response.op_code ~= op_codes.RESULT then
        error("Result expected")
    end
    assert(read_int(response.buffer) == result_kinds.ROWS)
    ngx.log(ngx.ERR, "body: '" .. debug_hex_string(response.buffer.str) .. "'")
end

return _M
