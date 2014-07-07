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

local mt = { __index = _M }

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
    ngx.log(ngx.ERR, "received frame: '" .. debug_hex_string(header) .. "'")
    return header
end

function _M.startup(self)
    local version = version_codes.REQUEST
    local flags = '\000'
    local stream_id = '\000'
    local op_code = op_codes.STARTUP
    local body = string_map_representation({["CQL_VERSION"]=CQL_VERSION})
    local length = int_representation(#body)
    local frame = version .. flags .. stream_id .. op_code .. length .. body

    ngx.log(ngx.ERR, "frame: '" .. debug_hex_string(frame) .. "'")
    local bytes, err = self.sock:send(frame)
    if not bytes then
        error("Failed to send data to cassandra: " .. err)
    end

    read_frame(self)
    return true
end


return _M