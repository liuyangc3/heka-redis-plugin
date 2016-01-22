--[[
hekad.toml

[HttpOutput]
message_matcher = "TRUE"
type = "SandboxOutput"
filename = "nxin/log4j_output.lua"
    [Log4jOutput.config]
    url = "http://mc.nxin.com/message/sendsmsCommonNxin?message="
    method = "POST"
    req_body = "foo=bar"
    outfile = "log.txt"
]]

require"io"
require"ltn12"
require"string"
local http = require("socket.http")


local url      = read_config("url") or error("url must be set")
local method   = read_config("method") or error("method must be set")
local req_body = read_config("req_body")
local outfile  = read_config("outfile")

function process_message()
    local log = read_message("Payload")
    --local ok, json = pcall(cjson.decode, log)
    --if not ok then
    --    return -1
    --end
    
    -- do request
    local respbody = {}
    local result, respcode, respheaders, respstatus = http.request {
        url = url,
        method = method,
        source = ltn12.source.string(reqbody), 
        headers = {
            ["content-type"] = "text/plain",
            ["content-length"] = tostring(#reqbody)
        },
        sink = ltn12.sink.table(respbody)
    }
    respbody = table.concat(respbody)
    
    -- wirte respbody to file
    local f, e = io.open("/opt/res", "a+")
    f:write(respbody)
    return 0
end
