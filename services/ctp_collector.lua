local S = require "lctp2.templates.ctp_collector" 
local collector = assert(S._collector)

local service = require "service"
local ctp = require "lctp2"

local inspect = require "inspect"

local function on_tick(tick)
    ctp.log_debug("on_tick | %s | %f", tick.InstrumentID, tick.LastPrice)
    service.send("db_writer", "on_tick", tick)
end

function service.on_idle()
    local tick
    while true do 
        tick = collector:recv(false) -- non-blocking
        if tick == nil then break end
        on_tick(tick)
    end
end

return S