local inspect = require "inspect"
local ctp = require "lctp2"
ctp.log_set_level("LOG_DEBUG")

local service = require "lservice3" .input(...)
local config = service.config; do 
        -- config.server = { front_addr =  "tcp://180.169.75.18:61213", broker = "7090", user = "85506493" }
        assert(config.server, "no config.server")
    end

local S = {}
local collector = ctp.new_collector(config.server)

function S.start()
    ctp.log_debug("is_ready: %s", collector:is_ready() and "true" or "false")
    collector:async(service.get_async())
    collector:start()

    --
    -- wait for the collector to be ready
    --
    local ready = false
    while not ready do  
        ready = collector:is_ready()
        ctp.log_debug("is_ready: %s", ready and "true" or "false")
        service.sleep(50)
    end

    -- 连接成功后再subscribe
    if config.symbol then 
        collector:subscribe { config.symbol }
    end

    return 1
end

function S.quit()
    service.quit()
end

function S.subscribe(symbols)
    if (type(symbols) == "string") then 
        symbols = { symbols }
    end

    if type(symbols) == "table" then 
        print("collector subscribe", inspect(symbols))
        collector:subscribe(symbols)
        return #symbols
    else 
        return 0
    end
end

function S.unsubscribe(symbols)
    if (type(symbols) == "string") then 
        symbols = { symbols }
    end

    collector:unsubscribe(symbols)
    return 1
end

local function on_tick(tick)
    ctp.log_debug("on_tick | %s | %f", tick.InstrumentID, tick.LastPrice)
    service.send("db_writer", "on_tick", tick)
    -- print("on_tick", inspect(tick))
end

function service.on_idle()
    local tick
    while true do 
        tick = collector:recv(false) -- non-blocking
        if tick == nil then break end
        on_tick(tick)
    end
end

return service.dispatch(S)