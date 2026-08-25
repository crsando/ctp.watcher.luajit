local inspect = require "inspect"
local service = require "lservice3".input(...)
local config = service.config
local ctp = require "lctp2"
local cjson = require "cjson.safe"

local S = {}

-- cache

local book = { }

local function load_accounts()
    -- 获取家目录路径（兼容 Windows 和 Linux/macOS）
    local home = os.getenv("HOME") or os.getenv("USERPROFILE")
    if not home then return nil end

    local filepath = home .. "/.tifa/accounts.lua"

    -- 加载文件（此时只编译不执行）
    local chunk, err = loadfile(filepath)
    if not chunk then
        error("failed loading: " .. tostring(err))
    end

    -- 执行文件，获取返回值
    -- 假设 accounts.lua 结尾有 return 了一个 table
    local accounts = chunk() 
    return accounts
end

function S.boot()
    local accounts = load_accounts()

    service.spawn { name = "db_writer", source = "@services/db_writer.lua", config = {} }

    service.spawn { name = "collector", source = "@services/ctp_collector.lua", config = { 
            server = accounts["collector"]["gtja-3"],
            symbol = symbol
        } }

    service.call("collector", "start")

    service.spawn { name = "trader", source = "@services/ctp_trader.lua", config = {
            server = accounts["trader"]["gtja-3"],
        } }
    local rsp = service.call("trader", "start") -- blocking, until trader starts

    local instruments = service.call("trader", "query_instrument")

    do 
        local subs = {}
        local count = 0
        for _, entry in ipairs(instruments) do 
            count = count + 1
            table.insert(subs, entry.InstrumentID)

            -- subscribe for every 10 instruments
            if count == 10 then 
                service.call("collector", "subscribe", subs)
                subs = {}
                count = 0
            end
        end
        if #subs > 0 then 
            service.call("collector", "subscribe", subs)
        end
    end -- end subscribe
end


function S.quit()
    ctp.log_debug("root is quiting")
    service.send("db_writer", "quit")
    service.send("collector", "quit")
    service.send("trader", "quit")
    service.quit()
end

return service.dispatch(S)
