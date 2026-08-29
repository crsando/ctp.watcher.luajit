-- questdb.luajit is kept as a source dependency in ~/src.
local qdb = require "questdb"
local service = require "service"
local config = service.config or {}

local TABLE_NAME = "ctp_tmp"
local QUESTDB_CONF = config.questdb_conf or "http::addr=192.168.5.13:9000;"
local FLUSH_INTERVAL_MS = 1000

local sender, sender_err = qdb.sender { conf = QUESTDB_CONF }
assert(sender, "failed to connect to QuestDB: " .. tostring(sender_err))

local function positive_integer(value, name, default)
    if value == nil then
        value = default
    end
    value = tonumber(value)
    if not value or value ~= value or value == math.huge or value == -math.huge or
        value < 1 or value % 1 ~= 0 then
        error(name .. " must be a positive integer", 2)
    end
    return value
end

local BATCH_SIZE = positive_integer(config.batch_size, "config.batch_size", 1000)
local buffer, buffer_err = sender:buffer()
assert(buffer, "failed to create QuestDB buffer: " .. tostring(buffer_err))
local pending_rows = 0

local S = {}

local function required_string(value, name)
    if type(value) ~= "string" or value == "" then
        error(name .. " must be a non-empty string", 3)
    end
    return value
end

local function required_number(value, name)
    value = tonumber(value)
    if not value or value ~= value or value == math.huge or value == -math.huge then
        error(name .. " must be a finite number", 3)
    end
    return value
end

-- CTP's ActionDay is the natural calendar date. TradingDay is intentionally
-- not used because it can differ from the date on which the quote arrived.
local function tick_timestamp(tick)
    local action_day = required_string(tick.ActionDay, "tick.ActionDay")
    local year, month, day = action_day:match("^(%d%d%d%d)(%d%d)(%d%d)$")
    if not year then
        year, month, day = action_day:match("^(%d%d%d%d)%-(%d%d)%-(%d%d)$")
    end
    if not year then
        error("tick.ActionDay must be YYYYMMDD or YYYY-MM-DD", 3)
    end

    local update_time = required_string(tick.UpdateTime, "tick.UpdateTime")
    local hour, minute, second = update_time:match("^(%d%d):(%d%d):(%d%d)$")
    if not hour then
        error("tick.UpdateTime must be HH:MM:SS", 3)
    end

    -- UpdateMillisec is the name in CThostFtdcDepthMarketDataField. Some
    -- adapters expose the same value as Milliseconds, so accept both.
    local milliseconds = tick.Milliseconds
    if milliseconds == nil then
        milliseconds = tick.UpdateMillisec
    end
    milliseconds = required_number(milliseconds, "tick.Milliseconds")
    if milliseconds % 1 ~= 0 or milliseconds < 0 or milliseconds > 999 then
        error("tick.Milliseconds must be an integer between 0 and 999", 3)
    end

    -- CTP timestamps are local China time. Parsing the explicit +08:00 offset
    -- keeps the stored epoch value independent of the process's local TZ.
    local text = string.format(
        "%s-%s-%sT%s:%s:%s.%03d+08:00",
        year, month, day, hour, minute, second, milliseconds)
    return qdb.time.parse_rfc3339(text)
end

local function check(ok, err)
    if not ok then
        error("QuestDB write failed: " .. tostring(err), 3)
    end
    return ok
end

local function flush_buffer()
    local ok, err = buffer:flush()
    if ok then
        pending_rows = 0
    end
    return ok, err
end

local function append_tick(tick)
    if type(tick) ~= "table" then
        error("tick must be a table", 2)
    end

    local symbol = required_string(tick.InstrumentID, "tick.InstrumentID")
    local price = required_number(tick.LastPrice, "tick.LastPrice")
    local volume = tick.Volume
    if volume == nil then
        error("tick.Volume is required", 2)
    end

    local timestamp = tick_timestamp(tick)
    local row, err = buffer:row(TABLE_NAME)
    if not row then
        error("QuestDB row creation failed: " .. tostring(err), 2)
    end

    check(row:symbol("symbol", symbol))
    check(row:double("price", price))
    check(row:int64("volume", volume))
    check(row:at(timestamp))

    pending_rows = pending_rows + 1
    if pending_rows >= BATCH_SIZE then
        check(flush_buffer())
    end
    return true
end

local function on_flush_timer()
    local ok, err = flush_buffer()
    if not ok then
        io.stderr:write("QuestDB timed flush failed: " .. tostring(err) .. "\n")
    end
end

local flush_timer, timer_err = service.uv.new_timer()
assert(flush_timer, "failed to create QuestDB flush timer: " .. tostring(timer_err))
local timer_ok, timer_start_err = flush_timer:start(
    FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, on_flush_timer)
assert(timer_ok, "failed to start QuestDB flush timer: " .. tostring(timer_start_err))

-- The collector currently sends the on_tick message. Keep that entry point
-- and expose S.tick as well for callers using the shorter function name.
function S.tick(tick)
    return append_tick(tick)
end

S.on_tick = S.tick

function S.quit()
    if flush_timer then
        flush_timer:stop()
        flush_timer:close()
        flush_timer = nil
    end

    local ok, err = flush_buffer()
    sender:close()
    service.quit()
    if not ok then
        error("QuestDB write failed during shutdown: " .. tostring(err), 2)
    end
    return true
end

return service.dispatch(S)
