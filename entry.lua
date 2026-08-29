local inspect = require "inspect"
local service = require "service"

local root_addr = service.new { 
    source = "@services/root.lua",
    config = { } 
}

service.start(root_addr)
service.send(service.get_id(root_addr), "boot")
service.join(root_addr)