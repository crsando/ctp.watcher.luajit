# ctp.watcher.luajit

一个基于 LuaJIT、CTP 和 QuestDB 的期货行情采集程序。项目使用
`lservice3` 将行情采集、交易查询和数据库写入拆分成独立 service：启动后连接
CTP，查询可用期货合约，订阅行情，并将每个 tick 写入 QuestDB。

当前仓库是一个面向本机环境的运行脚本集合，没有独立的包管理清单或自动化测试。
运行前需要先安装并配置项目依赖，以及 CTP 柜台和 QuestDB 网络连接。

## 数据流

```text
entry.lua
    |
    v
services/root.lua
    |-- services/ctp_trader.lua   连接交易前置，查询合约
    |-- services/ctp_collector.lua 连接行情前置，接收 tick
    `-- services/db_writer.lua    将 tick 写入 QuestDB
```

根 service 的启动流程如下：

1. 读取 `~/.tifa/accounts.lua`。
2. 启动 `db_writer`、`collector` 和 `trader`。
3. 等待交易 service 建立连接并查询期货合约。
4. 每 10 个合约一批调用行情 service 的 `subscribe`。
5. `collector` 收到 tick 后发送给 `db_writer`。

## 目录结构

| 路径 | 作用 |
| --- | --- |
| [`entry.lua`](entry.lua) | 创建并启动根 service |
| [`services/root.lua`](services/root.lua) | 编排三个子 service 的生命周期和订阅流程 |
| [`services/ctp_collector.lua`](services/ctp_collector.lua) | 连接 CTP 行情前置、订阅/取消订阅、转发 tick |
| [`services/ctp_trader.lua`](services/ctp_trader.lua) | 交易前置、查询请求队列和订单状态管理 |
| [`services/db_writer.lua`](services/db_writer.lua) | 校验 tick 并写入 QuestDB 表 `ctp_tmp` |
| `*.con` | CTP 运行时配套的状态/配置文件，需与所用 CTP SDK 保持一致 |

## 运行环境

至少需要以下组件：

- LuaJIT 2.1（Lua 5.1 API）；
- [`lservice3`](../lservice3) 及其 `lservice3_c` 原生模块；
- [`lctp2`](../ctpc2)，包括 `libctpc2`、CTP API 动态库和 `/usr/local/include/ctpc2` 头文件；
- [`questdb.luajit`](../questdb.luajit) 及其底层 `libquestdb_client`；
- Lua 模块 `inspect`、`iconv`、`cjson.safe` 和 `luv`；
- 可访问的 CTP 行情/交易前置；
- 可写入的 QuestDB 实例。

`lctp2` 当前默认从以下位置读取 CTP 头文件，并通过系统动态链接器加载
`libctpc2`：

```text
/usr/local/include/ctpc2
/usr/local/lib/libctpc2.so
```

如果依赖安装在其他位置，需要调整依赖自身的安装配置或动态链接器路径。

### 构建依赖

下面是一个与本机源码目录布局匹配的示例。每个依赖也可以按其自己的 README
选择其他安装方式：

```bash
cd /home/twando/src/lservice3
make
sudo make install

cd /home/twando/src/ctpc2
make
sudo make install

cd /home/twando/src/questdb.luajit
make install
```

构建 `questdb.luajit` 前，还需要准备与其 FFI ABI 匹配的
`libquestdb_client.so`。详情见 [`questdb.luajit` 的 README](../questdb.luajit/README.md)。

## CTP 账户配置

`services/root.lua` 固定读取：

```text
$HOME/.tifa/accounts.lua
```

Windows 环境会尝试使用 `USERPROFILE`。该文件必须返回一个 Lua table，并包含
`collector.gtja-3` 和 `trader.gtja-3` 两个配置。示例：

```lua
-- ~/.tifa/accounts.lua
return {
    collector = {
        ["gtja-3"] = {
            front_addr = "tcp://行情前置地址:端口",
            broker = "期货公司编号",
            user = "期货账户",
        },
    },
    trader = {
        ["gtja-3"] = {
            front_addr = "tcp://交易前置地址:端口",
            broker = "期货公司编号",
            user = "期货账户",
            pass = "账户密码",
            app_id = "应用标识",
            auth_code = "授权码",
        },
    },
}
```

`front_addr`、`broker`、`user`、`pass`、`app_id` 和 `auth_code` 的具体值由
CTP 柜台提供。不要将真实账户文件提交到 Git，也不要把密码和授权码写进本仓库。

## QuestDB

`db_writer` 使用 `questdb.luajit` 的行式写入接口，固定写入表名 `ctp_tmp`，
每行包含以下字段：

| 列名 | 类型 | 来源 |
| --- | --- | --- |
| `symbol` | SYMBOL | `tick.InstrumentID` |
| `price` | DOUBLE | `tick.LastPrice` |
| `volume` | LONG | `tick.Volume` |
| 时间戳 | TIMESTAMP | `ActionDay` + `UpdateTime` + 毫秒 |

时间戳按 CTP 的中国标准时间（`+08:00`）解析，日期优先使用
`ActionDay`，而不是 `TradingDay`。`ActionDay` 接受 `YYYYMMDD` 或
`YYYY-MM-DD`，`UpdateTime` 必须是 `HH:MM:SS`，毫秒字段支持
`Milliseconds` 和 `UpdateMillisec`。

默认连接字符串为：

```text
http::addr=192.168.5.10:9000;
```

可以在 `db_writer` 的 service 配置中传入 `questdb_conf` 覆盖它。例如：

```lua
service.spawn {
    name = "db_writer",
    source = "@services/db_writer.lua",
    config = { questdb_conf = "http::addr=127.0.0.1:9000;" },
}
```

当前 `root.lua` 启动 `db_writer` 时传入的是空配置，因此要修改默认地址，需先
调整根 service 的 spawn 配置，或让默认地址对应可访问的 QuestDB 实例。

## 启动

确认账户、CTP 动态库和 QuestDB 均已就绪后，在项目根目录运行：

```bash
cd /home/twando/src/ctp.watcher.luajit
luajit entry.lua
```

若 Lua 模块没有安装到系统路径，可显式设置 `LUA_PATH`。下面的示例适用于依赖
源码位于同一父目录的布局；`lservice3_c.so` 和 `libctpc2.so` 仍需位于
`LUA_CPATH` 或系统动态链接器可搜索的位置：

```bash
export LUA_PATH='/home/twando/src/lservice3/lua/?.lua;/home/twando/src/ctpc2/?.lua;/home/twando/src/ctpc2/?/init.lua;/home/twando/src/questdb.luajit/lua/?.lua;/home/twando/src/questdb.luajit/lua/?/init.lua;;'
export LUA_CPATH='/home/twando/src/lservice3/?.so;;'
luajit entry.lua
```

进程会一直运行以接收行情。退出时应通过 service 的退出流程停止；不要在有未完成
订单时直接强制终止进程。

## Service 接口

### `collector`

- `start()`：连接行情前置并等待 ready；
- `subscribe(symbol)` 或 `subscribe({ symbol1, symbol2, ... })`：订阅行情；
- `unsubscribe(symbol)` 或 `unsubscribe({ ... })`：取消订阅；
- `quit()`：停止 service。

收到行情后，collector 会调用 `service.send("db_writer", "on_tick", tick)`。

### `trader`

- `start()`：启动交易前置并等待结算完成；
- `query_account()`：查询账户资金；
- `query_position()`：查询持仓；
- `query_instrument([symbol])`：查询全部期货合约或单个合约；
- `query_instrument_margin_rate(symbol)`：查询合约保证金率；
- `query_order()`：查询订单；
- `trade(symbol, price, volume, flag)`：提交订单并等待订单完成；
- `trade({ symbol, price, volume, flag, timeout })`：提交订单，可设置超时自动撤单；
- `nuke()`：查询当前持仓并逐项平仓；
- `quit()`：停止 service。

交易接口是真实下单接口。`price` 为 `0` 时按当前代码约定表示市价单，`flag` 默认为
开仓。调用 `trade` 或 `nuke` 前请先确认账户、合约、方向和数量，建议先在仿真环境验证。

## 注意事项

- 账户路径、账户组名和默认 QuestDB 地址目前都写在代码约定中，尚未提供命令行参数或环境变量配置。
- 根 service 会根据交易查询结果订阅全部期货合约，可能产生较大的行情量和数据库写入压力。
- `root.lua` 中保留了可选的 `symbol` 配置字段，但默认入口没有为它提供值；如需只订阅指定合约，应在启动编排逻辑中显式传入并修改订阅流程。
- `db_writer` 会严格校验 `InstrumentID`、`LastPrice`、`Volume` 和时间字段，字段缺失或格式错误会抛出异常。
- 仓库当前没有自动化测试。修改行情字段映射、时间戳处理或订单状态机后，应至少在仿真柜台和测试 QuestDB 上做端到端验证。
