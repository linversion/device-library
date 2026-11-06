# device-library

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

**多个 AI agent 在同一台机器上调试安卓时，会同时抢同一台设备、互相踩掉对方的会话。** device-library 是一个本地协调器，专门解决这个冲突：agent 借用设备、获得独占租约、直接操作、用完归还。

一个无外部依赖的 Python 3.10+ 安卓设备借还协调器。借用方用租约授予的显式 selector（设备序列号/AVD 名）**直接用自己的 sim-use（默认）或 adb** 操作设备；协调器只负责借出、容量核算、归还交接和受限恢复，永不代理你的操作。

## 工作原理

1. **借用** —— `device-library borrow` 排队或激活一份 30 分钟租约，返回绑定你属主进程的独占操作 selector。
2. **直接操作** —— 拿着 selector 用自己的 sim-use 或 adb 驱动设备，协调器不经过手。
3. **归还** —— `device-library return LEASE_ID --stopped` 声明你的所有操作已停止，完成交接，设备释放给下一个借用方。
4. **故障安全** —— 每台主机一个协调器，SQLite 账本为唯一事实来源，epoch/lease fence 使过期租约失效；出问题时设备进入隔离等待运维处理，绝不盲目再借出。

## 30 秒上手

```sh
# 1. 安装
python3 -m pip install .

# 2. 启动协调器（首次运行创建账本；设备注册见 docs/operator.md）
device-library serve --init

# 3. 借用（另开一个终端；--owner-pid 必填，由你的运行环境提供）
device-library --credential-file ./my-loan.credential borrow \
  --kind emulator --requires api=35 --borrower alice --session run-42 \
  --task checkout-test --request-key checkout-device --owner-pid "$DEVICE_LIBRARY_OWNER_PID" --wait 60

# …… 用授予的 selector 直接操作设备 ……

# 4. 归还
device-library --credential-file ./my-loan.credential return LEASE_ID --reason done --stopped --wait 60
```

所有返回都是 JSON，含 `ok` 以及 `result` 或 `reason` 和退出码。完整命令列表见 `device-library -h`。

## 诚实的边界

- **合作式协调**：凭据、socket、数据库都是单 OS 用户的 0600 文件；这是协作客户端之间的仲裁，不是对恶意客户端的隔离。
- **单机、单用户**：0.1 版面向一台机器；多主机与多用户强制隔离不在范围内。
- **不是设备农场**：无远程访问、无 Web 面板、无横向扩展——只协调插在这台机器上的设备。
- **不是 UI 操作代理**：没有命令网关、测试执行器、点击/输入/截图封装或借用方进程管理。设备由你自己驱动，协调器只管谁在什么时间可以用哪台设备。

## 文档地图

- **借设备** —— [docs/borrowing.md](docs/borrowing.md)：三步借还流程、凭据文件规则、租约规则、属主绑定、恢复与退出码。
- **运维服务** —— [docs/operator.md](docs/operator.md)：运行 `serve`、注册/停用设备、隔离恢复、备份/还原与留存策略。
- **开发维护** —— [docs/dev.md](docs/dev.md)：测试套件、假运行时、账本/协调器协议、适配层内部与验收状态。

注：详细文档（docs/）目前仅有英文版本。

## 许可证

[MIT](LICENSE) —— Copyright (c) 2026 linversion
