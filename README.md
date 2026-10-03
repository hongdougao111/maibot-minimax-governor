# MiniMax 开支管家 (MiniMax Governor)

一个 [MaiBot](https://github.com/Mai-with-u/MaiBot) 插件：监测 MiniMax 套餐余量与机器人回复消耗，超预算自动降低发言频率，恢复后自动放开——让 AI 群友"会花钱、管住手"。

## 功能

- **余量监控**：定时查询 MiniMax 账户余额 / 套餐剩余百分比（兼容按量计费与 Token Plan 订阅两种返回格式）
- **实时调速**：按小时统计每个群的回复数，超过预算自动调低发言频率（`ctx.frequency.set_adjust`），回落预算后自动恢复，全程无需重启
- **分群独立预算**：每个群可以单独设置每小时回复上限
- **套餐余量保护**：余量百分比低于阈值时强制限流，并推送提醒
- **消耗预估**：根据历史查询自动估算每小时消耗速度与"还能用几天"
- **每日消费日报**：每天定时推送余量、消耗速度与回复统计
- **WebUI 配置界面**：所有配置可在 WebUI「插件管理 → 设置」中在线修改，保存即生效

## 无命令设计

本插件**不使用聊天命令**——所有配置在 WebUI 设置页完成，日报与告警自动推送，完全不需要在群里"测试机器人"。

## 安装

1. 将本仓库 clone（或下载 Release）到 MaiBot 的 `plugins/` 目录：

```bash
cd /path/to/MaiBot/plugins
git clone https://github.com/hongdougao111/maibot-minimax-governor.git minimax-governor
cd minimax-governor
```

2. 从模板生成配置并填写（仓库不直接提供 config.toml，避免 git pull 覆盖你已填好的密钥）：

```bash
cp config.example.toml config.toml
# 然后编辑 config.toml，填入 api_key 与 group_id
# （Token Plan 用户填 sk-cp- 开头的套餐订阅 Key）
```

也可以跳过第 2 步，启动后在 WebUI「插件管理 → 本插件 → 设置」中直接在线填写并保存。

3. 重启 MaiBot，在 WebUI「插件管理」中启用本插件。

## 安全说明

- 插件停用或卸载时，会自动归还所有已设置的发言频率调整值（`set_adjust` 归零），机器人不会带着限流状态"带病运行"。
- 告急提醒与每日日报推送到管理员私聊，不会把账户信息发进群聊。

## 配置说明

见 [`config.example.toml`](./config.example.toml)，每个字段都有注释。核心字段：

| 字段 | 说明 |
| --- | --- |
| `api_key` / `group_id` | MiniMax 开放平台的密钥与 GroupId |
| `default_reply_budget_per_hour` | 默认每小时回复软上限 |
| `group_budgets` | 分群独立预算（群号 = 每小时回复上限） |
| `low_percent` | 套餐余量百分比告急阈值 |
| `daily_report_hour` | 每日消费日报推送时刻（0-23） |

## 注意事项

- MiniMax 余额接口在 2026 年中改版过（返回字段由金额变为剩余百分比），本插件兼容两种返回格式；若你的账户返回结构特殊，请提 Issue 反馈。
- `set_adjust` 的取值语义官方文档未详述，本插件按"负数 = 更沉默"实现；如你的版本行为相反，请提 Issue。
- 请勿将填有真实密钥的 `config.toml` 提交到公开仓库。

## 许可证

[MIT](./LICENSE)
