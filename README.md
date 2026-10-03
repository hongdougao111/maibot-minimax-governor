# MiniMax 开支管家 (MiniMax Governor)

一个 [MaiBot](https://github.com/Mai-with-u/MaiBot) 插件：监测 MiniMax 套餐余量与机器人回复消耗，超预算自动降低发言频率，恢复后自动放开——让 AI 群友"会花钱、管住手"。

## 功能

- **余量监控**：定时查询 MiniMax 账户余额 / 套餐剩余百分比（兼容按量计费与 Token Plan 订阅两种返回格式）
- **实时调速**：按小时统计每个群的回复数，超过预算自动调低发言频率（`ctx.frequency.set_adjust`），回落预算后自动恢复，全程无需重启
- **分群独立预算**：每个群可以单独设置每小时回复上限
- **套餐余量保护**：余量百分比低于阈值时强制限流，并每日推送一次提醒
- **消耗预估**：根据历史查询自动估算每小时消耗速度与"还能用几天"
- **管理员专属命令**：省流开关与策略切换仅限配置的管理员 QQ 使用

## 命令

| 命令 | 说明 | 权限 |
| --- | --- | --- |
| `/余额` | 查询 MiniMax 余量、消耗速度、本小时回复数 | 所有人 |
| `/省流关` | 解除限流 12 小时（到时自动恢复），适合群聚爆聊 | 仅管理员 |
| `/省流开` | 立即恢复自动管控 | 仅管理员 |
| `/策略保守` | 预算 ×0.7，限流更深，月底吃紧时用 | 仅管理员 |
| `/策略均衡` | 恢复默认预算 | 仅管理员 |
| `/策略激进` | 预算 ×1.5，限流更浅，套餐充足时用 | 仅管理员 |

## 安装

1. 将本仓库 clone（或下载 Release）到 MaiBot 的 `plugins/` 目录：

```bash
cd /path/to/MaiBot/plugins
git clone https://github.com/hongdougao111/maibot-minimax-governor.git minimax-governor
```

2. 编辑 `minimax-governor/config.toml`，填写 `api_key` 与 `group_id`（Token Plan 用户填 `sk-sp-` 开头的套餐专属 Key）；
3. 重启 MaiBot，在 WebUI「插件管理」中启用本插件。

## 配置说明

见 [`config.toml`](./config.toml)，每个字段都有注释。核心字段：

| 字段 | 说明 |
| --- | --- |
| `api_key` / `group_id` | MiniMax 开放平台的密钥与 GroupId |
| `admin_qq` | 管理员 QQ，命令鉴权使用 |
| `default_reply_budget_per_hour` | 默认每小时回复软上限 |
| `group_budgets` | 分群独立预算（群号 = 每小时回复上限） |
| `low_percent` | 套餐余量百分比告急阈值 |

## 注意事项

- MiniMax 余额接口在 2026 年中改版过（返回字段由金额变为剩余百分比），本插件兼容两种返回格式；若你的账户返回结构特殊，请发 `/余额` 查看原始返回并在 Issue 中反馈。
- `set_adjust` 的取值语义官方文档未详述，本插件按"负数 = 更沉默"实现；如你的版本行为相反，请提 Issue。
- 请勿将填有真实密钥的 `config.toml` 提交到公开仓库。

## 许可证

[MIT](./LICENSE)
