import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict

import aiohttp

from maibot_sdk import EventHandler, Field, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import EventType

ENDPOINTS = [
    "https://api.minimaxi.com/v1/get_balance?GroupId={gid}",
    "https://api.minimax.chat/v1/get_balance?GroupId={gid}",
]
NUM_KEYS = ("balance", "percent", "remaining", "total", "used", "usage")
WINDOW = 3600

# 三档策略：budget_mult 调预算，throttle 限流深度，recover 触发恢复的预算占比
STRATEGIES = {
    "conservative": {"budget_mult": 0.7, "throttle": -0.7, "recover": 0.5},
    "balanced":     {"budget_mult": 1.0, "throttle": -0.6, "recover": 0.7},
    "aggressive":   {"budget_mult": 1.5, "throttle": -0.35, "recover": 0.9},
}


def _find_numbers(obj, prefix=""):
    found = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            name = f"{prefix}.{k}" if prefix else k
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                if any(n in k.lower() for n in NUM_KEYS) or v != 0:
                    found[name] = v
            elif isinstance(v, dict):
                found.update(_find_numbers(v, name))
    return found


# ---------- WebUI 配置模型（本节所有文字都会显示在插件设置页） ----------
class GovernorSection(PluginConfigBase):
    __ui_label__ = "开支管家设置"

    api_key: str = Field(
        default="",
        description="MiniMax API 密钥。Token Plan / M Plan 用户填 sk-cp- 开头的套餐订阅 Key",
        json_schema_extra={
            "label": "API 密钥",
            "placeholder": "sk-cp-xxxxxxxx",
            "hint": "在 MiniMax 开放平台「套餐详情」页面复制",
        },
    )
    group_id: str = Field(
        default="",
        description="MiniMax 账户的 GroupId。找不到可填 0，仅影响余量显示",
        json_schema_extra={
            "label": "GroupId",
            "placeholder": "1234567890",
            "hint": "在 MiniMax 开放平台「基本信息」页面查看",
        },
    )
    check_interval_minutes: int = Field(
        default=30,
        ge=5,
        description="每多少分钟查询一次套餐余量（最小 5）",
        json_schema_extra={"label": "余量查询间隔（分钟）"},
    )
    default_reply_budget_per_hour: int = Field(
        default=40,
        ge=1,
        description="默认每小时回复软上限。未单独设预算的群使用此值。参考：每次回复约消耗 1~3 万 token",
        json_schema_extra={"label": "默认每小时回复预算"},
    )
    group_budgets: Dict[str, int] = Field(
        default_factory=dict,
        description='分群独立预算。TOML 写法：group_budgets = { "群号" = 每小时回复上限 }',
        json_schema_extra={"label": "分群独立预算（高级）"},
    )
    hard_multiplier: float = Field(
        default=1.5,
        description="硬上限 = 软上限 × 此倍数。达到硬上限会最深度限流",
        json_schema_extra={"label": "硬上限倍数"},
    )
    strategy: str = Field(
        default="balanced",
        description="限流策略：conservative（保守，预算×0.7，省着用）/ balanced（均衡，预算×1.0）/ aggressive（激进，预算×1.5，放开聊）",
        json_schema_extra={"label": "限流策略", "hint": "conservative / balanced / aggressive 三选一"},
    )
    low_percent: int = Field(
        default=20,
        description="套餐余量百分比低于此值时强制限流并推送提醒",
        json_schema_extra={"label": "余量告急阈值（%）"},
    )
    notify_stream_id: str = Field(
        default="",
        description="余量告急提醒与每日消费日报的推送目标。格式：qq:QQ号:private（发私聊）或 qq:群号:group（发群）。留空则只记录日志",
        json_schema_extra={
            "label": "提醒推送目标",
            "placeholder": "qq:10000:private",
        },
    )
    daily_report_hour: int = Field(
        default=21,
        ge=0,
        le=23,
        description="每天几点推送消费日报（0-23 点）",
        json_schema_extra={"label": "每日日报推送时刻（点）"},
    )


class PluginMetaSection(PluginConfigBase):
    __ui_label__ = "插件基础"

    config_version: str = Field(default="1.0.0", description="配置版本号（请勿修改）")
    enabled: bool = Field(default=True, description="是否启用插件")


class GovernorConfig(PluginConfigBase):
    plugin: PluginMetaSection = Field(default_factory=PluginMetaSection)
    minimax_governor: GovernorSection = Field(default_factory=GovernorSection)


class MiniMaxGovernor(MaiBotPlugin):
    config_model = GovernorConfig

    def __init__(self):
        super().__init__()
        self.replies = deque()              # (ts, chat_id)
        self.chat_ids = set()
        self.applied = {}                   # chat_id -> 调整值
        self.usage_history = []
        self.last_percent = None
        self.last_alert_day = None
        self.last_report_day = None
        self.surveyed = False
        self.tasks = []

        self.strategy = "balanced"
        self.state_file = None

    # ---------- 生命周期 ----------
    async def on_load(self):
        self.state_file = None
        try:
            data_dir = self.ctx.paths.data_dir
            if data_dir:
                self.state_file = os.path.join(str(data_dir), "minimax_governor_state.json")
        except Exception:
            pass
        self._load_state()
        self.ctx.logger.info("MiniMax 开支管家已加载（策略=%s）", self.strategy)
        self.tasks.append(asyncio.create_task(self._gov_loop()))
        self.tasks.append(asyncio.create_task(self._api_loop()))

    async def on_unload(self):
        for t in self.tasks:
            t.cancel()
        # 归还所有频率调整，避免插件停用后机器人仍带着限流跑
        for cid in list(self.applied.keys()):
            try:
                await self.ctx.frequency.set_adjust(cid, 0.0)
            except Exception as e:
                self.ctx.logger.warning("管家卸载归还调整失败 %s: %s", cid, e)
        self.applied.clear()

    async def on_config_update(self, scope, config_data, version):
        # WebUI 保存配置后，立即同步运行中的策略，无需重启
        new_strategy = str(self._c("strategy", "balanced") or "balanced")
        if new_strategy in STRATEGIES and new_strategy != self.strategy:
            self.strategy = new_strategy
            self._save_state()
        self.ctx.logger.info("管家: 配置已更新（version=%s），当前策略=%s", version, self.strategy)

    # ---------- 配置读取 ----------
    def _c(self, key, default=None):
        """优先从强类型配置模型取值，兼容旧版字典式配置"""
        try:
            section = getattr(self.config, "minimax_governor", None)
            if section is not None and hasattr(section, key):
                val = getattr(section, key)
                if val is not None:
                    return val
        except Exception:
            pass
        cfg = self.config.get("minimax_governor", {}) if isinstance(self.config, dict) else {}
        val = cfg.get(key)
        return default if val is None else val

    def _load_state(self):
        if not self.state_file or not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            s = state.get("strategy", "balanced")
            if s in STRATEGIES:
                self.strategy = s
        except Exception as e:
            self.ctx.logger.warning("管家状态读取失败: %s", e)

    def _save_state(self):
        if not self.state_file:
            return
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump({"strategy": self.strategy}, f)
        except Exception as e:
            self.ctx.logger.warning("管家状态保存失败: %s", e)

    # ---------- 消息处理 ----------
    def _extract_chat_id(self, message):
        if not isinstance(message, dict):
            return None
        for path in (("chat_id",), ("stream_id",),
                     ("chat_info", "chat_id"), ("chat_info", "stream_id"),
                     ("message_info", "chat_info", "chat_id")):
            cur = message
            ok = True
            for p in path:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    ok = False
                    break
            if ok and isinstance(cur, str):
                return cur
        return None

    def _trim(self):
        cutoff = time.time() - WINDOW
        while self.replies and self.replies[0][0] < cutoff:
            self.replies.popleft()

    def _budget_for(self, chat_id):
        """分群独立预算：chat_id 里包含群号则用群号匹配，否则用默认"""
        base = float(self._c("default_reply_budget_per_hour", 40) or 40)
        budgets = self._c("group_budgets", {}) or {}
        mult = STRATEGIES.get(self.strategy, STRATEGIES["balanced"])["budget_mult"]
        if isinstance(budgets, dict) and isinstance(chat_id, str):
            for gid, val in budgets.items():
                if gid and str(gid) in chat_id:
                    try:
                        base = float(val)
                    except (TypeError, ValueError):
                        pass
                    break
        return base * mult

    @EventHandler("in_counter", event_type=EventType.ON_MESSAGE)
    async def count_in(self, **kwargs):
        msg = kwargs.get("message")
        cid = self._extract_chat_id(msg)
        if not self.surveyed:
            self.surveyed = True
            self.ctx.logger.info("管家[入站] 消息字段: %s -> chat_id=%s",
                                 list(msg.keys()) if isinstance(msg, dict) else type(msg), cid)
        if cid:
            self.chat_ids.add(cid)

    @EventHandler("out_counter", event_type=EventType.POST_SEND)
    async def count_out(self, **kwargs):
        cid = self._extract_chat_id(kwargs.get("message"))
        self.replies.append((time.time(), cid or "__global__"))
        if cid:
            self.chat_ids.add(cid)

    # ---------- 套餐查询 ----------
    async def fetch_balance(self):
        key = str(self._c("api_key", "") or "")
        gid = str(self._c("group_id", "") or "")
        if not key or not gid:
            return None, "未配置 api_key / group_id"
        headers = {"Authorization": f"Bearer {key}"}
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in ENDPOINTS:
                try:
                    async with session.get(url.format(gid=gid), headers=headers) as resp:
                        text = await resp.text()
                        try:
                            data = json.loads(text)
                        except Exception:
                            data = {"raw": text[:400]}
                        base = data.get("base_resp", {}) if isinstance(data, dict) else {}
                        if isinstance(base, dict) and base.get("status_code") in (0, None):
                            return data, None
                        self.ctx.logger.info("管家: %s -> %s", url.split("/")[2], str(data)[:200])
                except Exception as e:
                    self.ctx.logger.warning("管家请求失败 %s: %s", url.split("/")[2], e)
        return None, "余额接口全部失败"

    def _record(self, data):
        nums = _find_numbers(data)
        self.usage_history.append((time.time(), nums))
        self.usage_history = self.usage_history[-96:]
        for k, v in nums.items():
            if "percent" in k.lower() and isinstance(v, (int, float)):
                self.last_percent = v
        return nums

    def _burn_estimate(self):
        for i in range(len(self.usage_history) - 1, 0, -1):
            t1, n1 = self.usage_history[i - 1]
            t2, n2 = self.usage_history[i]
            hours = (t2 - t1) / 3600
            if hours <= 0:
                continue
            for k in n1:
                if k in n2 and isinstance(n1[k], (int, float)):
                    delta = n2[k] - n1[k]
                    if delta < 0:
                        rate = -delta / hours
                        eta = (n2[k] / rate / 24) if rate else None
                        return k, rate, eta
        return None

    def _fmt(self, nums):
        if not nums:
            return "接口没返回可识别的数值字段"
        return "\n".join(f"· {k} = {v}" for k, v in list(nums.items())[:10])

    # ---------- 调速 ----------
    async def _apply(self, cid, target, reason):
        if self.applied.get(cid) == target:
            return
        try:
            ok = await self.ctx.frequency.set_adjust(cid, target)
            if ok:
                self.applied[cid] = target
                self.ctx.logger.info("管家: %s -> chat=%s 调整值 %.2f", reason, cid, target)
        except Exception as e:
            self.ctx.logger.warning("管家 set_adjust 失败 %s: %s", cid, e)

    async def _alert(self, text):
        sid = str(self._c("notify_stream_id", "") or "")
        if not sid:
            return
        try:
            await self.ctx.send.text("【MiniMax 开支管家】" + text, sid)
        except Exception as e:
            self.ctx.logger.warning("管家提醒发送失败: %s", e)

    async def _gov_loop(self):
        await asyncio.sleep(60)
        last_report_date = None
        while True:
            try:
                self._trim()
                hard_mult = float(self._c("hard_multiplier", 1.5) or 1.5)
                preset = STRATEGIES.get(self.strategy, STRATEGIES["balanced"])
                recover_ratio = preset["recover"]
                throttle = preset["throttle"]
                counts = defaultdict(int)
                for _, cid in self.replies:
                    counts[cid] += 1
                low_pct = float(self._c("low_percent", 20) or 20)
                percent_low = self.last_percent is not None and self.last_percent < low_pct
                for cid in list(self.chat_ids):
                    budget = self._budget_for(cid)
                    hard = budget * hard_mult
                    n = counts.get(cid, 0)
                    if percent_low:
                        await self._apply(cid, throttle, f"套餐余量仅 {self.last_percent}%")
                    elif n >= hard:
                        await self._apply(cid, throttle, f"回复 {n} 条 触及硬上限（预算{budget:.0f}）")
                    elif n >= budget:
                        ratio = (n - budget) / max(1.0, hard - budget)
                        await self._apply(cid, throttle * (0.5 + 0.5 * ratio),
                                          f"回复 {n} 条 超预算{budget:.0f}")
                    elif n < budget * recover_ratio and self.applied.get(cid, 0) != 0:
                        await self._apply(cid, 0.0, f"回复 {n} 条 回落至预算内")

                # 每日消费日报
                now = datetime.now()
                today = now.strftime("%Y-%m-%d")
                report_hour = int(self._c("daily_report_hour", 21) or 21)
                if now.hour == report_hour and last_report_date != today:
                    last_report_date = today
                    self._trim()
                    await self._alert(self._daily_report(len(self.replies)))
            except Exception as e:
                self.ctx.logger.warning("管家调速循环异常: %s", e)
            await asyncio.sleep(60)

    def _daily_report(self, replies_last_hour):
        lines = ["📊 MiniMax 消费日报"]
        if self.last_percent is not None:
            lines.append(f"· 套餐余量：{self.last_percent}%")
        est = self._burn_estimate()
        if est:
            k, rate, eta = est
            eta_txt = f"，按此速度约能用 {eta:.1f} 天" if eta else ""
            lines.append(f"· 消耗速度：{k} 每小时约 {rate:.1f}{eta_txt}")
        lines.append(f"· 最近 1 小时回复：{replies_last_hour} 条")
        lines.append(f"· 当前策略：{self.strategy}")
        return "\n".join(lines)

    async def _api_loop(self):
        await asyncio.sleep(45)
        while True:
            try:
                data, err = await self.fetch_balance()
                if data:
                    nums = self._record(data)
                    est = self._burn_estimate()
                    if est:
                        k, rate, eta = est
                        eta_txt = f"，按此速度约能用 {eta:.1f} 天" if eta else ""
                        self.ctx.logger.info("管家: %s 每小时消耗 %.1f%s", k, rate, eta_txt)
                    today = time.strftime("%Y-%m-%d")
                    low_pct = float(self._c("low_percent", 20) or 20)
                    for k, v in nums.items():
                        if ("percent" in k.lower() and isinstance(v, (int, float))
                                and v < low_pct and self.last_alert_day != today):
                            self.last_alert_day = today
                            await self._alert(f"⚠️ 套餐余量仅剩 {v}%，建议省流或续费！")
            except Exception as e:
                self.ctx.logger.warning("管家查询循环异常: %s", e)
            try:
                interval = max(5, int(self._c("check_interval_minutes", 30) or 30))
            except Exception:
                interval = 30
            await asyncio.sleep(interval * 60)


def create_plugin():
    return MiniMaxGovernor()
