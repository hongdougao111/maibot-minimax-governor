import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

import aiohttp

from maibot_sdk import Command, EventHandler, MaiBotPlugin
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


class MiniMaxGovernor(MaiBotPlugin):
    def __init__(self):
        super().__init__()
        self.replies = deque()              # (ts, chat_id)
        self.chat_ids = set()
        self.applied = {}                   # chat_id -> 调整值
        self.usage_history = []
        self.last_percent = None
        self.last_alert_day = None
        self.surveyed = False
        self.tasks = []

        self.strategy = "balanced"
        self.free_until = None              # 老爹开了"随便聊"后的截止时间
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

    async def on_config_update(self, scope, config_data, version):
        pass

    # ---------- 配置与状态 ----------
    def _cfg(self, key, default):
        cfg = self.config.get("minimax_governor", {}) if isinstance(self.config, dict) else {}
        return cfg.get(key, default)

    def _load_state(self):
        if not self.state_file or not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            self.strategy = state.get("strategy", "balanced")
            if self.strategy not in STRATEGIES:
                self.strategy = "balanced"
            fu = state.get("free_until")
            self.free_until = datetime.fromisoformat(fu) if fu else None
        except Exception as e:
            self.ctx.logger.warning("管家状态读取失败: %s", e)

    def _save_state(self):
        if not self.state_file:
            return
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump({"strategy": self.strategy,
                           "free_until": self.free_until.isoformat() if self.free_until else None}, f)
        except Exception as e:
            self.ctx.logger.warning("管家状态保存失败: %s", e)

    def _is_free_now(self):
        if self.free_until and datetime.now() < self.free_until:
            return True
        if self.free_until and datetime.now() >= self.free_until:
            self.free_until = None   # 到期自动恢复
            self._save_state()
        return False

    def _admin_check(self, kwargs):
        """校验命令发送者是不是管理员；无法识别时放行并记录日志"""
        admin = str(self._cfg("admin_qq", ""))
        uid = self._find_user_id(kwargs.get("message"))
        if uid is None:
            self.ctx.logger.info("管家: 无法识别命令发送者，默认放行（建议核对日志）")
            return True
        if str(uid) == admin:
            return True
        return False

    def _find_user_id(self, obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "user_id" and v is not None:
                    return v
                r = self._find_user_id(v)
                if r is not None:
                    return r
        elif isinstance(obj, list):
            for item in obj:
                r = self._find_user_id(item)
                if r is not None:
                    return r
        return None

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
        base = float(self._cfg("default_reply_budget_per_hour", 40))
        budgets = self._cfg("group_budgets", {})
        mult = STRATEGIES.get(self.strategy, STRATEGIES["balanced"])["budget_mult"]
        if isinstance(budgets, dict) and isinstance(chat_id, str):
            for gid, val in budgets.items():
                if gid and str(gid) in chat_id:
                    base = float(val)
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
        key = self._cfg("api_key", "")
        gid = self._cfg("group_id", "")
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
    def _trim(self):
        cutoff = time.time() - WINDOW
        while self.replies and self.replies[0][0] < cutoff:
            self.replies.popleft()

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
        sid = self._cfg("notify_stream_id", "")
        if not sid:
            return
        try:
            await self.ctx.send.text("【MiniMax 开支管家】" + text, sid)
        except Exception as e:
            self.ctx.logger.warning("管家提醒发送失败: %s", e)

    async def _gov_loop(self):
        await asyncio.sleep(60)
        while True:
            try:
                self._trim()
                if self._is_free_now():
                    # 老爹开了随便聊：全部放开
                    for cid in list(self.chat_ids):
                        await self._apply(cid, 0.0, "省流关生效中")
                else:
                    hard_mult = float(self._cfg("hard_multiplier", 1.5))
                    preset = STRATEGIES.get(self.strategy, STRATEGIES["balanced"])
                    recover_ratio = preset["recover"]
                    throttle = preset["throttle"]
                    counts = defaultdict(int)
                    for _, cid in self.replies:
                        counts[cid] += 1
                    percent_low = (self.last_percent is not None
                                   and self.last_percent < float(self._cfg("low_percent", 20)))
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
                        elif self.applied.get(cid, 0) != 0:
                            await self._apply(cid, self.applied[cid] * 0.5, "逐步恢复")
            except Exception as e:
                self.ctx.logger.warning("管家调速循环异常: %s", e)
            await asyncio.sleep(60)

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
                    for k, v in nums.items():
                        if ("percent" in k.lower() and isinstance(v, (int, float))
                                and v < float(self._cfg("low_percent", 20))
                                and self.last_alert_day != today):
                            self.last_alert_day = today
                            await self._alert(f"⚠️ 套餐余量仅剩 {v}%，建议省流或续费！")
            except Exception as e:
                self.ctx.logger.warning("管家查询循环异常: %s", e)
            try:
                interval = max(5, int(self._cfg("check_interval_minutes", 30)))
            except Exception:
                interval = 30
            await asyncio.sleep(interval * 60)

    # ---------- 命令 ----------
    @Command("balance", pattern=r"^/余额$|^/balance$")
    async def cmd_balance(self, **kwargs):
        data, err = await self.fetch_balance()
        if err:
            await self.ctx.send.text("MiniMax 查询失败：" + err, kwargs["stream_id"])
            return True, err, 2
        nums = self._record(data)
        parts = ["【MiniMax 开支管家】", self._fmt(nums)]
        if self.last_percent is not None:
            parts.append(f"套餐余量：{self.last_percent}%")
        est = self._burn_estimate()
        if est:
            k, rate, eta = est
            eta_txt = f"，预计还能用 {eta:.1f} 天" if eta else ""
            parts.append(f"消耗速度：{k} 每小时约 {rate:.1f}{eta_txt}")
        parts.append(f"本小时回复：{len(self.replies)} 条")
        parts.append(f"当前策略：{self.strategy}" + ("｜省流关生效中" if self._is_free_now() else ""))
        parts.append("原始返回：" + json.dumps(data, ensure_ascii=False)[:300])
        await self.ctx.send.text("\n".join(parts), kwargs["stream_id"])
        return True, "ok", 2

    @Command("saveliu_off", pattern=r"^/省流关$")
    async def cmd_saveliu_off(self, **kwargs):
        if not self._admin_check(kwargs):
            await self.ctx.send.text("哼，只有管理员能动这个开关哦～", kwargs["stream_id"])
            return True, "denied", 2
        self.free_until = datetime.now() + timedelta(hours=12)
        self._save_state()
        for cid in list(self.chat_ids):
                        await self._apply(cid, 0.0, "省流关：管理员解锁随便聊")
        await self.ctx.send.text("好～今晚省流模式关闭，我放开聊！到明早自动恢复哦～", kwargs["stream_id"])
        return True, "ok", 2

    @Command("saveliu_on", pattern=r"^/省流开$")
    async def cmd_saveliu_on(self, **kwargs):
        if not self._admin_check(kwargs):
            await self.ctx.send.text("哼，只有管理员能动这个开关哦～", kwargs["stream_id"])
            return True, "denied", 2
        self.free_until = None
        self._save_state()
        await self.ctx.send.text("收到，省流模式恢复运行，我会看好钱包的～", kwargs["stream_id"])
        return True, "ok", 2

    @Command("strategy_c", pattern=r"^/策略保守$")
    async def cmd_strategy_c(self, **kwargs):
        return await self._set_strategy("conservative", "保守", kwargs)

    @Command("strategy_b", pattern=r"^/策略均衡$")
    async def cmd_strategy_b(self, **kwargs):
        return await self._set_strategy("balanced", "均衡", kwargs)

    @Command("strategy_a", pattern=r"^/策略激进$")
    async def cmd_strategy_a(self, **kwargs):
        return await self._set_strategy("aggressive", "激进", kwargs)

    async def _set_strategy(self, key, label, kwargs):
        if not self._admin_check(kwargs):
            await self.ctx.send.text("哼，只有管理员能动这个开关哦～", kwargs["stream_id"])
            return True, "denied", 2
        self.strategy = key
        self._save_state()
        await self.ctx.send.text(f"已切换到{label}策略：预算×{STRATEGIES[key]['budget_mult']}，"
                                 f"限流深度 {STRATEGIES[key]['throttle']}，即刻生效～",
                                 kwargs["stream_id"])
        return True, "ok", 2


def create_plugin():
    return MiniMaxGovernor()
