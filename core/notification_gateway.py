# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 — 企微 Webhook 异步推送网关（与选股引擎解耦）

设计要点：
- 仅使用企业微信机器人 markdown 消息；发送在 **max_workers=3 的全局 ThreadPoolExecutor** 中执行，不阻塞调用方。
- 内存防刷：``_pushed_records`` 键为「战区(pool_key_for_dedup) + 证券代码 + 战法原文(规范化)」，默认 30 分钟内同股同战法不重复推送；同股不同战法可连续推送。跨自然日（北京时间）整表清空，避免字典在极端高频下的长尾膨胀。
- 信号层去重：P3/P4 新增「当日同池同代码仅推一次（跨战法、跨主池/观察池）」硬防重，避免同票在盘中/尾盘反复刷屏；P2/P5 仍保持 30 分钟滑动窗口去重。
- HTTP 强制 (connect, read) 超时 + 失败退避重试，降低瞬时断网导致的高分信号丢失概率。
- format 层对缺字段、NaN、非有限浮点一律兜底，禁止拼接异常废整条消息。
- clear_push_cache / send_heartbeat：Daemon 早安例行清空防刷字典并推送系统状态（走同一总闸与 HTTP 重试）。
- 系统运维告警（notify_wechat_system_alert）：与 P2～P5 股票推送字典独立；去重账本为 ``data/runtime/alert_dedup_cache.json``（atomic_json_update），
  跨进程共享，同一告警特征码在冷却窗口内（默认 12h，可 per-call 覆盖）仅一条。
"""
from __future__ import annotations

# Standard library
import atexit
import hashlib
import html
import json
import logging
import math
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

# Local modules
from core.runtime_data_paths import path_wechat_signal_dedup_cache_json
from core.stock_name_utils import normalize_stock_display_name


def _get_wechat_webhook_url(cfg_key: str = "wechat_webhook_url") -> str:
    """获取企微 webhook URL。委托给 config_manager.get_notification_config 处理优先级。"""
    from core.config_manager import get_notification_config
    cfg = get_notification_config()
    return str(cfg.get(cfg_key) or "").strip()


def _load_notification_config() -> Dict[str, Any]:
    """读取 notification 配置。直接使用 config_manager.get_notification_config。"""
    from core.config_manager import get_notification_config
    return get_notification_config()

logger = logging.getLogger(__name__)

# 企微 HTTP 发送统一走有界线程池，避免瞬时无限 Thread 与 429；与 _send_markdown_sync 内重试配合限流。
_WECHAT_WEBHOOK_EXECUTOR = ThreadPoolExecutor(
    max_workers=3,
    thread_name_prefix="wechat-webhook",
)


def _shutdown_wechat_executor() -> None:
    try:
        _WECHAT_WEBHOOK_EXECUTOR.shutdown(wait=False, cancel_futures=False)
    except TypeError:
        _WECHAT_WEBHOOK_EXECUTOR.shutdown(wait=False)
    except Exception:
        pass


atexit.register(_shutdown_wechat_executor)

_SPAM_WINDOW_SEC = 30 * 60
# 运维/数据类告警独立去重（与信号推送 30 分钟字典分离）；同一 dedup 键默认 12h 仅推一条（落盘见 alert_dedup_cache.json）
_SYSTEM_ALERT_DEDUP_DEFAULT_SEC = 12 * 3600

# 企微 markdown 单条长度上限（官方约 4096）；留出余量截断，防爆内存与拒收
_MAX_MARKDOWN_CONTENT_LEN = 3800

# HTTP：连接超时 + 读取超时（秒），避免套接字永久挂死
_REQUEST_TIMEOUT = (5.0, 15.0)

# DeepSeek 分析建议兜底与提示词
_DEEPSEEK_FALLBACK_ADVICE = (
    "交易建议：观望\n"
    "当前优势：数据暂不可用，请参考系统原始指标\n"
    "当前不足：DeepSeek分析服务暂时不可用"
)
_DEEPSEEK_SYSTEM_PROMPT = (
    "你是一位客观中立的A股市场数据分析师，仅基于用户提供的结构化数据进行事实性分析，"
    "不对任何投资方向预设立场，不提供买卖建议。"
    "分析时请严格区分：【已确认事实】（数据直接可得）与【基于经验的推测】（须明确标注），"
    "不夸大有利因素，不淡化不利因素。"
    "请按以下固定格式返回，每个字段独立一行：\n"
    "关键数据摘录：[列出3-5个核心指标数值]\n"
    "值得关注的现象：[列出2-3个值得留意的数据特征，注明正面或负面]\n"
    "数据空白与不确定性：[列出1-2个因数据缺失导致无法判断的点]\n\n"
    "要求：1. 每个字段用短句描述，基于给出的具体数值；2. 不输出买卖建议；3. 如数据不足，明确说明哪些指标缺失。"
)


_DEEPSEEK_POOL_FOCUS = {
    "p2": "P2竞价池：集合竞价阶段。关注开盘涨幅、竞价量、换手率、量比与昨日成交的对比。",
    "p3": "P3盘中池：盘中交易阶段。关注实时涨幅、量比变化、均线位置、主力资金流向与成交额。",
    "p4": "P4尾盘池：尾盘收盘阶段。关注收盘涨幅、全天量价配合、尾盘成交节奏、均线偏离与市场整体情绪。",
    "p5": "P5盘后池：收盘复盘阶段。关注全天涨幅、量比、趋势指标、主力资金、换手率、行业板块与财务数据。",
}

_DEEPSEEK_POOL_SEMANTICS = {
    "p2": {
        "name": "竞价阶段",
        "time_window": "集合竞价至开盘",
        "core_question": "开盘价格与成交量的基本结构",
        "what_to_observe": "竞价量能、开盘涨幅、换手率量比相对于昨日成交的水平",
        "what_to_note": "注意甄别：竞价量高开未必代表全天强势，需结合昨日走势和当日市场环境综合判断",
    },
    "p3": {
        "name": "盘中阶段",
        "time_window": "盘中连续竞价",
        "core_question": "全天价格与成交量变化的主要特征",
        "what_to_observe": "实时涨幅变化、量比波动、均线系统位置、主力资金净流入方向与成交额",
        "what_to_note": "注意甄别：量比放大需区分主动性买盘还是对倒；主力净流入需看持续性而非单一时点",
    },
    "p4": {
        "name": "尾盘阶段",
        "time_window": "尾盘收盘前30分钟",
        "core_question": "尾盘价格行为与全天走势的呼应关系",
        "what_to_observe": "尾盘涨幅、成交量变化、均线偏离度、尾盘成交额占全天比例与市场情绪指标",
        "what_to_note": "注意甄别：尾盘急涨可能为次日出货留空间；无量推涨的持续性存疑",
    },
    "p5": {
        "name": "盘后阶段",
        "time_window": "收盘后复盘",
        "core_question": "全天走势的技术面与资金面特征，以及财务数据的基本情况",
        "what_to_observe": "全天涨幅量价比、趋势指标状态、主力资金净额与方向、换手率水平、行业板块与财务数据",
        "what_to_note": "注意甄别：单日数据有限，历史规律不等于未来走势；财务数据存在滞后性",
    },
}

_DEEPSEEK_TACTIC_KNOWLEDGE = {
    "竞价": {
        "meaning": "开盘前后资金抢筹或试盘信号，核心看高开质量与量能真实性。",
        "data_positive": "开盘涨幅绝对值；量比相对历史均量的水平；换手率绝对水平；竞价成交额相对昨日水平。",
        "data_negative": "高开幅度与量比是否匹配；量比是否处于极端区间；换手率是否异常偏高。",
    },
    "主升": {
        "meaning": "趋势进入加速或延续阶段，核心看均线多头、MA20斜率和资金持续性。",
        "data_positive": "MA20/MA60相对位置；MA20斜率方向与大小；MACD柱状图方向；量比绝对水平。",
        "data_negative": "价格与MA20的偏离百分比；量比是否处于历史高位；MACD柱是否收缩。",
    },
    "机构": {
        "meaning": "机构/大资金连续介入迹象，核心看资金净额与承接稳定性。",
        "data_positive": "主力净流入绝对额；成交额放大比例；换手率变化趋势。",
        "data_negative": "主力净流入是否为单日现象；大单成交频次；尾盘是否有撤单迹象。",
    },
    "苍穹": {
        "meaning": "高强度突破或强趋势结构，通常要求筹码/趋势/量能共振。",
        "data_positive": "价格与均线/平台的距离；量比与历史对比；筹码分布集中度。",
        "data_negative": "当前价格处于历史高位区间的位置；量比是否过热；筹码集中度是否提示派发压力。",
    },
    "底仓": {
        "meaning": "适合观察或小仓试错的中低位资金点火，重视安全边际。",
        "data_positive": "量比相对历史均量水平；MA20方向；主力资金流向基本方向。",
        "data_negative": "板块是否属于主线；量比放大是否仅为单日；是否有持续资金跟进迹象。",
    },
    "尾盘": {
        "meaning": "收盘前资金选择方向，核心判断次日溢价概率。",
        "data_positive": "尾盘涨幅；尾盘成交额占全天比例；尾盘量比变化。",
        "data_negative": "尾盘成交占全天比例是否异常偏低或偏高；尾盘涨幅与全天涨幅的偏离。",
    },
    "真龙": {
        "meaning": "盘后综合强势候选，要求趋势、资金、题材和基本面至少三项共振。",
        "data_positive": "趋势指标方向；量价配合情况；资金流入绝对额；行业板块排名。",
        "data_negative": "各指标是否有相互矛盾之处；量价是否仅为单日异动；财务数据是否有明显异常。",
    },
    "缩量": {
        "meaning": "缩量期的量价行为特征，关注承接与二次放量的可能性。",
        "data_positive": "缩量程度；价格与关键位的偏离。",
        "data_negative": "缩量持续时间；价格是否在关键支撑附近；次日是否重新放量。",
    },
    "直通车": {
        "meaning": "系统强信号快速入池，通常已有硬过滤通过，但仍需判断当下性价比。",
        "data_positive": "各项筛选指标的绝对数值；各指标之间的方向一致性。",
        "data_negative": "涨幅是否已透支；量比是否处于极端高位；是否有指标之间相互矛盾。",
    },
    "低吸": {
        "meaning": "回踩时的量价行为特征，关注支撑有效性与量能变化。",
        "data_positive": "回踩幅度；缩量程度；支撑位距离。",
        "data_negative": "回踩是否放量；主力资金流向是否转负；支撑位是否失守。",
    },
    "弱转强": {
        "meaning": "前一阶段分歧或弱势后，次日/盘中主动转强，核心看资金是否重新定价。",
        "data_positive": "高开不炸、放量过前高、站稳VWAP，主力资金由负转正或明显回流。",
        "data_negative": "高开是否有量能配合；板块整体表现；开盘后价格是否快速回落。",
    },
    "反包": {
        "meaning": "用强阳线修复上一交易日阴线或分歧，核心看是否完成情绪修复。",
        "data_positive": "阳线实体幅度；量比；收盘价与关键位的距离。",
        "data_negative": "阳线是否带长上影；尾盘是否有撤单；量能是否配合。",
    },
    "N字": {
        "meaning": "上涨—回踩—再上攻的趋势延续结构，核心看第二段上攻质量。",
        "data_positive": "回踩幅度；第二段上攻量比；价格与前高的关系。",
        "data_negative": "回踩是否破前低；第二段量比是否低于第一段；是否形成双头形态。",
    },
    "首阴": {
        "meaning": "强势上涨后的第一根阴线，关注是否是健康分歧而非趋势结束。",
        "data_positive": "阴线量能；价格与均线的距离。",
        "data_negative": "阴线是否放量；是否跌破关键均线；主力资金是否大幅流出。",
    },
    "二波": {
        "meaning": "第一轮上涨休整后再启动，核心看筹码消化和新资金接力。",
        "data_positive": "调整幅度；突破时量比；板块热度。",
        "data_negative": "调整时间是否充分；突破是否带量；板块是否仍有主线属性。",
    },
    "趋势加速": {
        "meaning": "趋势斜率提升阶段的价格行为特征，关注量能与价格的配合关系。",
        "data_positive": "MA20斜率变化；量比绝对水平。",
        "data_negative": "价格与均线偏离度；量比是否过热；尾盘是否有松动迹象。",
    },
    "平台突破": {
        "meaning": "横盘整理后突破箱体，核心看突破是否有效和是否带量。",
        "data_positive": "突破量比；收盘价与突破位的关系；回踩是否破突破位。",
        "data_negative": "突破是否缩量；回踩是否破突破位；突破位是否临近强压力。",
    },
    "回封": {
        "meaning": "炸板或分歧后重新封回，核心看分歧后的资金承接强度。",
        "data_positive": "回封速度；封单量；板块情绪。",
        "data_negative": "炸板次数；尾盘封单是否稳定；封单量级是否足够。",
    },
    "卡位": {
        "meaning": "同题材或同梯队中替代弱者成为资金前排，核心看相对强度。",
        "data_positive": "相对板块涨幅；量价比；是否率先突破。",
        "data_negative": "板块整体是否走弱；是否仅为短暂抢跑；后排是否跟随。",
    },
    "分歧转一致": {
        "meaning": "市场或个股经历分歧后资金重新统一方向，是强势股二次确认信号。",
        "data_positive": "承接深度；修复量比；板块情绪。",
        "data_negative": "承接是否破位；量比是否极端偏高；价格是否处于高位。",
    },
    "突破": {
        "meaning": "突破关键价格、均线或平台，核心看是否有成交量与资金确认。",
        "data_positive": "突破量比；突破后回踩是否破突破位；资金流向。",
        "data_negative": "突破是否无量；是否临近强压力；回踩是否破突破位。",
    },
    "回踩": {
        "meaning": "上涨途中的技术性回落，核心看支撑是否有效。",
        "data_positive": "回踩幅度；缩量程度；支撑位是否有效。",
        "data_negative": "回踩是否放量；是否破关键支撑；资金是否持续流出。",
    },
    "承接": {
        "meaning": "卖压出现时仍有资金接住，核心判断筹码和资金是否稳定。",
        "data_positive": "低点与VWAP的关系；成交额变化趋势。",
        "data_negative": "是否跌破均价线；尾盘资金是否撤退；低点是否持续下移。",
    },
    "放量": {
        "meaning": "资金参与度提升，必须结合位置判断是攻击还是派发。",
        "data_positive": "量比；价格变化方向；成交额绝对值。",
        "data_negative": "量比极端程度；价格是否滞涨；是否在高位出现长上影。",
    },
    "缩量回踩": {
        "meaning": "趋势内健康整理形态，核心看缩量是否守住关键位。",
        "data_positive": "缩量程度；价格与均线关系。",
        "data_negative": "次日是否重新放量；价格是否破均线；缩量持续时间。",
    },
    "首板": {
        "meaning": "第一根涨停或首次强确认，核心看题材新鲜度和封板质量。",
        "data_positive": "量比；封单量；板块助攻情况；开板次数。",
        "data_negative": "是否孤立首板；尾盘封单是否稳定；封单量级是否充足。",
    },
    "连板": {
        "meaning": "连续涨停的情绪强势结构，核心看高度与分歧承接。",
        "data_positive": "连板高度；换手率；回封速度；板块情绪。",
        "data_negative": "连板高度；量比是否过高；是否处于监管关注区间。",
    },
    "龙头": {
        "meaning": "题材或市场最强核心标的，核心看带动性与辨识度。",
        "data_positive": "相对涨幅；成交额；封单量；板块带动性。",
        "data_negative": "高位量比；后排是否掉队；自身是否出现滞涨迹象。",
    },
}

_DEEPSEEK_FIELD_ALIASES = {
    "code": ("代码", "code", "ts_code", "symbol"),
    "name": ("名称", "name", "stock_name", "证券简称", "股票简称"),
    "price": ("现价", "price", "close", "latest_price"),
    "pct_chg": ("涨幅", "pct_chg", "pct", "change_pct"),
    "realtime_pct_chg": ("realtime_pct_chg", "实时涨幅", "rt_pct_chg"),
    "prev_close": ("昨收", "prev_close", "pre_close"),
    "volume": ("volume", "vol", "成交量"),
    "turnover_rate": ("turnover_rate", "换手率"),
    "realtime_turnover_rate": ("realtime_turnover_rate", "实时换手率", "rt_turnover_rate"),
    "amount": ("amount", "成交额", "成交金额"),
    "volume_ratio": ("volume_ratio", "量比", "量比_昨", "盘前/昨日量比"),
    "realtime_volume_ratio": ("realtime_volume_ratio", "实时量比", "rt_volume_ratio"),
    "avg_vol_n": ("avg_vol_n", "avg_vol_5", "avg_vol_10", "5日均量", "10日均量"),
    "ma5": ("ma5", "MA5"),
    "ma10": ("ma10", "MA10"),
    "ma20": ("ma20", "MA20"),
    "ma60": ("ma60", "MA60"),
    "vwap": ("vwap", "VWAP"),
    "macd_dif": ("macd_dif", "DIF", "dif"),
    "macd_dea": ("macd_dea", "DEA", "dea"),
    "macd_bar": ("macd_bar", "MACD柱", "macd"),
    "k_shape": ("k_shape", "K线形态", "kline_shape"),
    "resistance": ("resistance", "压力位"),
    "support": ("support", "支撑位"),
    "tactic": ("战法", "tactic", "strategy"),
    "tactic_code": ("tactic_code", "战法代码", "strategy_code"),
    "tactic_name": ("tactic_name", "战法名称", "strategy_name"),
    "entry_reason": ("入池理由", "买入原因", "why_selected", "selection_reason", "entry_reason"),
    "data_negative": ("风险标签", "risk", "risk_tags"),
    "exec_tier": ("执行层级", "执行建议", "exec_tier"),
    "position": ("建议仓位", "position", "position_hint"),
    "score": ("综合分", "score", "burst_score", "p1_score"),
    "buy_hint": ("买入提示", "wechat_hint", "buy_hint"),
    "pool_tier": ("pool_tier",),
    "pool_source": ("pool_source",),
    "deepseek_fin_text": ("deepseek_fin_text", "财报摘要"),
    "annual_report": ("annual_report", "年报"),
    "quarter_report": ("quarter_report", "季报"),
    "financial": ("financial", "财报", "finance"),
    "market_emotion": ("market_emotion", "市场情绪"),
    "emotion_score": ("emotion_score", "情绪分数"),
    "top_industry": ("top_industry", "主线行业", "industry"),
    "top_concept": ("top_concept", "主线概念", "concept", "概念"),
    "realtime_main_inflow_rate": ("realtime_main_inflow_rate", "实时主力净流入占比", "main_inflow_rate"),
    "realtime_triggered": ("realtime_triggered", "盘中是否已触发"),
    "realtime_above_vwap": ("realtime_above_vwap", "现价是否高于VWAP", "above_vwap"),
}

_DEEPSEEK_FINANCIAL_FIELDS = (
    "summary_text",
    "risk_flags",
    "report_type",
    "report_period",
    "revenue_yoy",
    "net_profit_yoy",
    "deduct_net_profit_yoy",
    "op_cash_flow",
    "asset_liab_rate",
    "goodwill",
    "accounts_receivable",
    "inventories",
)

# 发送失败后的重试次数（含首次共 attempts 次）；间隔秒（简易退避）
_WEBHOOK_MAX_ATTEMPTS = 3
_WEBHOOK_RETRY_SLEEP_SEC = 3.0
_WEBHOOK_CIRCUIT_FAILURE_THRESHOLD = 3
_WEBHOOK_CIRCUIT_OPEN_SEC = 15 * 60

# 防刷字典极端保护：超过该条数时强制收缩（正常 30 分钟滑动窗口远小于此）
_SPAM_RECORDS_HARD_CAP = 8000


def _path_alert_dedup_cache_file() -> str:
    """系统告警去重账本路径：``data/runtime/alert_dedup_cache.json``。"""
    try:
        from core.runtime_data_paths import path_alert_dedup_cache_json

        return path_alert_dedup_cache_json()
    except Exception:
        here = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(here)
        d = os.path.join(root, "data", "runtime")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, "alert_dedup_cache.json")


def _file_reserve_system_alert_slot(dedup_key: str, window_sec: float) -> bool:
    """
    跨进程系统告警去重：atomic_json_update 读写账本；True=允许发送并已写入时间戳，False=冷却期内跳过。
    """
    from core.file_utils import atomic_json_update

    win = max(60.0, min(float(window_sec), float(7 * 24 * 3600)))
    k = f"sys:{str(dedup_key)[:200]}"
    now = time.time()
    box: Dict[str, bool] = {"allow": False}

    def _upd(data: Dict[str, Any]) -> None:
        cutoff_age = max(14.0 * 24 * 3600, 2.0 * win)
        cutoff_ts = now - cutoff_age
        for sk in list(data.keys()):
            if not isinstance(sk, str):
                continue
            sv = data.get(sk)
            try:
                fv = float(sv)
            except (TypeError, ValueError):
                continue
            if fv < cutoff_ts:
                try:
                    del data[sk]
                except KeyError:
                    pass

        last = 0.0
        try:
            last = float(data.get(k, 0) or 0)
        except (TypeError, ValueError):
            last = 0.0

        if now - last < win:
            box["allow"] = False
            return

        data[k] = now
        box["allow"] = True

    try:
        atomic_json_update(_path_alert_dedup_cache_file(), _upd, timeout=8)
    except Exception as e:
        logger.warning("企微系统告警文件去重账本异常，本次放行发送以避免漏告: %s", e)
        return True

    return box["allow"]


# 战区展示名（与 UI 心智对齐）
_POOL_ALERT_TITLES = {
    "p2": ("🚨", "P2竞价突袭"),
    "p3": ("🚨", "P3盘中防守"),
    "p4": ("🚨", "P4尾盘盲狙"),
    "p5": ("🌙", "P5盘后真龙"),
}

_gateway_singleton: Optional["WechatNotificationGateway"] = None
_gateway_secondary_singleton: Optional["WechatNotificationGateway"] = None
_gateway_lock = threading.RLock()


def _bj_calendar_key() -> str:
    """北京时间日历键 YYYYMMDD，用于跨日清空防刷字典（与 A 股日界对齐）。"""
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    except Exception:
        return datetime.now().strftime("%Y%m%d")


def _bj_now_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """北京时间格式化时间字符串。"""
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime(fmt)
    except Exception:
        return datetime.now().strftime(fmt)


def _norm_ts_code(code: Any) -> str:
    s = str(code or "").strip()
    if not s:
        return ""
    s = s.split(".")[0]
    return s[:6] if len(s) >= 6 else s.zfill(6)[:6]


def _strategy_dedup_segment(raw: Any) -> str:
    """
    从 stock_dict「战法」字段构造去重键片段：去首尾空白、压缩连续空白。
    缺失或空串时使用占位 ``__NA__``，避免与真实战法冲突。
    """
    s = str(raw or "").strip()
    if not s:
        return "__NA__"
    return " ".join(s.split())


def _strategy_log_fragment(raw: Any) -> str:
    """日志用短文案，与 _strategy_dedup_segment 语义一致。"""
    sk = _strategy_dedup_segment(raw)
    if sk == "__NA__":
        return "(未填战法)"
    return sk if len(sk) <= 80 else sk[:77] + "..."


def _finite_float_or_none(val: Any) -> Optional[float]:
    """转 float；NaN/inf/异常一律 None，供涨幅与得分安全展示。"""
    if val is None:
        return None
    try:
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        x = float(val)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _parse_pct_to_float(pct: Any) -> float:
    """解析「12.34%」或数字为 float；无法解析返回 0.0。"""
    if pct is None:
        return 0.0
    if isinstance(pct, (int, float)):
        try:
            x = float(pct)
            if math.isnan(x) or math.isinf(x):
                return 0.0
            return x
        except (TypeError, ValueError):
            return 0.0
    s = str(pct).strip().replace("%", "")
    if not s or s.lower() in ("nan", "none", "--"):
        return 0.0
    try:
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return 0.0
        return x
    except (TypeError, ValueError):
        return 0.0


def _format_system_alert_body(title: str, detail: str, category: str) -> str:
    """企微 markdown：数据/扫描异常温馨提示（与交易信号格式区分）。"""
    try:
        from zoneinfo import ZoneInfo

        ts = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    d = (detail or "").strip()
    if len(d) > 1500:
        d = d[:1500] + "\n…(已截断)"
    lines = [
        "⚠️【小杰系统提示】",
        _esc(title),
        f"时间：{_esc(ts)}",
        f"类别：{_esc(category)}",
        "---",
    ]
    for ln in d.split("\n"):
        lines.append(_esc(ln))
    body = "\n".join(lines)
    if len(body) > _MAX_MARKDOWN_CONTENT_LEN:
        body = body[: _MAX_MARKDOWN_CONTENT_LEN - 20] + "\n…(已截断)"
    return body


def _esc(s: Any) -> str:
    """HTML 转义；任意类型转 str，避免 None 拼接炸栈。"""
    try:
        if s is None:
            return "--"
        if isinstance(s, float) and (math.isnan(s) or math.isinf(s)):
            return "--"
        return html.escape(str(s), quote=False)
    except Exception:
        return "--"


def _safe_score_display(stock_dict: Dict[str, Any]) -> str:
    """综合分：有限 float 则两位小数；否则 '--'。"""
    raw = stock_dict.get("综合分", None)
    x = _finite_float_or_none(raw)
    if x is None:
        try:
            s = str(raw).strip()
            return s if s else "--"
        except Exception:
            return "--"
    return f"{x:.2f}"


def _trim_deepseek_advice(raw: Any) -> str:
    """净洗 DeepSeek 返回：去掉标记头 + 每行截断至60字（保留格式三段式）。"""
    try:
        text = str(raw or "").strip()
    except Exception:
        return _DEEPSEEK_FALLBACK_ADVICE
    if not text:
        return _DEEPSEEK_FALLBACK_ADVICE
    # Remove prefix markers only
    for prefix in ("【DeepSeek分析建议】", "DeepSeek分析建议", "```", "```json", "```text"):
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
    # 三段式截断：每行超过60字则截断，并在末尾加…
    lines = text.split("\n")
    trimmed_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 去掉行首可能残留的标号（如 "1. " 或 "关键数据摘录："）
        line = line.lstrip("0123456789.、)） ")
        # 截断至60字
        if len(line) > 60:
            line = line[:59] + "…"
        if line:
            trimmed_lines.append(line)
    result = "\n".join(trimmed_lines)
    return result.strip() or _DEEPSEEK_FALLBACK_ADVICE


def _is_deepseek_empty_value(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return True
    if isinstance(val, str) and val.strip().lower() in ("", "--", "none", "null", "nan", "暂无"):
        return True
    return False


def _get_first_deepseek_value(stock_dict: Dict[str, Any], aliases: Any) -> Any:
    """从顶层、detail 以及 detail 内一层子字典中找字段；兼容数据库列不齐但策略中间结果已有字段。"""
    for key in aliases:
        if key in stock_dict:
            val = stock_dict.get(key)
            if not _is_deepseek_empty_value(val):
                return val
    detail = stock_dict.get("detail")
    if isinstance(detail, dict):
        for key in aliases:
            if key in detail:
                val = detail.get(key)
                if not _is_deepseek_empty_value(val):
                    return val
        for sub in detail.values():
            if not isinstance(sub, dict):
                continue
            for key in aliases:
                if key in sub:
                    val = sub.get(key)
                    if not _is_deepseek_empty_value(val):
                        return val
    return None


def _deepseek_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.strip().replace("%", "").replace(",", "")
            if val in ("", "--"):
                return None
        x = float(val)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _extract_tactic_keywords(tactic_text: Any, entry_reason: Any = None) -> List[str]:
    text = f"{str(tactic_text or '')} {str(entry_reason or '')}".strip().replace("_", " ")
    if not text:
        return []
    alias_map = {
        "低位吸筹": "低吸",
        "低吸": "低吸",
        "弱转强": "弱转强",
        "转强": "弱转强",
        "反包": "反包",
        "N字": "N字",
        "N型": "N字",
        "首阴": "首阴",
        "二波": "二波",
        "二次上攻": "二波",
        "趋势加速": "趋势加速",
        "加速": "趋势加速",
        "平台突破": "平台突破",
        "突破平台": "平台突破",
        "回封": "回封",
        "卡位": "卡位",
        "分歧转一致": "分歧转一致",
        "分歧一致": "分歧转一致",
        "一致转强": "分歧转一致",
        "竞价": "竞价",
        "主升": "主升",
        "机构": "机构",
        "苍穹": "苍穹",
        "底仓": "底仓",
        "尾盘": "尾盘",
        "真龙": "真龙",
        "缩量": "缩量",
        "缩量回踩": "缩量回踩",
        "直通车": "直通车",
        "突破": "突破",
        "回踩": "回踩",
        "承接": "承接",
        "放量": "放量",
        "首板": "首板",
        "连板": "连板",
        "龙头": "龙头",
    }
    keys: List[str] = []
    seen = set()
    for alias, canon in sorted(alias_map.items(), key=lambda x: len(x[0]), reverse=True):
        if alias in text and canon not in seen:
            seen.add(canon)
            keys.append(canon)
    return keys[:6]


def _build_deepseek_tactic_context(clean: Dict[str, Any], stock_dict: Dict[str, Any]) -> Dict[str, Any]:
    tactic = clean.get("tactic") or stock_dict.get("战法") or stock_dict.get("tactic")
    entry_reason = clean.get("entry_reason") or stock_dict.get("入池理由") or stock_dict.get("entry_reason")
    kws = _extract_tactic_keywords(tactic, entry_reason)
    if not kws:
        return {}
    out: Dict[str, Any] = {"matched_keywords": kws}
    tactic_map: Dict[str, Any] = {}
    for kw in kws:
        meta = _DEEPSEEK_TACTIC_KNOWLEDGE.get(kw)
        if meta:
            tactic_map[kw] = meta
    if tactic_map:
        out["knowledge"] = tactic_map
    return out


def _build_deepseek_pool_semantics(pool_key: str) -> Dict[str, Any]:
    pk = str(pool_key or "").strip().lower()
    meta = _DEEPSEEK_POOL_SEMANTICS.get(pk, {})
    return dict(meta) if isinstance(meta, dict) else {}


def _deepseek_pick_float(clean: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        x = _deepseek_float(clean.get(key))
        if x is not None:
            return x
    return None


def _enrich_deepseek_derived_fields(pool_key: str, clean: Dict[str, Any], stock_dict: Dict[str, Any]) -> None:
    """
    补充 DeepSeek 所需的核心字段。
    数据来源优先级：clean（策略层 rt 字典）> stock_dict（数据库 stock 表）> 推导计算
    注意：部分实时数据（主力净流入/量比/换手率）在特定策略或时段可能不可用，
    必须做兜底处理，不能强求。
    """
    # === 1. 综合分（score） ===
    score = _deepseek_pick_float(clean, "score", "综合分")
    if score is None:
        score = _finite_float_or_none(stock_dict.get("综合分", stock_dict.get("score", stock_dict.get("burst_score"))))
    if score is not None:
        clean.setdefault("score", round(float(score), 2))

    # === 2. 战法字段 ===
    tactic = str(clean.get("tactic") or "").strip()
    if tactic and "tactic_name" not in clean:
        clean["tactic_name"] = tactic.split("_")[-1].replace("[", "").replace("]", "")[:40]
    if tactic and "tactic_code" not in clean:
        prefix = tactic.split("_", 1)[0].strip()
        if prefix and any(ch.isdigit() for ch in prefix):
            clean["tactic_code"] = prefix[:20]

    # === 3. 价格 / VWAP ===
    price = _deepseek_pick_float(clean, "price")
    vwap = _deepseek_pick_float(clean, "vwap")
    if "realtime_above_vwap" not in clean and price is not None and vwap not in (None, 0.0):
        clean["realtime_above_vwap"] = bool(price >= float(vwap))
        clean["price_vs_vwap_pct"] = round((price / float(vwap) - 1.0) * 100.0, 2)

    # === 4. MA20 价格偏离 ===
    ma20 = _deepseek_pick_float(clean, "ma20")
    if price is not None and ma20 not in (None, 0.0):
        clean.setdefault("price_vs_ma20_pct", round((price / float(ma20) - 1.0) * 100.0, 2))

    # === 5. 主力资金净流入（A股核心字段） ===
    # 来源优先级：clean > stock_dict > stock_dict.detail
    main_net = _deepseek_float(
        clean.get("net_main_amount")
        or clean.get("主力净额")
        or clean.get("main_net_amount")
        or clean.get("main_net")
        or stock_dict.get("net_main_amount")
        or stock_dict.get("主力净额")
        or stock_dict.get("main_net_amount")
        or stock_dict.get("main_net")
    )
    # 从 detail 里再捞一次
    if main_net is None and isinstance(stock_dict.get("detail"), dict):
        main_net = _deepseek_float(stock_dict["detail"].get("net_main_amount"))
    amount = _deepseek_pick_float(clean, "amount")
    if main_net is not None and amount not in (None, 0.0):
        clean.setdefault("main_net_amount", round(float(main_net), 2))
        clean.setdefault("realtime_main_inflow_rate", round(float(main_net) / float(amount) * 100.0, 2))
    # 兜底：没有主力资金数据就不设这个字段，不强求

    # === 6. 量比 + 换手率（盘中核心指标） ===
    # 来源优先级：clean（策略 rt 字典）> stock_dict
    for key in ("volume_ratio", "量比", "realtime_volume_ratio", "rt_volume_ratio"):
        vr = _deepseek_float(clean.get(key))
        if vr is not None:
            clean.setdefault("volume_ratio", round(float(vr), 2))
            break
    for key in ("turnover_rate", "换手率", "realtime_turnover_rate", "rt_turnover_rate"):
        tr = _deepseek_float(clean.get(key))
        if tr is not None:
            clean.setdefault("turnover_rate", round(float(tr), 4))
            break
    # 这两个字段如果没有就跳过，不做推导（无法推导）

    # === 7. MACD 核心指标（策略层常计算） ===
    for key in ("macd_dif", "macd_dea", "macd_bar", "DIF", "DEA", "MACD柱"):
        if key in clean and clean.get(key) is not None:
            continue  # 已有则跳过
        for alt in (key, key.lower()):
            val = _deepseek_float(stock_dict.get(alt))
            if val is not None:
                clean.setdefault(key, round(float(val), 4))
                break

    # === 8. 支撑 / 压力位（策略层常有计算） ===
    for key in ("resistance", "压力位", "support", "支撑位"):
        if key in stock_dict:
            clean.setdefault(key, stock_dict[key])

    # === 9. 触发标记 & 执行层级 & 仓位建议 ===
    if "realtime_triggered" not in clean:
        clean["realtime_triggered"] = True
    if "exec_tier" not in clean and score is not None:
        clean["exec_tier"] = "A" if score >= 85 else "B" if score >= 65 else "C"
    if "position" not in clean and score is not None:
        clean["position"] = "待观察" if score < 85 else "可跟踪"

    # === 10. 风险标签 ===
    if "risk" not in clean:
        pct = _deepseek_pick_float(clean, "pct_chg", "realtime_pct_chg")
        risks = []
        if pct is not None and pct >= 7.0:
            risks.append("涨幅相对较高")
        vr = _deepseek_pick_float(clean, "realtime_volume_ratio", "volume_ratio")
        if vr is not None and vr >= 5.0:
            risks.append("量比相对较高")
        clean["risk"] = "；".join(risks) if risks else "无显著异常"

    # === 11. 市场环境 ===
    pk = str(pool_key or "").lower()
    if "market_emotion" not in clean:
        clean["market_emotion"] = "数据待补充" if pk == "p5" else "市场整体数据未入库"

    # === 12. P3 策略层特有字段 ===
    if pk == "p3":
        for key in ("burst_score", "surge_bonus", "penalty", "p3_core_screener_pass",
                     "p3_veto_reason", "p3_strategy_checks", "risk_tags",
                     "suggested_min_entry_score", "risk_control", "entry_reason"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val not in (None, "", []):
                    clean[key] = val
        # P3 特有指标：早盘量比/上影率/VWAP偏离
        for key in ("vr_morning_floor", "vr_1030", "upper_shadow_ratio", "vwap_deviation_pct",
                     "price_vs_vwap", "is_vwap_support", "regime_state", "regime_score"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val
        # P3 DuckDB Z-Score 附录
        for key in ("zscore_vol_60d", "zscore_mean", "zscore_std"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val
        # P3 T1 记忆因子
        for key in ("_t1_memory_boost", "_t1_win_rate_pct", "_t1_avg_ret_pct"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val

    # === 13. P4 策略层特有字段 ===
    if pk == "p4":
        for key in ("p4_core_screener_pass", "p4_veto_reason", "p4_strategy_checks",
                     "risk_tags", "suggested_min_entry_score", "risk_control",
                     "tail_signal", "tail_pattern", "tail_score"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val not in (None, "", []):
                    clean[key] = val
        # P4 行业/板块因子
        for key in ("industry", "sector", "industry_strength", "sector_rotation",
                     "concept_board", "hot_concept", "industry_rank"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val
        # P4 尾盘特有指标
        for key in ("close_price", "close_ma5_ratio", "close_ma10_ratio",
                     "late_volume_ratio", "late_main_net"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val

    # === 14. P5 策略层特有字段 ===
    if pk == "p5":
        for key in ("p5_core_screener_pass", "p5_veto_reason", "p5_strategy_checks",
                     "risk_tags", "suggested_min_entry_score", "risk_control",
                     "postmarket_signal", "postmarket_pattern", "postmarket_score"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val not in (None, "", []):
                    clean[key] = val
        # P5 盘后/财务因子
        for key in ("financial_score", "fund_score", "pe_ttm", "pb", "roe",
                     "revenue_growth", "net_profit_growth", "eps"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val
        # P5 长周期指标
        for key in ("weekly_ma5_ratio", "weekly_ma10_ratio", "weekly_ma20_ratio",
                     "monthly_trend", "long_term_ma_ratio"):
            if key in stock_dict and key not in clean:
                val = stock_dict[key]
                if val is not None:
                    clean[key] = val
        # P5 财务快照补充
        if "financial" not in clean:
            fin: Dict[str, Any] = {}
            for fk in ("pe_ttm", "pb", "roe", "revenue_growth", "net_profit_growth", "eps"):
                fv = stock_dict.get(fk)
                if fv is not None:
                    fin[fk] = fv
            if fin:
                clean["financial"] = fin

    # === 15. 共性兜底：stock_dict 中未被上述处理的数值字段 ===
    for key, val in stock_dict.items():
        if key in clean:
            continue  # 已有则跳过
        if isinstance(val, (dict, list, tuple)):
            continue  # 复杂结构不直接塞入
        if _is_deepseek_empty_value(val):
            continue
        if key in ("df", "hist", "raw", "dataframe", "detail", "strategies", "hits"):
            continue  # 排除大结构体
        if key.startswith("_"):
            continue  # 排除内部标记
        # 对于 P3/P4/P5 特有的未处理字段，做最后一道兜底
        if pk in ("p3", "p4", "p5") and len(clean) < 48:
            clean[key] = val


def _normalize_deepseek_financial(raw: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(raw, dict):
        for key in _DEEPSEEK_FINANCIAL_FIELDS:
            val = raw.get(key)
            if not _is_deepseek_empty_value(val):
                out[key] = val
    elif not _is_deepseek_empty_value(raw):
        out["summary_text"] = raw
    return out


def _load_financial_snapshot_for_deepseek(code_like: Any) -> Dict[str, Any]:
    """从 fact_financial_reports 读取最近财报快照，补充 DeepSeek 年报/季报字段。"""
    ts_code = str(code_like or "").strip().upper()
    if not ts_code:
        return {}
    if "." not in ts_code:
        s6 = _norm_ts_code(ts_code)
        if s6.startswith("6"):
            ts_code = f"{s6}.SH"
        elif s6.startswith(("8", "4")):
            ts_code = f"{s6}.BJ"
        else:
            ts_code = f"{s6}.SZ"
    try:
        from data.db_core import get_read_conn_singleton, table_exists

        if not table_exists("fact_financial_reports"):
            return {}
        con = get_read_conn_singleton(max_wait_sec=5.0)
        if con is None:
            return {}
        q = """
            SELECT *
            FROM fact_financial_reports
            WHERE ts_code = ?
            ORDER BY COALESCE(ann_date, '') DESC, COALESCE(end_date, '') DESC
            LIMIT 2
        """
        df = con.execute(q, [ts_code]).fetchdf()
    except Exception as e:
        logger.debug("读取财报增强表失败 ts_code=%s: %s", ts_code, e)
        return {}
    if df is None or df.empty:
        return {}
    rows = df.to_dict(orient="records")
    latest = rows[0] if rows else {}
    annual = next((r for r in rows if str(r.get("report_type") or "") == "annual"), latest)
    quarter = latest
    fin = {k: latest.get(k) for k in _DEEPSEEK_FINANCIAL_FIELDS if not _is_deepseek_empty_value(latest.get(k))}
    return {
        "deepseek_fin_text": latest.get("summary_text") or "",
        "annual_report": annual.get("summary_text") or latest.get("summary_text") or "",
        "quarter_report": quarter.get("summary_text") or latest.get("summary_text") or "",
        "financial": fin,
    }


def _build_deepseek_stock_payload(pool_key: str, stock_dict: Dict[str, Any]) -> str:
    """按白名单整理 DeepSeek 入参；保留 extra 兜底但避免无边界塞入脏字段。"""
    if not isinstance(stock_dict, dict):
        stock_dict = {}
    pk = str(pool_key or "").strip().lower()
    clean: Dict[str, Any] = {"pool_key": pk}
    used_keys = set()

    for canonical, aliases in _DEEPSEEK_FIELD_ALIASES.items():
        val = _get_first_deepseek_value(stock_dict, aliases)
        if not _is_deepseek_empty_value(val):
            clean[canonical] = _normalize_deepseek_financial(val) if canonical == "financial" else val
            for alias in aliases:
                if alias in stock_dict:
                    used_keys.add(alias)

    if "code" in clean:
        clean["code"] = _norm_ts_code(clean.get("code")) or clean.get("code")
    if "name" in clean:
        clean["name"] = normalize_stock_display_name(clean.get("name"))

    fin_snap = _load_financial_snapshot_for_deepseek(stock_dict.get("ts_code") or clean.get("code"))
    for k, v in fin_snap.items():
        if k == "financial":
            cur_fin = clean.get("financial") if isinstance(clean.get("financial"), dict) else {}
            if not isinstance(cur_fin, dict):
                cur_fin = {}
            if isinstance(v, dict):
                for fk, fv in v.items():
                    if _is_deepseek_empty_value(cur_fin.get(fk)) and not _is_deepseek_empty_value(fv):
                        cur_fin[fk] = fv
            if cur_fin:
                clean["financial"] = cur_fin
        elif _is_deepseek_empty_value(clean.get(k)) and not _is_deepseek_empty_value(v):
            clean[k] = v

    _enrich_deepseek_derived_fields(pk, clean, stock_dict)

    financial = clean.get("financial")
    if isinstance(financial, dict):
        for key in _DEEPSEEK_FINANCIAL_FIELDS:
            val = stock_dict.get(key)
            if not _is_deepseek_empty_value(val) and key not in financial:
                financial[key] = val
    elif any(k in stock_dict for k in _DEEPSEEK_FINANCIAL_FIELDS):
        fin2: Dict[str, Any] = {}
        for key in _DEEPSEEK_FINANCIAL_FIELDS:
            val = stock_dict.get(key)
            if not _is_deepseek_empty_value(val):
                fin2[key] = val
        if fin2:
            clean["financial"] = fin2

    extra: Dict[str, Any] = {}
    for key, val in stock_dict.items():
        if key in used_keys or key in _DEEPSEEK_FINANCIAL_FIELDS:
            continue
        if key in ("df", "hist", "raw", "dataframe") or _is_deepseek_empty_value(val):
            continue
        if isinstance(val, (dict, list, tuple)):
            continue
        extra[str(key)] = val
        if len(extra) >= 24:
            break
    missing_core = [
        k for k in _DEEPSEEK_FIELD_ALIASES.keys()
        if k not in clean and k not in {"financial", "annual_report", "quarter_report"}
    ]
    clean["missing_fields"] = missing_core[:40]
    clean["extra"] = extra

    payload = {
        "pool_key": pk,
        "pool_focus": _DEEPSEEK_POOL_FOCUS.get(pk, "关注当日价格与成交量的基本数据特征。"),
        "pool_semantics": _build_deepseek_pool_semantics(pk),
        "tactic_context": _build_deepseek_tactic_context(clean, stock_dict),
        "stock": clean,
    }
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=False, default=str)
    except Exception:
        safe_payload = {
            "pool_key": pk,
            "pool_focus": _DEEPSEEK_POOL_FOCUS.get(pk, "关注当日价格与成交量的基本数据特征。"),
            "stock": {str(k): str(v) for k, v in clean.items()},
        }
        return json.dumps(safe_payload, ensure_ascii=False, sort_keys=False)


def _append_deepseek_advice(content: str, advice: str) -> str:
    block = f"\n\n【DeepSeek分析建议】\n{_trim_deepseek_advice(advice)}"
    merged = f"{str(content or '').rstrip()}{block}"
    if len(merged) > _MAX_MARKDOWN_CONTENT_LEN:
        merged = merged[: _MAX_MARKDOWN_CONTENT_LEN - 20].rstrip() + "\n…(已截断)"
    return merged


def _deepseek_ts_code_full(stock_dict: Dict[str, Any]) -> str:
    raw = stock_dict.get("ts_code") or stock_dict.get("代码") or stock_dict.get("code")
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if "." in s:
        return s
    s6 = _norm_ts_code(s)
    if s6.startswith("6"):
        return f"{s6}.SH"
    if s6.startswith(("8", "4")):
        return f"{s6}.BJ"
    return f"{s6}.SZ"


def _classify_deepseek_error(err: Any, status_code: Optional[int] = None, body: str = "") -> str:
    """把 DeepSeek 失败原因压成醒目的中文分类，便于日志/AI 日志快速定位。"""
    msg = f"{str(err or '')} {str(body or '')}".lower()
    code = int(status_code or 0)
    if code in (401, 403) or "unauthorized" in msg or "invalid api key" in msg or "authentication" in msg:
        return "鉴权失败/API Key无效"
    if code == 400 or "bad request" in msg or "invalid" in msg or "unknown parameter" in msg:
        return "请求参数错误"
    if code == 404 or "model" in msg and "not" in msg:
        return "模型或接口地址错误"
    if code == 429 or "rate limit" in msg or "quota" in msg or "insufficient" in msg:
        return "限速/额度不足"
    if 500 <= code < 600:
        return "DeepSeek服务端异常"
    if "timeout" in msg or "timed out" in msg or "read timed" in msg:
        return "请求超时"
    if "connection" in msg or "dns" in msg or "proxy" in msg or "ssl" in msg:
        return "网络/代理/证书异常"
    if "choices" in msg:
        return "返回结构异常"
    return "未知异常"


def _log_deepseek_analysis(
    *,
    pool_key: str,
    stock_dict: Dict[str, Any],
    stock_info: str,
    advice: str,
    model: str,
    latency_ms: float,
    success: bool,
    error_msg: str = "",
    error_type: str = "",
) -> None:
    """AI 分析日志独立落库，失败只记 debug，绝不影响企微推送。"""
    try:
        from data.db_core import ensure_v26_tables, save_df_to_sql
        import pandas as pd

        ensure_v26_tables()
        ts_code = _deepseek_ts_code_full(stock_dict)
        trade_date = str(stock_dict.get("trade_date") or stock_dict.get("交易日期") or _bj_now_str("%Y-%m-%d"))[:10]
        prompt_hash = hashlib.sha256(str(stock_info or "").encode("utf-8", errors="ignore")).hexdigest()[:24]
        row = {
            "id": uuid.uuid4().hex,
            "ts_code": ts_code,
            "trade_date": trade_date,
            "pool_key": str(pool_key or "").lower(),
            "prompt_hash": prompt_hash,
            "input_json": str(stock_info or "")[:12000],
            "advice": _trim_deepseek_advice(advice),
            "model": str(model or ""),
            "latency_ms": float(latency_ms or 0.0),
            "success": bool(success),
            "error_msg": str((f"[{error_type}] " if error_type else "") + str(error_msg or ""))[:1000],
            "created_at": _bj_now_str("%Y-%m-%d %H:%M:%S"),
        }
        save_df_to_sql(pd.DataFrame([row]), "fact_ai_analysis_log")
    except Exception as e:
        logger.debug("DeepSeek 分析日志落库失败(不影响推送): %s", e)


def _maybe_get_deepseek_advice(pool_key: str, stock_dict: Dict[str, Any]) -> str:
    """仅对 P2/P3/P4/P5 生成 DeepSeek 建议；失败时返回兜底文案。"""
    pk = str(pool_key or "").strip().lower()
    if pk not in {"p2", "p3", "p4", "p5"}:
        return ""
    try:
        from core.config_manager import get_deepseek_analysis_config

        cfg = get_deepseek_analysis_config()
    except Exception as e:
        logger.debug("DeepSeek 配置读取失败: %s", e)
        return _DEEPSEEK_FALLBACK_ADVICE
    if not cfg.get("enabled"):
        return ""
    api_key = str(cfg.get("api_key") or "").strip()
    base_url = str(cfg.get("base_url") or "").strip()
    model = str(cfg.get("model") or "").strip()
    thinking_enabled = bool(cfg.get("thinking_enabled", True))
    reasoning_effort = str(cfg.get("reasoning_effort") or "high").strip().lower()
    timeout_seconds = float(cfg.get("timeout_seconds") or 45.0)
    max_tokens = int(cfg.get("max_tokens") or 8000)


    if not api_key or not base_url or not model:
        logger.debug("DeepSeek 配置不完整，跳过分析 | pool=%s", pk)
        return _DEEPSEEK_FALLBACK_ADVICE
    try:
        import requests
    except ImportError:
        logger.warning("未安装 requests，无法调用 DeepSeek")
        return _DEEPSEEK_FALLBACK_ADVICE

    stock_info = _build_deepseek_stock_payload(pk, stock_dict)
    pool_focus = _DEEPSEEK_POOL_FOCUS.get(pk, "关注当日价格与成交量的基本数据特征。")

    def _build_payload(*, enable_thinking: bool, token_budget: int, concise_retry: bool = False) -> Dict[str, Any]:
        user_prompt = (
            f"当前池子：{pk.upper()}\n"
            f"分析侧重点：{pool_focus}\n"
            "你将看到池子说明与战法知识库。请仅基于以下结构化数据提取关键信息，"
            "严格区分【已确认事实】与【基于经验的推测】，不输出任何买卖建议。"
            "请严格按以下三行格式返回，每行不超过60字，不输出其他内容：\n"
            "交易建议：[根据量价与基本面给出简洁建议，如：可关注/观望/谨慎]\n"
            "当前优势：[列出1-2个核心正面数据，不超过60字]\n"
            "当前不足：[列出1-2个核心风险点，不超过60字]"
        )
        if concise_retry:
            user_prompt += "本次是最终结论重试，请只输出上述三行格式，每行不超过60字，不要展开思考过程，不输出任何买卖建议。"
        else:
            user_prompt += "基于具体数值进行描述，不附加买卖建议。"
        user_prompt += f"\n股票信息如下：\n{stock_info}"
        payload_local: Dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": _DEEPSEEK_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": int(token_budget),
            "stream": False,
        }
        if enable_thinking:
            payload_local["reasoning_effort"] = reasoning_effort
            payload_local["thinking"] = {"type": "enabled"}
        return payload_local

    def _do_request(payload_local: Dict[str, Any], tag: str) -> Dict[str, Any]:
        started_local = time.perf_counter()
        resp = requests.post(
            base_url,
            json=payload_local,
            timeout=(min(5.0, timeout_seconds), timeout_seconds + 10.0),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json; charset=utf-8",
            },
        )
        elapsed_ms = (time.perf_counter() - started_local) * 1000.0
        if not (200 <= int(resp.status_code) < 300):
            body = resp.text[:800] if getattr(resp, "text", None) else ""
            et = _classify_deepseek_error(f"HTTP {resp.status_code}", resp.status_code, body)
            raise RuntimeError(f"[{tag}] {et} | HTTP {resp.status_code} | {body}")
        data = resp.json() if resp.content else {}
        choices = data.get("choices") if isinstance(data, dict) else None
        if not (isinstance(choices, list) and choices):
            raise RuntimeError(f"[{tag}] DeepSeek 返回 choices 为空")
        first = choices[0] if isinstance(choices[0], dict) else {}
        msg = first.get("message") if isinstance(first, dict) else None
        content = msg.get("content") if isinstance(msg, dict) else ""
        finish_reason = str(first.get("finish_reason") or "").strip().lower()
        return {
            "content": str(content or ""),
            "finish_reason": finish_reason,
            "elapsed_ms": elapsed_ms,
            "raw": data,
        }

    code = _norm_ts_code(stock_dict.get("代码") if isinstance(stock_dict, dict) else None)
    try:
        first_try = _do_request(_build_payload(enable_thinking=thinking_enabled, token_budget=max(max_tokens, 220), concise_retry=False), "thinking-pass")
        advice = _trim_deepseek_advice(first_try["content"])
        if not (first_try["finish_reason"] == "length" and len(str(first_try["content"] or "").strip()) < 16):
            _log_deepseek_analysis(
                pool_key=pk,
                stock_dict=stock_dict,
                stock_info=stock_info,
                advice=advice,
                model=model,
                latency_ms=float(first_try["elapsed_ms"]),
                success=True,
                error_type="",
            )
            return advice

        logger.warning(
            "【DeepSeek重试】type=返回截断 pool=%s code=%s model=%s first_elapsed_ms=%.0f finish_reason=%s content=%r",
            pk,
            code,
            model,
            float(first_try["elapsed_ms"]),
            first_try["finish_reason"],
            str(first_try["content"] or "")[:120],
        )

        second_try = _do_request(_build_payload(enable_thinking=False, token_budget=max(max_tokens * 2, 2000), concise_retry=True), "final-answer-retry")
        advice2 = _trim_deepseek_advice(second_try["content"])
        if advice2 and advice2 != _DEEPSEEK_FALLBACK_ADVICE:
            _log_deepseek_analysis(
                pool_key=pk,
                stock_dict=stock_dict,
                stock_info=stock_info,
                advice=advice2,
                model=f"{model}#retry_no_thinking",
                latency_ms=float(first_try["elapsed_ms"]) + float(second_try["elapsed_ms"]),
                success=True,
                error_type="",
            )
            return advice2

        err_msg = (
            f"DeepSeek 二次重试后仍未得到有效结论 | first_finish={first_try['finish_reason']} "
            f"first_content={first_try['content']!r} second_finish={second_try['finish_reason']} second_content={second_try['content']!r}"
        )
        err_type = "返回截断"
    except Exception as e:
        err_msg = str(e)
        err_type = _classify_deepseek_error(e)
        logger.error(
            "【DeepSeek失败】type=%s pool=%s code=%s model=%s base_url=%s err=%s",
            err_type,
            pk,
            code,
            model,
            base_url,
            err_msg[:800],
        )

    _log_deepseek_analysis(
        pool_key=pk,
        stock_dict=stock_dict,
        stock_info=stock_info,
        advice=_DEEPSEEK_FALLBACK_ADVICE,
        model=model,
        latency_ms=0.0,
        success=False,
        error_msg=err_msg,
        error_type=err_type,
    )
    return _DEEPSEEK_FALLBACK_ADVICE


def format_wechat_markdown(pool_key: str, stock_dict: Dict[str, Any]) -> str:
    """
    企业微信 markdown 正文（msgtype=markdown 的 content 字段）。
    颜色：涨幅>0 用 warning（橙红），否则 info（绿）；辅助说明用 comment（灰）。
    战法行整体加粗展示；观察池带【缩量期备选】时追加醒目提示行。

    【防爆】stock_dict 非 dict、缺键、NaN、嵌套异常时一律降级为占位符，不向外抛导致整条推送作废。
    """
    try:
        if not isinstance(stock_dict, dict):
            stock_dict = {}

        pool_name = str(pool_key or "").strip().upper()
        pk = pool_name.lower()
        emoji, zone_name = _POOL_ALERT_TITLES.get(pk, ("🚨", pool_name or "未知战区"))
        tier = str(stock_dict.get("pool_tier", "") or "")
        pool_source = str(stock_dict.get("pool_source", "") or "")
        zhanfa = str(stock_dict.get("战法", "") or "")
        version_tag = "V26.7"
        if tier == "observation" or "【缩量期备选】" in zhanfa:
            title = f"{version_tag} {emoji} 【{zone_name}·观察池】信号触发"
        elif pool_source == "直通车" or tier == "fastlane":
            title = f"{version_tag} {emoji} 【{zone_name}·直通车】信号触发"
        else:
            title = f"{version_tag} {emoji} 【{zone_name}】信号触发"

        code = _esc(_norm_ts_code(stock_dict.get("代码")) or stock_dict.get("代码", "--"))
        name_raw = normalize_stock_display_name(stock_dict.get("名称", "--"))
        code_plain = _norm_ts_code(stock_dict.get("代码")) or str(stock_dict.get("代码", "") or "").strip()
        if not code_plain:
            code_plain = "--"
        name_combined = f"{name_raw if name_raw is not None else '--'}"
        name = _esc(name_combined)

        price_raw = stock_dict.get("现价", "--")
        price = _esc(price_raw if price_raw is not None else "--")

        pct_raw = stock_dict.get("涨幅", "--")
        pct_f = _parse_pct_to_float(pct_raw)
        # 展示串：优先保留原始文案；若为脏 NaN 则显示 --
        try:
            if isinstance(pct_raw, float) and (math.isnan(pct_raw) or math.isinf(pct_raw)):
                pct_disp = "--"
            else:
                pct_disp = str(pct_raw).strip() if pct_raw is not None else "--"
        except Exception:
            pct_disp = "--"
        if not pct_disp or pct_disp.lower() == "nan":
            pct_disp = "--"
        pct_disp_esc = _esc(pct_disp)

        if pct_f > 0:
            pct_line = f'<font color="warning">{pct_disp_esc}</font>'
        else:
            pct_line = f'<font color="info">{pct_disp_esc}</font>'

        score_show = _safe_score_display(stock_dict)
        score_line = _esc(score_show)
        score_num = _finite_float_or_none(stock_dict.get("综合分", None))
        if score_num is None:
            score_num = _finite_float_or_none(stock_dict.get("burst_score", stock_dict.get("score", None)))
        if score_num is None:
            score_num = 0.0
        if score_num >= 85.0:
            template_label = "可以继续看"
        elif score_num >= 40.0:
            template_label = "先观察"
        else:
            template_label = "暂不追高"
        if pool_name == "P1":
            if score_num >= 80.0:
                template_label = "可以继续看"
            elif score_num >= 65.0:
                template_label = "先观察"
            else:
                template_label = "暂不追高"

        hot_sector_bonus = _finite_float_or_none(stock_dict.get("hot_sector_bonus", stock_dict.get("热门板块加成", None)))
        hot_sector_text = None
        if hot_sector_bonus is not None and abs(hot_sector_bonus) > 1e-9:
            hot_sector_text = f"{hot_sector_bonus:+.2f}".rstrip("0").rstrip(".")

        tactic_raw = stock_dict.get("战法", "--")
        tactic = _esc(tactic_raw if tactic_raw is not None else "--")
        buy_hint_raw = (
            stock_dict.get("买入提示")
            or stock_dict.get("wechat_hint")
            or stock_dict.get("buy_hint")
            or ""
        )
        buy_hint_text = str(buy_hint_raw).strip() if buy_hint_raw else ""
        buy_hint = _esc(buy_hint_text) if buy_hint_text else ""
        exec_tier_raw = str(stock_dict.get("执行层级", "") or "").strip()
        position_raw = str(stock_dict.get("建议仓位", "") or "").strip()
        risk_tags_raw = str(stock_dict.get("风险标签", "") or "").strip()
        exec_tier = _esc(exec_tier_raw) if exec_tier_raw and exec_tier_raw != "--" else ""
        position_hint = _esc(position_raw) if position_raw and position_raw != "--" else ""
        risk_map = {
            "竞价放量确认": "走势稳，承接不错，可以继续看",
            "贴线温和放量(稳健)": "走势稳，承接不错，可以继续看",
            "尾盘增量确认": "走势稳，承接不错，可以继续看",
            "板块共振(主线同步)": "走势稳，承接不错，可以继续看",
            "⚡[强爆发]": "走势稳，承接不错，可以继续看",
            "主线共振": "走势稳，承接不错，可以继续看",
            "承接不错": "走势稳，承接不错，可以继续看",
        }
        risk_tags = _esc(risk_map.get(risk_tags_raw, "走势稳，承接不错，可以继续看")) if risk_tags_raw and risk_tags_raw != "--" else ""
        entry_reason_raw = (
            stock_dict.get("入池理由")
            or stock_dict.get("买入原因")
            or stock_dict.get("why_selected")
            or stock_dict.get("selection_reason")
            or ""
        )
        if not entry_reason_raw:
            detail = stock_dict.get("detail") if isinstance(stock_dict.get("detail"), dict) else {}
            if isinstance(detail, dict):
                entry_reason_raw = (
                    detail.get("mainline_reason")
                    or detail.get("p2_screener", {}).get("mainline_reason") if isinstance(detail.get("p2_screener"), dict) else ""
                )
        entry_reason = _esc(str(entry_reason_raw).strip()) if entry_reason_raw else ""

        now_str = _bj_now_str("%Y-%m-%d %H:%M:%S")
        header_time = f'<font color="comment">{_esc(now_str)}</font>'
        ts_short = _esc(_bj_now_str("%H:%M:%S"))
        title = f"{title} [{ts_short}]"
        lines = [
            title,
            "",
            header_time,
            "",
            f'- <font color="comment">代码</font>：<font color="comment">{code}</font>',
            f'- <font color="comment">名称</font>：{name}',
            f'- <font color="comment">现价</font>：<font color="comment">{price}</font>',
            f'- <font color="comment">涨幅</font>：<font color="comment">{pct_line}</font>',
            f'- <font color="comment">综合分</font>：<font color="comment">{score_line}</font>',
        ]
        # 【V26.7 增强】P3 盘中告警补充四项关键数据
        if pool_name == "P3":
            _vwap = _finite_float_or_none(stock_dict.get("vwap"))
            _vwap_dev = _finite_float_or_none(stock_dict.get("vwap_dev_pct"))
            if _vwap is not None and _vwap_dev is not None:
                _vwap_color = "warning" if _vwap_dev > 0 else "info"
                _vwap_dev_str = f"{_vwap_dev:+.2f}%"
                lines.append(f'- <font color="comment">VWAP均价</font>：<font color="comment">{_vwap:.3f}</font>  <font color="comment">偏离</font>：<font color="{_vwap_color}">{_vwap_dev_str}</font>')
            _ma_fields = [("ma5", "MA5"), ("ma10", "MA10"), ("ma20", "MA20"), ("ma60", "MA60")]
            _ma_parts = []
            for _k, _label in _ma_fields:
                _ma_val = _finite_float_or_none(stock_dict.get(_k))
                _ma_dev = _finite_float_or_none(stock_dict.get(f"{_k}_dev_pct"))
                if _ma_val is not None and _ma_dev is not None:
                    _mc = "warning" if _ma_dev > 0 else "info"
                    _ma_parts.append(f'{_label}={_ma_val:.2f}({_ma_dev:+.1f}%)')
            if _ma_parts:
                lines.append(f'- <font color="comment">均线位置</font>：<font color="comment">{"  ".join(_ma_parts)}</font>')
            _net_main = _finite_float_or_none(stock_dict.get("net_main_amount"))
            if _net_main is not None:
                _nm_color = "warning" if _net_main > 0 else "info"
                _nm_arrow = "▲" if _net_main > 0 else "▼"
                lines.append(f'- <font color="comment">主力净额(万)</font>：<font color="{_nm_color}">{_nm_arrow}{abs(_net_main):.1f}万</font>  <font color="comment">(昨)</font>')
            _macd_bar = _finite_float_or_none(stock_dict.get("macd_bar"))
            _macd_dif = _finite_float_or_none(stock_dict.get("macd_dif"))
            _macd_dea = _finite_float_or_none(stock_dict.get("macd_dea"))
            if _macd_bar is not None and _macd_dif is not None and _macd_dea is not None:
                _mc_state = "红柱" if _macd_bar > 0 else "绿柱"
                _mc_color = "warning" if _macd_bar > 0 else "info"
                lines.append(f'- <font color="comment">MACD</font>：<font color="{_mc_color}">{_mc_state}({_macd_bar:.4f})</font>  <font color="comment">DIF={_macd_dif:.4f} DEA={_macd_dea:.4f}</font>')
        if entry_reason:
            lines.append(f'- <font color="comment">入池理由</font>：<font color="warning">{entry_reason}</font>')
        if hot_sector_text:
            lines.append(f'- <font color="comment">热门板块加成</font>：<font color="warning">{hot_sector_text}</font>')
        lines.append(f'- <font color="comment">命中战法</font>：**{tactic}**')
        if pool_source == "直通车" or tier == "fastlane":
            lines.append('- <font color="warning">来源</font>：**直通车**')
        elif pool_name in {"P3", "P4"}:
            lines.append('- <font color="info">来源</font>：**底仓池**')
        if exec_tier:
            tier_text = exec_tier
            tier_map = {
                "A": "可以重点看",
                "B": "可以小仓跟踪",
                "C": "先观察为主",
            }
            tier_text = tier_map.get(exec_tier.upper(), exec_tier)
            lines.append(f'- <font color="comment">执行建议</font>：<font color="warning">{tier_text}</font>')
        if position_hint:
            pos_text = position_hint
            pos_map = {
                "主仓候选: 20%-30%（分批）": "可以按主仓思路分批看",
                "试错仓位: 8%-15%": "先小仓试一下",
                "观察仓位: 0%-8%": "先观察，不急着上",
                "主仓候选: 20%-30%": "可以按主仓思路分批看",
                "试错仓位: 8%-15%（轻仓）": "先小仓试一下",
                "观察仓位: 0%-8%（轻仓）": "先观察，不急着上",
            }
            pos_text = pos_map.get(position_raw, position_hint)
            lines.append(f'- <font color="comment">建议仓位</font>：<font color="warning">{pos_text}</font>')
        if risk_tags:
            lines.append(f'- <font color="comment">风险标签</font>：<font color="warning">{risk_tags}</font>')
        if buy_hint:
            buy_text = buy_hint_text
            buy_map = {
                "次日先看能否站稳关键位，确认后再考虑加仓。": "先看能不能站稳关键位，再决定要不要跟",
                "均线发散确认后再跟，先看承接是否持续。": "确认站稳后再跟，先看承接是否持续",
                "尾盘确认后可跟踪，先看次日能否站稳关键位。": "先看次日能不能站稳关键位，再决定要不要跟",
                "尾盘偏强但需防兑现，建议次日确认站稳再介入。": "先确认站稳，再考虑介入",
                "主线真龙可次日轻仓跟随，回踩VWAP或5日线确认后再加。": "先轻仓跟随，等回踩确认后再加",
            }
            buy_text = buy_map.get(buy_hint_text, buy_hint_text)
            lines.append(f'- <font color="comment">买入提示</font>：<font color="warning">{_esc(buy_text)}</font>')
        if tier == "observation" or "【缩量期备选】" in zhanfa:
            lines.append("")
            lines.append(
                '> <font color="warning">**【缩量观察池】** 非主池共识信号，高风险备选，请人工复核！</font>'
            )
        lines.append("")
        if buy_hint_text:
            buy_text = buy_hint_text
            buy_map = {
                "次日先看能否站稳关键位，确认后再考虑加仓。": "先看能不能站稳关键位，再决定要不要跟",
                "均线发散确认后再跟，先看承接是否持续。": "确认站稳后再跟，先看承接是否持续",
                "尾盘确认后可跟踪，先看次日能否站稳关键位。": "先看次日能不能站稳关键位，再决定要不要跟",
                "尾盘偏强但需防兑现，建议次日确认站稳再介入。": "先确认站稳，再考虑介入",
                "主线真龙可次日轻仓跟随，回踩VWAP或5日线确认后再加。": "先轻仓跟随，等回踩确认后再加",
            }
            buy_text = buy_map.get(buy_hint_text, buy_hint_text)
            lines.append(
                f'> <font color="warning">**买入提示：{_esc(buy_text)}**</font>'
            )
        lines.append(
            '> <font color="comment">温馨提示：炒股有风险，决策需谨慎；本消息仅作策略信号参考，不构成投资建议。</font>'
        )

        body = "\n".join(lines)
        if len(body) > _MAX_MARKDOWN_CONTENT_LEN:
            body = body[: _MAX_MARKDOWN_CONTENT_LEN - 20] + "\n…(已截断)"
        return body
    except Exception as e:
        # 最后防线：仍返回一条可读的极简消息，避免调用方完全无内容可发
        logger.warning("format_wechat_markdown 降级为极简模板: %s", e, exc_info=True)
        code_f = _esc(_norm_ts_code(stock_dict.get("代码")) if isinstance(stock_dict, dict) else "--")
        return "\n".join(
            [
                "🚨 【信号】格式化降级",
                f"- 代码：{code_f}",
                "- 详情：字段异常已兜底，请至终端查看日志",
            ]
        )


class WechatNotificationGateway:
    """
    企微 Webhook 推送网关：有界线程池投递 _send_markdown_sync + 股票信号内存防刷去重 + requests 超时与重试；
    系统运维告警去重为 ``data/runtime/alert_dedup_cache.json`` 文件账本（跨进程）。
    """

    def __init__(self, webhook_url: str) -> None:
        self._url = (webhook_url or "").strip()
        self._pushed_records: Dict[str, float] = {}
        self._pushed_signal_codes_once: Dict[str, float] = {}
        self._pushed_minute_records: Dict[str, float] = {}
        self._records_lock = threading.RLock()
        # 当前防刷字典对应的北京时间日历键；跨日首次操作时整表 clear（凌晨无推送时也会在次日首次推送前清空）
        self._spam_calendar_key: str = ""
        # 连续失败熔断：避免企业微信网关抖动时在每次信号触发点重复打满重试线程
        self._failure_streak = 0
        self._circuit_open_until = 0.0
        self._circuit_lock = threading.RLock()

    def _reset_spam_if_new_calendar_day_bj(self) -> None:
        """
        【慢性中毒修复】跨自然日（北京时间）静默清空防刷字典。
        说明：滑动 30 分钟 prune 已限制「键数量级」，但跨日清空可释放引用、统一日界心智，并防止极端时钟回拨/长期进程的边角累积。
        """
        today_k = _bj_calendar_key()
        with self._records_lock:
            if self._spam_calendar_key != today_k:
                if self._pushed_records or self._pushed_signal_codes_once:
                    logger.info(
                        "企微防刷 _pushed_records 跨日清空 | 旧日键=%s → 新日键=%s | 条数=%s",
                        self._spam_calendar_key or "(init)",
                        today_k,
                        len(self._pushed_records),
                    )
                self._pushed_records.clear()
                self._pushed_signal_codes_once.clear()
                self._pushed_minute_records.clear()
                # 系统告警去重已迁至 data/runtime/alert_dedup_cache.json（跨进程），不在此跨日清空。
                self._spam_calendar_key = today_k

    def _prune_old_records(self, now: float) -> None:
        """剔除超过 30 分钟窗口的记录。"""
        cutoff = now - _SPAM_WINDOW_SEC
        with self._records_lock:
            self._pushed_records = {k: v for k, v in self._pushed_records.items() if v > cutoff}

    def _shrink_if_oversized(self) -> None:
        """硬顶：字典异常膨胀时只保留时间戳最新的若干条，防止 OOM。"""
        with self._records_lock:
            n = len(self._pushed_records)
            if n <= _SPAM_RECORDS_HARD_CAP:
                return
            sorted_items = sorted(self._pushed_records.items(), key=lambda kv: kv[1], reverse=True)
            self._pushed_records = dict(sorted_items[: _SPAM_RECORDS_HARD_CAP // 2])
            logger.warning(
                "企微防刷字典触顶收缩 | 原条数=%s → 保留半帽=%s",
                n,
                len(self._pushed_records),
            )

    def clear_push_cache(self) -> None:
        """
        硬性清空 _pushed_records，释放内存引用；用于每日早安例行或运维排障。
        与跨日 _reset_spam 互补：后者依赖「有推送行为才触发」，本方法保证定时落闸。
        同时删除系统告警文件去重账本 alert_dedup_cache.json（与旧版「清空系统告警内存键」语义对齐）。
        """
        with self._records_lock:
            n = len(self._pushed_records)
            n_codes = len(self._pushed_signal_codes_once)
            n_minutes = len(self._pushed_minute_records)
            self._pushed_records.clear()
            self._pushed_signal_codes_once.clear()
            self._pushed_minute_records.clear()
        removed_json = False
        try:
            ap = _path_alert_dedup_cache_file()
            if os.path.isfile(ap):
                os.unlink(ap)
                removed_json = True
        except Exception as e:
            logger.warning("企微网关 clear_push_cache 删除系统告警去重账本失败(忽略): %s", e)
        logger.info(
            "企微网关 clear_push_cache | 战法防刷键=%s | 当日代码防刷键=%s | 当日分钟防刷键=%s | 系统告警账本已删=%s",
            n,
            n_codes,
            n_minutes,
            removed_json,
        )

    @staticmethod
    def _is_signal_pool_slot(pool_key: str) -> bool:
        """仅对 P3/P4（含 *_obs）启用「当日同代码仅推一次」硬防重。"""
        pk = str(pool_key or "").strip().lower()
        return pk.startswith("p3") or pk.startswith("p4")

    @staticmethod
    def _signal_pool_bucket(pool_key: str) -> str:
        """
        将战区归一为池维度桶：
        - p3 / p3_obs -> p3
        - p4 / p4_obs -> p4
        - 其它池原样返回
        """
        pk = str(pool_key or "").strip().lower()
        if pk.startswith("p3"):
            return "p3"
        if pk.startswith("p4"):
            return "p4"
        return pk

    def send_heartbeat(self, status_dict: Dict[str, Any]) -> None:
        """
        拼装「早安 / 系统唤醒」Markdown；经 push_markdown_async 提交线程池，**不阻塞**调度主线程。

        status_dict 约定键（可扩展）：
        - wechat_push_label / daemon_cruise_label：展示用「开启」或「关闭」
        - wechat_push_enabled / daemon_auto_cruise_enabled：若无 label 则由 bool 推导中文
        """
        if not isinstance(status_dict, dict):
            status_dict = {}
        wl = status_dict.get("wechat_push_label")
        cl = status_dict.get("daemon_cruise_label")
        if wl is None:
            wl = "开启" if status_dict.get("wechat_push_enabled", True) else "关闭"
        if cl is None:
            cl = "开启" if status_dict.get("daemon_auto_cruise_enabled", True) else "关闭"
        wls = str(wl).strip() or "--"
        cls = str(cl).strip() or "--"
        body = "\n".join(
            [
                f"🟢 【小杰AI选股系统 Pro V26.7】早安！系统已唤醒 [{_bj_now_str('%H:%M:%S')}]",
                "今日为交易日，各项数据已就绪。",
                f"🎛️ 企微推送：{wls}",
                f"🤖 自动巡航：{cls}",
            ]
        )
        extra = status_dict.get("extra_lines")
        if isinstance(extra, list) and extra:
            body = body + "\n" + "\n".join(str(x) for x in extra if str(x).strip())
        if len(body) > _MAX_MARKDOWN_CONTENT_LEN:
            body = body[: _MAX_MARKDOWN_CONTENT_LEN - 20] + "\n…(已截断)"
        self.push_markdown_async(body)

    def _reserve_push_slot(self, pool_key: str, ts_code: str, strategy_raw: Any) -> bool:
        """
        P3/P4：同池同代码当日仅推一次（跨战法合并），无论命中多少个战法均只推送第一条。
        P3/P4 不同池之间不共享去重，同一只股票可分别出现在P3和P4各一次。
        P2/P5：同代码+同战法30分钟冷却窗口（允许不同战法分别推送）。
        跨进程文件级去重由 _reserve_signal_daily_once_file 保证（用于P3/P4）。
        """
        self._reset_spam_if_new_calendar_day_bj()
        pk = str(pool_key).strip().lower()
        sk = _strategy_dedup_segment(strategy_raw)
        bucket = self._signal_pool_bucket(pk)
        is_signal_pool = self._is_signal_pool_slot(pk)
        ts6 = _norm_ts_code(ts_code)
        minute_key = f"{bucket}:{ts6}:{_bj_now_str('%Y%m%d%H%M')}"
        key = f"{bucket}_{ts6}" if is_signal_pool else f"{pk}_{ts6}_{sk}"
        once_code_key = f"sig:{bucket}:{ts6}"
        now = time.time()
        self._prune_old_records(now)
        self._shrink_if_oversized()

        if is_signal_pool:
            if self._reserve_signal_daily_once_file(pk, ts6, sk):
                with self._records_lock:
                    self._pushed_signal_codes_once[once_code_key] = now
                return True
            logger.debug("企微推送跳过(跨进程当日同池已推送): code=%s pool=%s bucket=%s", ts6, pk, bucket)
            return False

        with self._records_lock:
            if minute_key in self._pushed_minute_records:
                logger.debug(
                    "企微推送跳过(同一分钟重复触发): code=%s pool=%s bucket=%s",
                    ts6,
                    pk,
                    bucket,
                )
                return False
            last = self._pushed_records.get(key, 0.0)
            if now - last < _SPAM_WINDOW_SEC:
                logger.debug(
                    "%s %s 处于冷却期，已静默",
                    ts6,
                    _strategy_log_fragment(strategy_raw),
                )
                return False
            self._pushed_records[key] = now
            self._pushed_minute_records[minute_key] = now
            return True

    def _reserve_signal_daily_once_file(self, pool_key: str, ts6: str, strategy_raw: Any) -> bool:
        """P3/P4 股票信号跨进程去重：同池同代码同日仅推一次（跨战法合并）。
        同一只股票在P3或P4当日无论命中多少个战法，均只推送第一条，后面的战法命中被静默。"""
        from core.file_utils import atomic_json_update

        dedup_path = path_wechat_signal_dedup_cache_json()
        today = _bj_calendar_key()
        pk = str(pool_key).strip().lower()
        sk = _strategy_dedup_segment(strategy_raw)
        # 去重键不含战法：同池同代码同日无论命中几个战法，仅推送第一次
        slot_key = f"{today}:{pk}:{ts6}"
        now = time.time()
        reserved = {"ok": False}

        def _upd(data: Dict[str, Any]) -> None:
            cutoff_ts = now - float(7 * 24 * 3600)
            for k in list(data.keys()):
                if not isinstance(k, str):
                    continue
                try:
                    v = data.get(k) or {}
                    if not isinstance(v, dict):
                        continue
                    ts = float(v.get("ts", 0.0) or 0.0)
                    if ts < cutoff_ts:
                        del data[k]
                except Exception:
                    continue

            old = data.get(slot_key)
            if isinstance(old, dict):
                old_ts = float(old.get("ts", 0.0) or 0.0)
                # 【BugFix】原逻辑：str(old.get("date") or "") == today
                # 问题：当 date 为 None 时，str(None or "") 恒等于 ""，永远不等于 today，
                # 导致旧记录（date=None 但时间戳在今日滑动窗口内）无法被识别为「今日已推送」，
                # 防重完全失效。
                # 修复：分离 date 读取 + 时间戳双重保护：
                # 1. 若 date 字段存在且等于 today → 视为今日已推送
                # 2. 若 date 字段缺失（None）但 old_ts 在今日 0 点之后 → 同样视为今日已推送
                old_date = str(old.get("date") or "")
                if old_ts > 0:
                    if old_date == today:
                        # date 字段明确等于今日
                        reserved["ok"] = False
                        return
                    # date 字段缺失但时间戳在今日范围内（兜底防重，防止 date=None 导致永久失效）
                    import time as _time_mod
                    try:
                        from zoneinfo import ZoneInfo
                        today_start = datetime.now(ZoneInfo("Asia/Shanghai")).replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).timestamp()
                    except Exception:
                        today_start = datetime.now().replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).timestamp()
                    if old_ts >= today_start:
                        reserved["ok"] = False
                        return

            data[slot_key] = {
                "ts": now,
                "date": today,
                "pool_key": pk,
                "code": ts6,
                "strategy": _strategy_log_fragment(strategy_raw),
            }
            reserved["ok"] = True

        try:
            atomic_json_update(dedup_path, _upd, timeout=8)
        except Exception as e:
            logger.warning("企微股票信号去重账本异常，退回内存去重放行: %s", e)
            return True
        return reserved["ok"]

    def _send_markdown_sync(self, content: str) -> None:
        """
        同步发送：强制超时 + 失败重试。
        【断网盲区】每次 requests.post 使用 connect/read 双超时；异常或 5xx 时 sleep 后重试，尽量不因一秒抖动丢高分票。
        连续失败达到阈值后打开熔断，短时间内直接跳过后续发送，避免守护线程被网关抖动拖死。
        """
        if not self._url:
            logger.debug("企微 webhook 未配置，跳过发送")
            return
        if not content or not str(content).strip():
            logger.debug("企微 markdown 内容为空，跳过发送")
            return

        now = time.time()
        with self._circuit_lock:
            if now < float(self._circuit_open_until or 0.0):
                logger.warning(
                    "企微 webhook 熔断中，跳过发送（剩余 %.0fs）",
                    float(self._circuit_open_until) - now,
                )
                return

        try:
            from core.master_control import is_wechat_push_master_enabled

            if not is_wechat_push_master_enabled():
                logger.debug("企微推送已跳过：物理总控台「企微实盘推送」关闭 (master_control.json)")
                return
        except Exception as e:
            logger.debug("读取 master_control 失败，继续尝试发送: %s", e)

        try:
            import requests
        except ImportError:
            logger.error("未安装 requests，无法发送企微 webhook")
            return

        payload = {"msgtype": "markdown", "markdown": {"content": str(content)}}
        last_err: Optional[BaseException] = None

        for attempt in range(1, _WEBHOOK_MAX_ATTEMPTS + 1):
            try:
                resp = requests.post(
                    self._url,
                    json=payload,
                    timeout=_REQUEST_TIMEOUT,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                )
                code = int(resp.status_code)
                if 200 <= code < 300:
                    if attempt > 1:
                        logger.info("企微 webhook 第 %s 次尝试成功", attempt)
                    with self._circuit_lock:
                        self._failure_streak = 0
                        self._circuit_open_until = 0.0
                    return
                last_err = RuntimeError(f"HTTP {code}: {resp.text[:300]}")
                logger.warning("企微 webhook HTTP 异常 attempt=%s: %s", attempt, last_err)
            except Exception as e:
                last_err = e
                logger.warning(
                    "企微 webhook 网络异常 attempt=%s/%s: %s",
                    attempt,
                    _WEBHOOK_MAX_ATTEMPTS,
                    e,
                )
            if attempt < _WEBHOOK_MAX_ATTEMPTS:
                try:
                    time.sleep(_WEBHOOK_RETRY_SLEEP_SEC)
                except Exception:
                    pass

        with self._circuit_lock:
            self._failure_streak += 1
            if self._failure_streak >= _WEBHOOK_CIRCUIT_FAILURE_THRESHOLD:
                self._circuit_open_until = time.time() + float(_WEBHOOK_CIRCUIT_OPEN_SEC)
                logger.error(
                    "企微 webhook 连续失败 %s 次，熔断 %ss",
                    self._failure_streak,
                    _WEBHOOK_CIRCUIT_OPEN_SEC,
                )
        logger.error("企微 webhook 已达最大重试仍失败: %s", last_err)

    def _reserve_system_alert_slot(
        self,
        dedup_key: str,
        *,
        window_sec: Optional[float] = None,
    ) -> bool:
        """
        系统告警：同 dedup_key 在 window_sec 内不重复推送（与 pool+代码 防刷独立）。
        window_sec 默认 12 小时；实盘熔断等可传更长间隔；去重账本为 alert_dedup_cache.json（跨进程）。
        """
        self._reset_spam_if_new_calendar_day_bj()
        win = float(window_sec) if window_sec is not None else float(_SYSTEM_ALERT_DEDUP_DEFAULT_SEC)
        win = max(60.0, min(win, float(7 * 24 * 3600)))
        key = f"sys:{str(dedup_key)[:200]}"
        allow = _file_reserve_system_alert_slot(dedup_key, win)
        if not allow:
            logger.debug("企微系统告警跳过(文件防刷): %s (窗口=%ss)", key, int(win))
        return allow

    def _send_system_markdown_sync(self, content: str) -> None:
        """与 _send_markdown_sync 相同 HTTP 路径，但总闸为「系统告警」而非「信号推送」。"""
        if not self._url:
            logger.debug("企微 webhook 未配置，跳过系统告警")
            return
        if not content or not str(content).strip():
            return
        try:
            from core.master_control import is_wechat_system_alert_enabled

            if not is_wechat_system_alert_enabled():
                logger.debug("企微系统告警已跳过：master_control「系统温馨提示」关闭")
                return
        except Exception as e:
            logger.debug("读取 master_control 系统告警开关失败，继续尝试: %s", e)

        try:
            import requests
        except ImportError:
            logger.error("未安装 requests，无法发送企微系统告警")
            return

        payload = {"msgtype": "markdown", "markdown": {"content": str(content)}}
        last_err: Optional[BaseException] = None

        for attempt in range(1, _WEBHOOK_MAX_ATTEMPTS + 1):
            try:
                resp = requests.post(
                    self._url,
                    json=payload,
                    timeout=_REQUEST_TIMEOUT,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                )
                code = int(resp.status_code)
                if 200 <= code < 300:
                    if attempt > 1:
                        logger.info("企微系统告警 webhook 第 %s 次尝试成功", attempt)
                    return
                last_err = RuntimeError(f"HTTP {code}: {resp.text[:300]}")
                logger.warning("企微系统告警 HTTP 异常 attempt=%s: %s", attempt, last_err)
            except Exception as e:
                last_err = e
                logger.warning(
                    "企微系统告警网络异常 attempt=%s/%s: %s",
                    attempt,
                    _WEBHOOK_MAX_ATTEMPTS,
                    e,
                )
            if attempt < _WEBHOOK_MAX_ATTEMPTS:
                try:
                    time.sleep(_WEBHOOK_RETRY_SLEEP_SEC)
                except Exception:
                    pass

        logger.error("企微系统告警已达最大重试仍失败: %s", last_err)

    def push_system_alert_async(
        self,
        content: str,
        dedup_key: str,
        *,
        dedup_window_sec: Optional[float] = None,
    ) -> None:
        if not self._reserve_system_alert_slot(dedup_key, window_sec=dedup_window_sec):
            return

        def _run() -> None:
            try:
                self._send_system_markdown_sync(content)
            except Exception as e:
                logger.warning("企微系统告警异步发送兜底异常(已吞): %s", e, exc_info=True)

        try:
            _WECHAT_WEBHOOK_EXECUTOR.submit(_run)
        except Exception as e:
            logger.error("企微系统告警提交线程池失败: %s", e)

    def push_markdown_async(self, content: str) -> None:
        """将 _send_markdown_sync 提交至全局有界线程池，不阻塞调用方。"""

        def _run() -> None:
            try:
                self._send_markdown_sync(content)
            except Exception as e:
                # 异步线程内失败不得冒泡到扫描引擎；日志用 warning 便于 7x24 巡检（重试细节已在 _send_markdown_sync 内打 error）
                logger.warning("企微异步发送兜底异常(已吞): %s", e, exc_info=True)

        try:
            _WECHAT_WEBHOOK_EXECUTOR.submit(_run)
        except Exception as e:
            logger.error("企微推送提交线程池失败: %s", e)

    def push_stock_if_allowed(
        self,
        pool_key: str,
        stock_dict: Dict[str, Any],
        *,
        pool_key_for_dedup: Optional[str] = None,
    ) -> None:
        """
        去重通过后异步推送单票 markdown。
        观察池与主池同票时用 pool_key_for_dedup（如 p3_obs）区分战区；防刷键默认保留 30 分钟滑动窗口，
        但 P3/P4 统一升级为「同池同代码当日仅推一次」，P2/P5 额外叠加「同一分钟重复触发静默」兜底。
        【V26.6 第二阶段】若该股在当日 p5_yesterday_validated.json 中标记为「已剔除」，则整段逻辑提前 return，企微侧零推送。
        """
        try:
            from core.master_control import is_wechat_push_master_enabled

            if not is_wechat_push_master_enabled():
                logger.debug("企微推送跳过(总控关闭): pool=%s", pool_key)
                return
        except Exception as e:
            logger.debug("push_stock_if_allowed master_control: %s", e)
        ts_code = _norm_ts_code(stock_dict.get("代码") if isinstance(stock_dict, dict) else None)
        if not ts_code:
            logger.debug("企微推送跳过：无有效代码")
            return
        # P5 次日早盘闭环：p5_yesterday_validated.json 中「已剔除」的代码一律不发企微（与 Daemon/UI 同源）
        try:
            from core.p5_morning_validation import is_code_blocked_by_morning_p5_validation

            if is_code_blocked_by_morning_p5_validation(ts_code):
                logger.debug("企微推送跳过(P5次日早盘已剔除): code=%s pool=%s", ts_code, pool_key)
                return
        except Exception as e:
            logger.debug("P5 早盘剔除闸读取失败(不拦截): %s", e)
        # 【BugFix】嵌套 .get() 作为 default 参数是 Python 反模式：
        # dict.get(key, dict.get(fallback_key, default)) 中，fallback_key 的
        # .get() 会先被求值（返回 default），而非作为「当 key 不存在时的真正备选」。
        # 此处当 '综合分' 不存在但 'score' 存在时，'score' 的值会被忽略，导致
        # score_v=0.0 而非预期的 score 值。正确做法是先取 _score_raw，再手动链式 fallback。
        _score_raw = stock_dict.get("综合分")
        _score_fb = None
        if _score_raw is not None:
            _score_fb = _finite_float_or_none(_score_raw)
        if _score_fb is None:
            _score_fb = _finite_float_or_none(stock_dict.get("score"))
        score_v = _score_fb if _score_fb is not None else 0.0
        if str(pool_key).lower() in {"p2", "p3", "p4"} and (score_v is None or score_v < 40.0):
            logger.debug("企微推送跳过：%s 分数过低 score=%s code=%s", pool_key, score_v, ts_code)
            return
        pk_slot = (pool_key_for_dedup or pool_key).strip().lower()
        zhanfa_raw = stock_dict.get("战法") if isinstance(stock_dict, dict) else None
        if not self._reserve_push_slot(pk_slot, ts_code, zhanfa_raw):
            return
        try:
            body = format_wechat_markdown(pool_key, stock_dict)
        except Exception as e:
            logger.warning("format_wechat_markdown 外层异常(不应发生): %s", e, exc_info=True)
            body = format_wechat_markdown(pool_key, {})
        advice = _maybe_get_deepseek_advice(pool_key, stock_dict)
        if advice:
            body = _append_deepseek_advice(body, advice)
        self.push_markdown_async(body)


def get_wechat_gateway(webhook_url: str) -> WechatNotificationGateway:
    """单例：同一进程共享受去重字典；URL 变更时重建实例。"""
    global _gateway_singleton
    u = (webhook_url or "").strip()
    with _gateway_lock:
        if _gateway_singleton is None or _gateway_singleton._url != u:
            _gateway_singleton = WechatNotificationGateway(u)
        return _gateway_singleton


def get_wechat_secondary_gateway(webhook_url: str) -> WechatNotificationGateway:
    """副路由单例：与主路由隔离，避免不同消息类型共享同一去重状态。"""
    global _gateway_secondary_singleton
    u = (webhook_url or "").strip()
    with _gateway_lock:
        if _gateway_secondary_singleton is None or _gateway_secondary_singleton._url != u:
            _gateway_secondary_singleton = WechatNotificationGateway(u)
        return _gateway_secondary_singleton


def notify_wechat_system_alert(
    *,
    title: str,
    detail: str = "",
    category: str = "system",
    dedup_key: Optional[str] = None,
    dedup_window_sec: Optional[float] = None,
) -> None:
    """
    数据下载失败、P1～P5 扫描异常等运维温馨提示；默认走 secondary webhook。
    如 secondary 未配置，则回退 primary，避免消息完全丢失。

    默认 dedup_window_sec=12 小时（与 P2～P5 股票池推送无关，后者走 push_stock_if_allowed）。
    显式传入 dedup_key 时，同一键 12h 内不重复；dedup_key 为 None 时按 category+title 稳定哈希去重。
    若某类告警需要更短或更长间隔，可传 dedup_window_sec 覆盖。
    """
    try:
        from core.master_control import is_wechat_system_alert_enabled

        if not is_wechat_system_alert_enabled():
            return
        from core.config_manager import get_notification_config

        cfg = get_notification_config()
        url = _get_wechat_webhook_url("wechat_webhook_url_secondary") or _get_wechat_webhook_url("wechat_webhook_url")
        if not url:
            return
        if dedup_key is None:
            _canon = f"{str(category).strip()}:{str(title).strip()}"[:800]
            dedup_key = hashlib.sha256(_canon.encode("utf-8", errors="ignore")).hexdigest()[:24]
        body = _format_system_alert_body(title, detail or "", category)
        gw = get_wechat_secondary_gateway(url)
        gw.push_system_alert_async(body, dedup_key, dedup_window_sec=dedup_window_sec)
    except Exception as e:
        logger.debug("notify_wechat_system_alert 跳过: %s", e)


def clear_wechat_push_cache_global() -> None:
    """
    清空当前进程已创建的网关单例内的防刷字典。
    若尚未有任何推送、单例未惰性创建，则本调用无操作（无泄漏可清）。
    """
    global _gateway_singleton
    with _gateway_lock:
        if _gateway_singleton is not None:
            _gateway_singleton.clear_push_cache()


def notify_scan_results_top3_p2p4(
    scan_targets: List[str],
    res_dict: Dict[str, Any],
    ui_push_enabled: bool,
) -> None:
    """
    在扫描成功后由 UI / 异步桥调用：P2/P3/P4/P5 主池 + 观察池均推送（观察池带 pool_tier/标题区分，去重键 *_obs）。
    主池 Top：P2/P3/P4 各 3；P5 各 10。观察池同档位数。
    不修改 res_dict；失败静默（打 debug 日志）。
    """
    try:
        from core.config_manager import get_notification_config
    except Exception as e:
        logger.debug("notify_scan_results_top3: 无法导入 config: %s", e)
        return

    cfg = get_notification_config()
    if not ui_push_enabled or not cfg.get("enabled"):
        return
    try:
        from core.master_control import is_wechat_push_master_enabled

        if not is_wechat_push_master_enabled():
            logger.debug("notify_scan_results_top3：总控企微推送关闭，跳过")
            return
    except Exception as e:
        logger.debug("notify_scan_results_top3：master_control 读取异常，继续: %s", e)
    url = _get_wechat_webhook_url("wechat_webhook_url")
    if not url:
        return

    gw = get_wechat_gateway(url)
    targets = {str(x).lower() for x in (scan_targets or [])}
    obs_root = res_dict.get("observation") or {}
    if not isinstance(obs_root, dict):
        obs_root = {}

    def _score(row: Dict[str, Any]) -> float:
        x = _finite_float_or_none(row.get("综合分", 0.0))
        return x if x is not None else 0.0

    for pk in ("p2", "p3", "p4", "p5"):
        if pk not in targets:
            continue
        top_n = 10 if pk == "p5" else 3
        rows = res_dict.get(pk)
        if isinstance(rows, list) and rows:
            sorted_rows = sorted(rows, key=_score, reverse=True)
            for row in sorted_rows[: max(1, top_n)]:
                if not isinstance(row, dict):
                    continue
                try:
                    gw.push_stock_if_allowed(pk, row)
                except Exception as e:
                    logger.debug("push_stock_if_allowed 异常: %s", e, exc_info=True)

        obs_rows = obs_root.get(pk)
        if isinstance(obs_rows, list) and obs_rows:
            sorted_obs = sorted(obs_rows, key=_score, reverse=True)
            for row in sorted_obs[: max(1, top_n)]:
                if not isinstance(row, dict):
                    continue
                try:
                    gw.push_stock_if_allowed(pk, row, pool_key_for_dedup=f"{pk}_obs")
                except Exception as e:
                    logger.debug("push_stock_if_allowed(obs) 异常: %s", e, exc_info=True)


def notify_scan_results_top3_p2p3p4(
    scan_targets: List[str],
    res_dict: Dict[str, Any],
    ui_push_enabled: bool,
) -> None:
    """向后兼容旧调用名，内部转发到 `notify_scan_results_top3_p2p4`。"""
    return notify_scan_results_top3_p2p4(scan_targets, res_dict, ui_push_enabled)

def _safe_float_p1(val: Any, default: float = 0.0) -> float:
    """从 P1 底仓项 hist 中解析浮点；不依赖 pandas，供高分池推送专用。"""
    if val is None:
        return default
    try:
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return default
        s = str(val).strip()
        if s in ("", "-", "None", "nan"):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _format_net_main_amount_display(yuan: Any) -> str:
    """
    底层 net_main_amount 单位为「元」：展示时换算为「万」或「亿」，保留两位小数。
    非有限值或缺失时返回「暂无」。
    """
    x = _finite_float_or_none(yuan)
    if x is None:
        return "暂无"
    ax = abs(x)
    if ax < 1e-9:
        return "0.00万"
    neg = x < 0
    if ax >= 1e8:
        s = f"{ax / 1e8:.2f}亿"
    elif ax >= 1e4:
        s = f"{ax / 1e4:.2f}万"
    else:
        s = f"{ax / 1e4:.2f}万"
    return ("-" if neg else "") + s


def _build_merged_field_map(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    合并 hist 与 df 最后一行字段，便于统一用 .get 提取 net_main_amount / 概念类列。
    df 列在后写时仅填补 hist 中缺失或为空的键，避免覆盖实时字段。
    """
    merged: Dict[str, Any] = {}
    hist = item.get("hist")
    if isinstance(hist, dict):
        merged.update(hist)
    df_obj = item.get("df")
    try:
        import pandas as pd

        if isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
            last = df_obj.iloc[-1]
            ld = last.to_dict()
            for k, v in ld.items():
                old = merged.get(k)
                if old is None or (isinstance(old, str) and str(old).strip() in ("", "nan", "--")):
                    try:
                        if pd.isna(v):
                            continue
                    except Exception:
                        pass
                    merged[k] = v
    except Exception:
        pass
    return merged


def _format_plate_and_concept(merged: Dict[str, Any], ts_code_full: Optional[str]) -> str:
    """
    安全拼接申万行业（库表）与概念/板块类字段；缺失时「暂无」。
    列名兼容：industry、concept、concepts、概念 等，视底层而定。
    """
    pieces: List[str] = []
    seen = set()

    def _add_one(val: Any) -> None:
        if val is None:
            return
        try:
            import pandas as pd

            if pd.isna(val):
                return
        except Exception:
            pass
        s = str(val).strip()
        if not s or s.lower() in ("nan", "none", "--", "未知"):
            return
        for part in s.replace("，", ",").split(","):
            p = part.strip()
            if p and p not in seen:
                seen.add(p)
                pieces.append(p)

    if ts_code_full:
        try:
            from data.db_core import get_stock_industry

            ind = get_stock_industry(str(ts_code_full).strip())
            _add_one(ind)
        except Exception:
            pass

    for key in (
        "industry",
        "concept",
        "concepts",
        "概念",
        "概念板块",
        "leading_concept",
        "sector_concept",
        "industry_name",
        "bk_name",
        "板块",
    ):
        raw = merged.get(key, None)
        if raw is not None:
            _add_one(raw)

    if not pieces:
        return "暂无"
    return ",".join(pieces[:16])


def _extract_net_main_yuan(merged: Dict[str, Any]) -> Any:
    """从合并字段中安全取主力净额（元）；多列名兜底。"""
    if not isinstance(merged, dict):
        return None
    for key in (
        "net_main_amount",
        "主力净额",
        "net_main",
        "main_net_amount",
        "main_net",
    ):
        if key in merged:
            return merged.get(key)
    return None


def _eastmoney_pc_quote_url(ts_code_raw: Any) -> str:
    """
    将 ts_code（如 600000.SH）转为东方财富 PC 行情页链接，供企微 markdown [text](url) 使用。
    """
    s = str(ts_code_raw or "").strip().upper()
    if not s:
        return "https://www.eastmoney.com/"
    if "." in s:
        num, suf = s.split(".", 1)
        num = num[:6].zfill(6)
        suf = suf.strip()
    else:
        num = s[:6].zfill(6)
        if num.startswith("6"):
            suf = "SH"
        elif num.startswith(("8", "4")):
            suf = "BJ"
        else:
            suf = "SZ"
    if suf in ("SH", "SSE"):
        return f"https://quote.eastmoney.com/sh{num}.html"
    if suf in ("SZ", "SHE"):
        return f"https://quote.eastmoney.com/sz{num}.html"
    if suf in ("BJ", "BSE"):
        return f"https://quote.eastmoney.com/bj{num}.html"
    return f"https://quote.eastmoney.com/sz{num}.html"


def _guess_ts_code_full(code_str: str, s_code: str) -> str:
    """6 位代码补全为 Tushare 风格 ts_code，供 stock_basic 查简称。"""
    t = str(code_str or "").strip()
    if "." in t:
        return t.upper()
    sc = (s_code or "")[:6].zfill(6)
    if sc.startswith("6"):
        return f"{sc}.SH"
    if sc.startswith(("0", "3")):
        return f"{sc}.SZ"
    if sc.startswith(("8", "4")):
        return f"{sc}.BJ"
    return f"{sc}.SZ"


def _flatten_merged_stock_name_raw(merged: Dict[str, Any]) -> str:
    """从 hist/df 合并字段取证券简称；纯 6 位数字视为无效（常为误填）。"""
    for key in ("name", "名称", "stock_name", "证券简称", "股票简称"):
        v = merged.get(key)
        if v is None:
            continue
        try:
            import pandas as pd

            if pd.isna(v):
                continue
        except Exception:
            pass
        t = str(v).strip()
        if not t or t.lower() in ("nan", "none", "--", "null"):
            continue
        if len(t) == 6 and t.isdigit():
            continue
        return t
    return ""


def _resolve_p1_push_display_name(merged: Dict[str, Any], code_str: str, s_code: str) -> str:
    """
    P1 企微推送用简称：daily_data 末行常无 name，仅用 6 位兜底会导致「002074 (002074)」。
    优先 merged；否则用本地 stock_basic（与 map_ts_codes_to_names_local 一致）。
    """
    _nm_raw = _flatten_merged_stock_name_raw(merged)
    name = normalize_stock_display_name(_nm_raw or s_code)
    looks_like_code_only = not _nm_raw or _nm_raw == s_code or name == s_code
    if not looks_like_code_only:
        return name
    tc = str(merged.get("ts_code") or "").strip()
    if not tc:
        tc = _guess_ts_code_full(code_str, s_code)
    try:
        from data.db_core import map_ts_codes_to_names_local

        m = map_ts_codes_to_names_local([tc])
        got = m.get(tc.upper().strip())
        if got and str(got).strip() and str(got).strip() != s_code:
            return normalize_stock_display_name(got)
    except Exception as e:
        logger.debug("P1 推送简称补全(map_ts_codes_to_names_local): %s", e)
    return name


def _fill_push_row_from_merged(
    merged: Dict[str, Any],
    code_str: str,
    p1_score_override: Optional[float],
) -> Optional[Dict[str, Any]]:
    """
    由合并字段生成推送行：现价/涨幅/综合分/板块概念/主力展示。
    """
    if not code_str:
        return None
    s_code = str(code_str).split(".")[0][:6]
    if len(s_code) < 6:
        s_code = s_code.zfill(6)[:6]
    name = _resolve_p1_push_display_name(merged, code_str, s_code)
    db_close_p = _safe_float_p1(merged.get("close", 0.0), 0.0)
    pre_c = _safe_float_p1(merged.get("pre_close"), 0.0)
    if pre_c <= 0:
        pre_c = _safe_float_p1(merged.get("close"), 0.0)
    pct = (db_close_p - pre_c) / pre_c * 100.0 if pre_c > 0 else 0.0

    if p1_score_override is not None:
        score = float(p1_score_override)
    else:
        # 【BugFix】同上，嵌套 .get() 反模式。若 'p1_score' 不存在但 '综合分' 存在，
        # merged.get("综合分", 0.0) 的 default 0.0 先被求值，导致综合分被忽略。
        _p1_raw = merged.get("p1_score")
        _p1_fb = None
        if _p1_raw is not None:
            _p1_fb = _finite_float_or_none(_p1_raw)
        if _p1_fb is None:
            _p1_fb = _finite_float_or_none(merged.get("综合分"))
        sc = _p1_fb
        score = float(sc) if sc is not None else 0.0

    nm_raw = _extract_net_main_yuan(merged)
    main_force_display = _format_net_main_amount_display(nm_raw)
    ts_full = str(merged.get("ts_code") or code_str or "").strip()
    if not ts_full or "." not in ts_full:
        ts_full = _guess_ts_code_full(code_str, s_code)
    plate_display = _format_plate_and_concept(merged, ts_full)

    return {
        "代码": s_code,
        "名称": name,
        "ts_code": ts_full,
        "综合分": score,
        "现价展示": f"{db_close_p:.2f}",
        "涨幅展示": f"{pct:.2f}%",
        "板块概念展示": plate_display,
        "主力展示": main_force_display,
    }


def _p1_base_item_to_push_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    将 build_p1_pool_and_cache 产出的单条底仓项转为推送用行（含综合分、展示用现价/涨幅字符串）。
    与 ui/app.py 中 P1 表格行的推算口径保持一致（昨收、收盘）。
    """
    if not isinstance(item, dict):
        return None
    code = item.get("code")
    if not code:
        return None
    merged = _build_merged_field_map(item)
    sc = _finite_float_or_none(item.get("p1_score", 0.0))
    p1_override = float(sc) if sc is not None else None
    return _fill_push_row_from_merged(merged, str(code).strip(), p1_override)


def _flat_record_to_push_row(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    将扁平 dict（如 DataFrame 行）转为与 _p1_base_item_to_push_row 相同结构的推送行。
    """
    if not isinstance(rec, dict):
        return None
    code = rec.get("ts_code") or rec.get("code") or rec.get("代码")
    if not code:
        return None
    merged = dict(rec)
    sc = _finite_float_or_none(rec.get("p1_score", rec.get("综合分")))
    p1_override = float(sc) if sc is not None else None
    return _fill_push_row_from_merged(merged, str(code).strip(), p1_override)


def normalize_p1_high_score_source(src: Any) -> List[Dict[str, Any]]:
    """
    将 list[底仓项] 或 pandas.DataFrame 规范为推送行列表；无法识别的项跳过。
    """
    if src is None:
        return []
    try:
        import pandas as pd

        if isinstance(src, pd.DataFrame):
            if src.empty:
                return []
            # 【性能优化 V3】使用向量化 to_dict 替代 iterrows，避免逐行 Python 迭代
            # 直接使用 pandas 的批量转换，比逐行 iterrows 快 10-50 倍
            try:
                # 批量转换为字典列表
                records = src.to_dict('records')
                out: List[Dict[str, Any]] = []
                for d in records:
                    pr = _flat_record_to_push_row(d)
                    if pr:
                        out.append(pr)
                return out
            except Exception:
                # 终极兜底：即使转换失败也返回空列表
                return []
    except Exception:
        pass

    if isinstance(src, list):
        out2: List[Dict[str, Any]] = []
        for it in src:
            if not isinstance(it, dict):
                continue
            if it.get("hist") is not None or (
                it.get("df") is not None and it.get("code")
            ):
                r = _p1_base_item_to_push_row(it)
            else:
                r = _flat_record_to_push_row(it)
            if r:
                out2.append(r)
        return out2
    return []


def format_p1_high_score_pool_markdown(
    rows: List[Dict[str, Any]],
    total_ge_60: int,
    shown: int,
    *,
    total_main: int = 0,
    total_backup: int = 0,
    main_score: float = 75.0,
) -> str:
    """
    组装「P1 核心战略底仓 (高分池)」企微 markdown 正文；走与单票推送相同的 HTML 转义策略。
    rows：已截断后的列表，每项含 代码/名称/ts_code/综合分/现价展示/涨幅展示/板块概念展示/主力展示。
    """
    lines: List[str] = []
    title = f"## V26.6 🌙 P1 底仓池 [{_bj_now_str('%H:%M:%S')}]"
    lines.append(title)
    lines.append("")
    try:
        from zoneinfo import ZoneInfo

        now_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f'<font color="comment">{_esc(now_str)}</font>')
    lines.append("")
    for i, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        code_plain = _norm_ts_code(row.get("代码")) or str(row.get("代码", "") or "").strip() or "--"
        name_raw = normalize_stock_display_name(row.get("名称", "--"))
        link_label = _esc(f"{name_raw if name_raw is not None else '--'} ({code_plain})")
        url = _eastmoney_pc_quote_url(row.get("ts_code") or row.get("代码"))
        score_v = _finite_float_or_none(row.get("综合分", 0.0))
        score_txt = f"{float(score_v):.2f}" if score_v is not None else "--"
        price_txt = _esc(row.get("现价展示", "--"))
        pct_txt = _esc(row.get("涨幅展示", "--"))
        link_line = f"[{link_label}]({url})"
        if score_v is not None and score_v >= 85.0:
            tag = "主推"
        else:
            tag = "观察"
        lines.append(
            f"{i}. [{tag}] {link_line} - 综合分: {score_txt}分 | 现价: {price_txt} | 涨幅: {pct_txt}"
        )
        plate = _esc(str(row.get("板块概念展示", "暂无")))
        mainf = _esc(str(row.get("主力展示", "暂无")))
        lines.append(f"   └ 🏷️ 板块: {plate} | 💰 主力: {mainf}")
    lines.append("")
    lines.append(
        f"💡 提示：共发现 {int(total_ge_60)} 只 60分以上标的，其中主推 {int(total_main)} 只、备选 {int(total_backup)} 只；仅展示前 {int(shown)} 只。"
    )
    if int(shown) > 0:
        lines.append(
            "> 温馨提示：炒股有风险，决策需谨慎；本消息仅作策略信号参考，不构成投资建议。"
        )
    body = "\n".join(lines)
    if len(body) > _MAX_MARKDOWN_CONTENT_LEN:
        body = body[: _MAX_MARKDOWN_CONTENT_LEN - 20] + "\n…(已截断)"
    return body


def notify_p1_high_score_pool_after_wash(
    base_items: Union[List[Dict[str, Any]], Any],
    *,
    min_score: float = 60.0,
    main_score: float = 75.0,
    max_items: int = 10,
) -> None:
    """
    在 UI 完成「全量洗盘」且 P1 底仓列表已落盘之后调用：
    - 仅当 master_control「推送 P1 高分池」开启时执行；关闭时立即返回，不做遍历与网络请求。
    - 过滤综合分>=min_score，按分数降序；正文中统一标注 [主推]/[观察]，85 分以上才算主推。
    - 全量展示，不做截断；UI 侧有多少显示多少。
    - 复用 get_wechat_gateway + push_markdown_async，与现有企微链路一致。
    - base_items：list[底仓 dict]（含 hist/df/code）或 pandas.DataFrame（列名需含 code/ts_code 与行情字段）。
    """
    try:
        from core.master_control import is_push_p1_high_score_enabled, is_wechat_push_master_enabled
    except Exception as e:
        logger.debug("notify_p1_high_score_pool: master_control 导入失败: %s", e)
        return

    if not is_push_p1_high_score_enabled():
        return
    if not is_wechat_push_master_enabled():
        logger.debug("notify_p1_high_score_pool：企微总闸关闭，跳过")
        return
    if base_items is None:
        return

    try:
        from core.config_manager import get_notification_config
    except Exception as e:
        logger.debug("notify_p1_high_score_pool: 无法导入 config: %s", e)
        return

    cfg = get_notification_config()
    if not cfg.get("enabled"):
        return
    url = _get_wechat_webhook_url("wechat_webhook_url")
    if not url:
        return

    parsed = normalize_p1_high_score_source(base_items)
    if not parsed:
        return

    min_score = float(min_score)
    main_score = float(main_score)
    max_items = max(1, int(max_items))

    filtered = [r for r in parsed if float(r.get("综合分", 0.0)) >= min_score]
    if not filtered:
        return

    filtered.sort(key=lambda x: float(x.get("综合分", 0.0)), reverse=True)
    main_rows = [r for r in filtered if float(r.get("综合分", 0.0)) >= 85.0]
    backup_rows = [r for r in filtered if min_score <= float(r.get("综合分", 0.0)) < 85.0]
    display_rows = filtered[:15]
    shown = len(display_rows)
    total_ge_60 = len(filtered)

    try:
        body = format_p1_high_score_pool_markdown(
            display_rows,
            total_ge_60,
            shown,
            total_main=len(main_rows),
            total_backup=len(backup_rows),
            main_score=85.0,
        )
    except Exception as e:
        logger.warning("format_p1_high_score_pool_markdown 异常: %s", e, exc_info=True)
        return

    gw = get_wechat_gateway(url)
    try:
        gw.push_markdown_async(body)
    except Exception as e:
        logger.debug("notify_p1_high_score_pool push_markdown_async: %s", e, exc_info=True)
