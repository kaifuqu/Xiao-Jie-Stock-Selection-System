# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 — 策略参数统一入口：从项目根 config.yaml 的 strategies: 读取，
并合并 Streamlit 会话中的「策略实验室」覆写（无需重启）。

说明：
- 非 UI 环境（CLI/定时任务）下实验室覆写为空 dict，行为与仅 YAML 一致。
- 每次 get_* 会检查 config.yaml 的 mtime，文件被外部修改后自动重载。
- 根节点 ``risk_control:`` 供三层风控 ``RiskControlConfig`` 使用（见 ``get_risk_control_config``）。
- API Key 配置：优先从环境变量读取，其次从 .env 文件读取，支持首次运行交互式引导。
"""
from __future__ import annotations

import getpass
import logging
import math
import os
import re
import shutil
import sys
import threading
from dataclasses import MISSING, fields, is_dataclass, replace
from typing import Any, Dict, List, Optional, Type, TypeVar

import yaml

logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_RAW_CACHE: Dict[str, Any] = {}  # path -> {"mtime": float, "data": dict}

T = TypeVar("T")


def _project_root() -> str:
    d = os.path.dirname(os.path.abspath(__file__))
    for _ in range(5):
        if os.path.exists(os.path.join(d, "config.yaml")):
            return d
        d = os.path.dirname(d)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _env_path() -> str:
    """返回 .env 文件路径（位于项目根目录）"""
    return os.path.join(_project_root(), ".env")


def _env_example_path() -> str:
    """返回 .env.example 模板文件路径"""
    return os.path.join(_project_root(), ".env.example")


def _load_env_vars() -> Dict[str, str]:
    """
    加载 .env 文件中的环境变量到 os.environ。
    仅首次加载（导入时自动调用一次），后续从 os.environ 直接读取。
    """
    env_path = _env_path()
    loaded: Dict[str, str] = {}
    if not os.path.exists(env_path):
        return loaded
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if "=" not in stripped:
                    continue
                key, _, val = stripped.partition("=")
                key = key.strip()
                val = val.strip()
                # 移除可能的引号
                if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    val = val[1:-1]
                if key and val:
                    os.environ.setdefault(key, val)
                    loaded[key] = val
    except Exception as e:
        logger.debug("_load_env_vars 读取 .env 失败: %s", e)
    return loaded


def _is_placeholder(value: str) -> bool:
    """判断是否为占位符（空或只含提示性内容）"""
    if not value:
        return True
    # 常见占位符模式
    placeholder_patterns = [
        r"^<.*>$",          # <your-token-here>
        r"^your[-_]token$", # your_token / your-token
        r"^sk[-_]?placeholder$",
        r"^xxx+$",          # xxx, xxxxxx
        r"^[*]+$",          # ****
        r"^填.*key",        # 填入key
    ]
    for p in placeholder_patterns:
        if re.match(p, value, re.IGNORECASE):
            return True
    return False


def _is_tushare_token_valid(token: str) -> bool:
    """简单验证 Tushare token 格式（32位以上字母数字）"""
    if not token or len(token) < 16:
        return False
    return bool(re.match(r'^[a-zA-Z0-9]+$', token))


def _is_deepseek_key_valid(key: str) -> bool:
    """简单验证 DeepSeek API key 格式（sk- 开头）"""
    if not key:
        return False
    return bool(re.match(r'^sk-[a-zA-Z0-9]+', key))


def _setup_api_keys_interactive() -> None:
    """
    交互式首次配置引导：引导用户输入 Tushare Token、DeepSeek Key 和企微 Webhook URL，并保存到 .env。
    仅当 .env 不存在或 key 全为占位符时才触发。
    """
    print("\n" + "=" * 60)
    print("小杰AI选股系统 — 首次配置向导")
    print("=" * 60)
    print("检测到尚未配置 Key，请按提示输入：")
    print("（输入后自动保存到 .env 文件，下次运行无需再输入）\n")

    root = _project_root()

    # --- Tushare Token ---
    print("【1/4】Tushare Pro Token（必填）")
    print("  → 从 https://tushare.pro/register 注册后获取")
    print("  → 注册即送 2000 积分，每日签到 +10 积分")
    while True:
        token_input = input("  请输入 Tushare Token: ").strip()
        if not token_input:
            print("  ⚠️  Token 不能为空，否则无法下载股票数据")
            continue
        if _is_tushare_token_valid(token_input):
            token_to_save = token_input
            print(f"  ✅ Token 格式验证通过（长度={len(token_input)}）")
            break
        print("  ⚠️  Token 格式不正确（应为32位以上字母数字），请重新输入")

    # --- DeepSeek API Key ---
    print("\n【2/4】DeepSeek API Key（可选，回车跳过）")
    print("  → 从 https://platform.deepseek.com 注册后获取")
    print("  → 用于 AI 选股分析建议，跳过则不启用该功能")
    while True:
        key_input = input("  请输入 DeepSeek API Key（直接回车跳过）: ").strip()
        if not key_input:
            print("  ⏭️  已跳过，AI 分析功能将不可用")
            key_to_save = ""
            break
        if _is_deepseek_key_valid(key_input):
            key_to_save = key_input
            print(f"  ✅ API Key 格式验证通过")
            break
        print("  ⚠️  API Key 格式不正确（应以 sk- 开头），请重新输入")

    # --- 企微 Webhook URL（主）---
    print("\n【3/4】企业微信 Webhook URL（可选，回车跳过）")
    print("  → 用于接收 P1~P5 选股推送结果")
    print("  → 在企业微信群中添加「自定义机器人」，复制 Webhook 地址")
    while True:
        wx_input = input("  请输入企微主 Webhook URL（直接回车跳过）: ").strip()
        if not wx_input:
            print("  ⏭️  已跳过，推送功能将不可用")
            wx_to_save = ""
            break
        if wx_input.startswith("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="):
            wx_to_save = wx_input
            print("  ✅ Webhook URL 格式正确")
            break
        print("  ⚠️  URL 格式不正确，应以 https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key= 开头")

    # --- 企微 Webhook URL（次）---
    print("\n【4/4】企业微信 Webhook URL（次要，可选，回车跳过）")
    print("  → 用于接收系统维护、下载进度、错误告警等系统消息")
    print("  → 可与主 URL 相同，跳过则与主 URL 共用")
    while True:
        wx2_input = input("  请输入企微次 Webhook URL（直接回车与主 URL 共用）: ").strip()
        if not wx2_input:
            print("  ⏭️  次 URL 将与主 URL 共用")
            wx2_to_save = ""
            break
        if wx2_input.startswith("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="):
            wx2_to_save = wx2_input
            print("  ✅ Webhook URL 格式正确")
            break
        print("  ⚠️  URL 格式不正确，应以 https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key= 开头")

    # --- 保存到 .env ---
    print("\n正在保存配置到 .env ...")
    env_lines = []
    env_path = _env_path()

    example_path = _env_example_path()
    if os.path.exists(example_path):
        try:
            with open(example_path, "r", encoding="utf-8") as f:
                env_lines = f.readlines()
        except Exception:
            env_lines = []

    if not env_lines:
        import datetime
        env_lines = [
            f"# 小杰AI选股系统 — 环境变量配置（生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}）\n",
            "\n",
            "TUSHARE_TOKEN=\n",
            "DEEPSEEK_API_KEY=\n",
            "WEIXIN_WEBHOOK_URL=\n",
            "WEIXIN_WEBHOOK_URL_SECONDARY=\n",
        ]

    new_content_lines = []
    pending = {
        "TUSHARE_TOKEN": token_to_save,
        "DEEPSEEK_API_KEY": key_to_save,
        "WEIXIN_WEBHOOK_URL": wx_to_save,
        "WEIXIN_WEBHOOK_URL_SECONDARY": wx2_to_save,
    }

    for line in env_lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            new_content_lines.append(line)
            continue
        if "=" in stripped:
            key_part = stripped.split("=", 1)[0].strip()
            if key_part in pending and pending[key_part]:
                new_content_lines.append(f'{key_part}={pending[key_part]}\n')
                pending[key_part] = ""  # 标记已处理
            else:
                new_content_lines.append(line)
        else:
            new_content_lines.append(line)

    # 追加未处理的 key
    for k, v in pending.items():
        if v:
            new_content_lines.append(f'{k}={v}\n')

    try:
        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(new_content_lines)
        print(f"  ✅ 配置已保存到 {env_path}")
        _load_env_vars()
    except Exception as e:
        print(f"  ⚠️  保存 .env 失败: {e}")
        print("  请手动创建 .env 文件，内容如下：")
        for k, v in pending.items():
            print(f"  {k}={v or '<请填入>'}")

    print("\n" + "=" * 60)
    print("首次配置完成！重新运行系统即可正常使用。")
    print("=" * 60 + "\n")


def _check_and_prompt_first_time_setup() -> None:
    """
    检测是否需要首次配置引导。
    触发条件：.env 不存在，或其中的 key 全为占位符。
    仅在主线程且 TTY 可用时才触发交互式引导。
    """
    env_path = _env_path()
    needs_setup = False

    if not os.path.exists(env_path):
        needs_setup = True
    else:
        # .env 存在，检查值是否为占位符
        env_vars = _load_env_vars()
        ts = env_vars.get("TUSHARE_TOKEN", "")
        ds = env_vars.get("DEEPSEEK_API_KEY", "")
        # 如果 .env 存在但两个 key 都是空的/占位符，也触发引导
        if _is_placeholder(ts) and _is_placeholder(ds):
            needs_setup = True

    if needs_setup and sys.stdout.isatty() and sys.stdin.isatty():
        _setup_api_keys_interactive()


# 导入时自动检查首次配置（仅主进程执行，防止多进程重复触发）
_import_check_done = False
if not _import_check_done:
    _import_check_done = True
    # 加载已有 .env
    _load_env_vars()
    # 检查并引导首次配置
    _check_and_prompt_first_time_setup()


def _config_path() -> str:
    return os.path.join(_project_root(), "config.yaml")


def _load_yaml_raw(force: bool = False) -> dict:
    """
    【性能优化 V2】
    - 移除 copy.deepcopy()：返回缓存字典的直接引用，而非每次深拷贝。
    - 调用方若需修改，应自行深拷贝。
    - mtime 未变化时直接返回缓存引用，避免每次读配置（50+ 次 get_* 调用）都克隆数百个嵌套键。
    """
    path = _config_path()
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return {}
    with _LOCK:
        if (
            not force
            and _RAW_CACHE.get("path") == path
            and _RAW_CACHE.get("mtime") == mtime
            and isinstance(_RAW_CACHE.get("data"), dict)
        ):
            # 【优化V2】不再 deepcopy，直接返回缓存引用（调用方如需修改请自行拷贝）
            return _RAW_CACHE["data"]
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning("读取 config.yaml 失败: %s", e)
            data = {}
        _RAW_CACHE["path"] = path
        _RAW_CACHE["mtime"] = mtime
        _RAW_CACHE["data"] = data
        # 【优化V2】返回缓存引用而非深拷贝
        return data


def get_strategies_dict(force_reload: bool = False) -> dict:
    raw = _load_yaml_raw(force=force_reload)
    s = raw.get("strategies")
    return s if isinstance(s, dict) else {}


def invalidate_config_cache() -> None:
    """实验室写入 YAML 或需强制刷新时调用。"""
    with _LOCK:
        _RAW_CACHE.clear()


def _lab_root() -> dict:
    if os.environ.get("XIAOJIE_DAEMON_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        return {}
    try:
        import streamlit as st

        root = st.session_state.get("strategy_lab_overrides")
        return dict(root) if isinstance(root, dict) else {}
    except Exception:
        return {}


def _lab_for(pool_key: str) -> dict:
    d = _lab_root().get(pool_key)
    return dict(d) if isinstance(d, dict) else {}


def deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _field_default(f) -> Any:
    if getattr(f, "default_factory", MISSING) is not MISSING:
        return f.default_factory()
    if getattr(f, "default", MISSING) is not MISSING:
        return f.default
    return None


def _coerce_for_field(name: str, val: Any, annotation: Any) -> Any:
    if val is None:
        return None
    ann = annotation
    if ann is int or getattr(ann, "__name__", "") == "int":
        try:
            return int(round(float(val)))
        except (TypeError, ValueError):
            return int(val)
    if ann is float or getattr(ann, "__name__", "") == "float":
        return float(val)
    if ann is bool or getattr(ann, "__name__", "") == "bool":
        return bool(val)
    return val


def dataclass_from_merged(cls: Type[T], yaml_flat: dict, lab_flat: dict) -> T:
    if not is_dataclass(cls):
        raise TypeError(f"{cls} is not a dataclass")
    y = {k: v for k, v in (yaml_flat or {}).items() if v is not None}
    lab = lab_flat or {}
    kwargs: Dict[str, Any] = {}
    for f in fields(cls):
        if f.name in lab:
            raw = lab[f.name]
        elif f.name in y:
            raw = y[f.name]
        else:
            kwargs[f.name] = _field_default(f)
            continue
        kwargs[f.name] = _coerce_for_field(f.name, raw, f.type)
    return cls(**kwargs)


# 策略 YAML / 策略实验室会话中旧键 → 当前 dataclass 字段（新键缺省时拷贝数值，不删旧键以免干扰排查）
_STRATEGY_YAML_ALIASES: Dict[str, Dict[str, str]] = {
    "p2": {"s4_net_elg_ratio_of_float_mv": "s4_net_main_ratio_of_float_mv"},
    "p3": {
        "s4_net_elg_ratio_of_float_mv": "s4_net_main_ratio_of_float_mv",
        "s2_ma10_touch_ratio": "s2_ma20_touch_ratio",
        "s5_avg_cost_mult": "s5_cost50_mult",
    },
}


def _apply_strategy_flat_aliases(flat: Any, pool_key: str) -> dict:
    if not isinstance(flat, dict):
        return {}
    aliases = _STRATEGY_YAML_ALIASES.get(pool_key, {})
    out = dict(flat)
    for old_k, new_k in aliases.items():
        if old_k in out and new_k not in out:
            out[new_k] = out[old_k]
    return out


def _profile_key_for_regime(regime_name: Optional[str]) -> str:
    name = str(regime_name or "").strip()
    if any(k in name for k in ["主升", "趋势"]):
        return "strict"
    if any(k in name for k in ["退潮", "空头", "主跌"]):
        return "relaxed"
    return "neutral"


def get_p1_profiles_merged(regime_name: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """返回三档 profile（已合并 YAML + 实验室对 profiles 子树的覆写）。"""
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    profiles = p1.get("profiles") if isinstance(p1.get("profiles"), dict) else {}
    lab_p1 = _lab_for("p1")
    if isinstance(lab_p1.get("profiles"), dict):
        profiles = deep_merge(profiles, lab_p1["profiles"])
    return {
        "strict": dict(profiles.get("strict") or {}),
        "neutral": dict(profiles.get("neutral") or {}),
        "relaxed": dict(profiles.get("relaxed") or {}),
        "_active_key": _profile_key_for_regime(regime_name),
    }


def _p1_prof_to_threshold_dict(prof: dict) -> dict:
    """
    将单档 profile dict 转换为阈值 dict。
    
    【⚖️ 宪法级 50 分绝对底线】：
    pass_line 是选股系统的宪法级门槛，无论配置文件如何设置，最终值不得低于 50.0 分。
    - 若配置文件 pass_line < 50.0 → 强制提升至 50.0
    - 若配置文件 pass_line 为 None / 空 / 非数字 → 回退至 50.0
    - 若配置文件 pass_line > 100.0 → 封顶至 100.0（物理上界）
    """
    _pl_raw = prof.get("pass_line") if isinstance(prof, dict) else None
    try:
        _pl_val = float(_pl_raw) if _pl_raw is not None else None
    except (TypeError, ValueError):
        _pl_val = None
    # 【⚖️ 宪法级 clamp】：最终 pass_line 锁定在 [50.0, 100.0] 区间
    _pl_clamped = max(min(float(_pl_val if _pl_val is not None else 50.0), 100.0), 50.0)
    
    return {
        "trend_ma120_min_ratio": float(prof.get("trend_ma120_min_ratio", 0.98)) if isinstance(prof, dict) else 0.98,
        "trend_slope_fastpass": float(prof.get("trend_slope_fastpass", 0.25)) if isinstance(prof, dict) else 0.25,
        "near_ma20_min_ratio": float(prof.get("near_ma20_min_ratio", 0.985)) if isinstance(prof, dict) else 0.985,
        "macd_bar_kill": float(prof.get("macd_bar_kill", -0.13)) if isinstance(prof, dict) else -0.13,
        "vol_divergence_ratio": float(prof.get("vol_divergence_ratio", 0.85)) if isinstance(prof, dict) else 0.85,
        # 【⚖️ 宪法级 50 分绝对底线】：
        "pass_line": _pl_clamped,
    }


def get_p1_regime_thresholds(regime_name: Optional[str] = None) -> dict:
    """
    与 pool_manager._get_regime_thresholds 输出结构一致。
    
    【⚖️ 宪法级 50 分绝对底线】：
    本函数是整个 pass_line 配置链的核心入口。
    无论 config.yaml 是否存在、YAML 是否解析失败、策略实验室是否传入异常值，
    最终返回的 pass_line 字段必定在 [50.0, 100.0] 区间内。
    这是 config_manager 层的最后一道防线，配合 pool_manager._get_regime_thresholds
    的第二道保险，共同构成双层 50 分宪法护城河。
    """
    merged = get_p1_profiles_merged(regime_name=regime_name)
    key = merged.pop("_active_key", "neutral")
    prof = merged.get(key) or merged.get("neutral") or {}
    return _p1_prof_to_threshold_dict(prof)


def get_p1_select_min_circ_mv_wan() -> int:
    """
    P1 候选 SQL 与安检共用流通下限（万元）。
    优先级：策略实验室 p1.select_min_circ_mv_wan > config.yaml strategies.p1 > constants.P1_SELECT_MIN_CIRC_MV_WAN。
    """
    import constants as c

    default = int(getattr(c, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    lab = _lab_for("p1")
    v = lab.get("select_min_circ_mv_wan")
    if v is None:
        v = p1.get("select_min_circ_mv_wan")
    if v is None:
        return default
    try:
        return max(0, int(float(v)))
    except (TypeError, ValueError):
        return default


def get_p1_respect_scan_blacklist() -> bool:
    """
    是否在 P1 洗盘中拦截 scan_engine 写入的 danger 黑名单（blacklist.json）。
    默认 False：黑名单仍用于扫描链路禁买/记录，但不因一次误判 danger 把标的永久踢出 P1 基因池。
    设为 True 可恢复「冷却期内 P1 也拦截」的旧行为。
    """
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    lab = _lab_for("p1")
    v = lab.get("respect_scan_blacklist_for_p1")
    if v is None:
        v = p1.get("respect_scan_blacklist_for_p1")
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s2 = str(v).strip().lower()
    return s2 in ("1", "true", "yes", "on")


def get_p1_use_tushare_unlock_blacklist() -> bool:
    """
    是否启用 Tushare share_float 拉取「未来约 30 天解禁」并在 P1 安检中拦截。
    默认 False：不调接口、不拦截，避免专线/代理超时或权限问题拖慢洗盘。
    需要该风控时改为 true（并确保 Tushare 可连通）。
    """
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    lab = _lab_for("p1")
    v = lab.get("use_tushare_unlock_blacklist_for_p1")
    if v is None:
        v = p1.get("use_tushare_unlock_blacklist_for_p1")
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s2 = str(v).strip().lower()
    return s2 in ("1", "true", "yes", "on")


def get_p1_fund_memory_weight() -> float:
    """
    多维分项主分与 fund_memory_score（0~200 映射到 0~100）凸组合权重 w；最终分为 (1-w)*分项主分 + w*记忆分，再 cap 100。
    0 表示纯分项主分；实验室覆写优先生效。w 裁剪到 [0, 0.30]。
    """
    import constants as c

    default = float(getattr(c, "FUND_MEMORY_WEIGHT_P1", 0.10))
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    lab = _lab_for("p1")
    v = lab.get("fund_memory_weight_p1")
    if v is None:
        v = p1.get("fund_memory_weight_p1")
    if v is None:
        w = default
    else:
        try:
            w = float(v)
        except (TypeError, ValueError):
            w = default
    return float(max(0.0, min(0.30, w)))


def get_p1_combo_multiplier_config() -> Dict[str, float]:
    """读取 P1 组合特征乘数参数，便于实盘从 config.yaml 微调。"""
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    lab = _lab_for("p1")
    base = p1.get("combo_multiplier") if isinstance(p1.get("combo_multiplier"), dict) else {}
    override = lab.get("combo_multiplier") if isinstance(lab.get("combo_multiplier"), dict) else {}
    merged = deep_merge(base, override)

    def _f(key: str, default: float) -> float:
        try:
            v = float(merged.get(key, default))
        except (TypeError, ValueError):
            v = default
        return float(v)

    return {
        "enabled": bool(merged.get("enabled", True)),
        "min": _f("min", 0.90),
        "max": _f("max", 1.20),
        "top_tier": _f("top_tier", 1.20),
        "second_tier": _f("second_tier", 1.15),
        "third_tier": _f("third_tier", 1.12),
        "industry_dark_flow": _f("industry_dark_flow", 1.08),
        "fund_shape_wakeup": _f("fund_shape_wakeup", 1.18),
        "fund_pulse_confirm": _f("fund_pulse_confirm", 1.10),
        "weak_pattern_discount": _f("weak_pattern_discount", 0.94),
        "bubble_discount": _f("bubble_discount", 0.92),
        "hot_low_fund_discount": _f("hot_low_fund_discount", 0.95),
        "chip_core_min": _f("chip_core_min", 8.5),
        "trend_dist_min": _f("trend_dist_min", 6.0),
        "mat_min": _f("mat_min", 4.5),
        "fund_core_min": _f("fund_core_min", 8.0),
        "momentum_core_min": _f("momentum_core_min", 6.5),
        "golden_core_min": _f("golden_core_min", 8.0),
        "healthy_core_min": _f("healthy_core_min", 2.4),
        "slope_core_min": _f("slope_core_min", 4.2),
        "fund_hot_min": _f("fund_hot_min", 11.0),
        "chip_hot_min": _f("chip_hot_min", 6.0),
        "trend_health_min": _f("trend_health_min", 2.0),
        "industry_bonus_min": _f("industry_bonus_min", 8.0),
        "rank_bonus_min": _f("rank_bonus_min", 2.0),
    }


def get_p1_stock_behavior_penalty_config() -> Dict[str, Any]:
    """读取 P1 股性惩罚参数（长上影 / 冲高回落黑名单）。"""
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    lab = _lab_for("p1")
    base = p1.get("stock_behavior_penalty") if isinstance(p1.get("stock_behavior_penalty"), dict) else {}
    override = lab.get("stock_behavior_penalty") if isinstance(lab.get("stock_behavior_penalty"), dict) else {}
    merged = deep_merge(base, override)
    return {
        "enabled": bool(merged.get("enabled", True)),
        "lookback_days": int(float(merged.get("lookback_days", 20))),
        "long_upper_shadow_hits_min": int(float(merged.get("long_upper_shadow_hits_min", 3))),
        "intraday_dump_hits_min": int(float(merged.get("intraday_dump_hits_min", 3))),
        "score_penalty": float(merged.get("score_penalty", 8.0)),
    }


def get_p1_thresholds_for_profile(regime_name: Optional[str], profile_key: str) -> dict:
    """指定 strict/neutral/relaxed 档位的六键阈值（已合并 YAML + 实验室该档 pass_line 等）。"""
    merged = get_p1_profiles_merged(regime_name=regime_name)
    pk = str(profile_key or "").strip() or "neutral"
    prof = dict(merged.get(pk) or merged.get("neutral") or {})
    return _p1_prof_to_threshold_dict(prof)


def get_p1_threshold_profile_label(regime_name: Optional[str] = None) -> str:
    name = str(regime_name or "").strip()
    if any(k in name for k in ["主升", "趋势"]):
        return "趋势市·严格精选"
    if any(k in name for k in ["退潮", "空头", "主跌"]):
        return "退潮/空头·适度放宽"
    return "震荡市·稳健中性"


def get_p2_screener_config() -> Any:
    from core.strategies.p2_auction_screener import P2ScreenerConfig

    s = get_strategies_dict()
    flat = s.get("p2") if isinstance(s.get("p2"), dict) else {}
    flat = _apply_strategy_flat_aliases(flat, "p2")
    lab = _apply_strategy_flat_aliases(_lab_for("p2"), "p2")
    return dataclass_from_merged(P2ScreenerConfig, flat, lab)


def get_p3_intraday_screener_config() -> Any:
    from core.strategies.p3_intraday_screener import P3IntradayScreenerConfig

    s = get_strategies_dict()
    flat = s.get("p3") if isinstance(s.get("p3"), dict) else {}
    flat = _apply_strategy_flat_aliases(flat, "p3")
    lab = _apply_strategy_flat_aliases(_lab_for("p3"), "p3")
    return dataclass_from_merged(P3IntradayScreenerConfig, flat, lab)


def get_p4_tail_screener_config() -> Any:
    from core.strategies.p4_tail_screener import P4TailScreenerConfig

    s = get_strategies_dict()
    flat = s.get("p4") if isinstance(s.get("p4"), dict) else {}
    flat = _apply_strategy_flat_aliases(flat, "p4")
    lab = _apply_strategy_flat_aliases(_lab_for("p4"), "p4")
    return dataclass_from_merged(P4TailScreenerConfig, flat, lab)


def get_p5_postmarket_config() -> Any:
    from core.strategies.p5_postmarket_screener import P5PostmarketConfig

    s = get_strategies_dict()
    flat = s.get("p5") if isinstance(s.get("p5"), dict) else {}
    flat = _apply_strategy_flat_aliases(flat, "p5")
    lab = _apply_strategy_flat_aliases(_lab_for("p5"), "p5")
    return dataclass_from_merged(P5PostmarketConfig, flat, lab)


def get_golden_config() -> Dict[str, float]:
    s = get_strategies_dict()
    g = s.get("golden_burst") if isinstance(s.get("golden_burst"), dict) else {}
    lab_g = _lab_for("golden_burst")
    merged = {**g, **lab_g}
    return {
        "golden_burst_pct_low": float(merged.get("golden_burst_pct_low", 4.0)),
        "golden_burst_pct_high": float(merged.get("golden_burst_pct_high", 6.0)),
        "golden_burst_vr_low": float(merged.get("golden_burst_vr_low", 1.8)),
        "golden_burst_vr_high": float(merged.get("golden_burst_vr_high", 2.5)),
        "p5_golden_vr_min": float(merged.get("p5_golden_vr_min", 1.2)),
        "p5_golden_pct_low": float(merged.get("p5_golden_pct_low", 2.0)),
        "p5_golden_pct_high": float(merged.get("p5_golden_pct_high", 7.0)),
    }


def sync_scan_engine_strategy_configs() -> None:
    """
    兼容旧调用点（可删）。P2–P5 已在各自 run_all 首行从本模块 get_p* 拉最新配置，
    避免 scan_engine 与长生命周期引擎对象并发写 _cfg。
    """
    return


def _lab_opr_session_overrides_safe() -> Dict[str, Any]:
    """
    策略实验室：从 Streamlit session_state 读取 lab_opr_overrides。
    非 Streamlit 环境、未运行 run、或键类型错误时返回空 dict，绝不抛异常。
    """
    try:
        import streamlit as st  # type: ignore

        if not hasattr(st, "session_state"):
            return {}
        ov = st.session_state.get("lab_opr_overrides")
        if not isinstance(ov, dict):
            return {}
        return dict(ov)
    except Exception:
        return {}


def _observation_pool_relax_clamp_to_valid(out: Dict[str, float], defaults: Dict[str, float]) -> None:
    """就地修正越界项为 defaults（三键语义与 get_observation_pool_relax_settings 一致）。"""
    if not (0.5 < out.get("vr_shrink_gate", 0.0) <= 3.0):
        out["vr_shrink_gate"] = float(defaults["vr_shrink_gate"])
    if out.get("large_cap_yi_min", 0.0) < 10.0 or out.get("large_cap_yi_min", 0.0) > 20000.0:
        out["large_cap_yi_min"] = float(defaults["large_cap_yi_min"])
    if out.get("turnover_floor_pct", 0.0) < 0.05 or out.get("turnover_floor_pct", 0.0) > 5.0:
        out["turnover_floor_pct"] = float(defaults["turnover_floor_pct"])


def get_observation_pool_relax_settings(
    force_reload: bool = False,
    ignore_session_overrides: bool = False,
) -> Dict[str, float]:
    """
    读取 sop_v11.observation_pool_relax（极端缩量期观察池相关阈值）。

    若未传 ignore_session_overrides=True，则在 YAML 有效值之上再 deep_merge
    st.session_state['lab_opr_overrides']（策略实验室滑块）；覆写键类型错误或非有限数则跳过该键，
    越界则保留 YAML 合并后的当前值。最终再跑一次范围钳制。

    防御性：读盘失败、节点缺失时回退内置默认，保证 fund_mv_utils 不因配置损坏崩溃。

    返回固定三键（float）：vr_shrink_gate, large_cap_yi_min, turnover_floor_pct。
    """
    defaults: Dict[str, float] = {
        "vr_shrink_gate": 0.95,
        "large_cap_yi_min": 500.0,
        "turnover_floor_pct": 0.56,
    }
    out = dict(defaults)
    try:
        raw = _load_yaml_raw(force=force_reload)
        sop = raw.get("sop_v11")
        if isinstance(sop, dict):
            blk = sop.get("observation_pool_relax")
            if isinstance(blk, dict):
                for key in defaults:
                    if key not in blk:
                        continue
                    try:
                        f = float(blk[key])
                    except (TypeError, ValueError):
                        continue
                    if not math.isfinite(f):
                        continue
                    out[key] = f
        _observation_pool_relax_clamp_to_valid(out, defaults)
    except Exception as e:
        logger.debug("get_observation_pool_relax_settings YAML 段异常，使用默认: %s", e)
        out = dict(defaults)

    if ignore_session_overrides:
        return dict(out)

    yaml_merged = dict(out)
    sess = _lab_opr_session_overrides_safe()

    def _override_in_range(k: str, v: float) -> bool:
        if k == "vr_shrink_gate":
            return 0.5 < v <= 3.0
        if k == "large_cap_yi_min":
            return 10.0 <= v <= 20000.0
        if k == "turnover_floor_pct":
            return 0.05 <= v <= 5.0
        return False

    for key in defaults:
        if key not in sess:
            continue
        try:
            cand = float(sess[key])
        except (TypeError, ValueError):
            logger.debug(
                "lab_opr_overrides[%s] 类型不可转 float，保留 YAML 值 %.4f",
                key,
                float(yaml_merged[key]),
            )
            continue
        if not math.isfinite(cand):
            logger.debug(
                "lab_opr_overrides[%s] 非有限数，保留 YAML 值 %.4f",
                key,
                float(yaml_merged[key]),
            )
            continue
        if not _override_in_range(key, cand):
            logger.debug(
                "lab_opr_overrides[%s]=%s 超出允许范围，保留 YAML 值 %.4f",
                key,
                cand,
                float(yaml_merged[key]),
            )
            continue
        out[key] = cand

    _observation_pool_relax_clamp_to_valid(out, defaults)
    return out


def _get_env_webhook_url(env_key: str) -> str:
    """
    从环境变量 / .env 文件中读取企微 webhook URL。
    仅当 .env 中对应变量非空时才覆盖 YAML 配置。
    """
    # 1. 环境变量
    val = os.environ.get(env_key, "")
    if val:
        return val.strip()
    # 2. .env 文件
    ep = _env_path()
    if os.path.exists(ep):
        try:
            with open(ep, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    k, _, v = s.partition("=")
                    k = k.strip()
                    if k == env_key:
                        return v.strip().strip('"').strip("'")
        except Exception:
            pass
    return ""


def get_notification_config(force_reload: bool = False) -> Dict[str, Any]:
    """
    读取根节点 notification:
    {
      enabled,
      wechat_webhook_url,
      wechat_webhook_url_secondary
    }

    - primary：仅用于 p1~p5 选股推送 + 每天 9:18 固定消息
    - secondary：用于维护、下载、报错、任务执行结果等其他系统消息

    兼容逻辑：primary 为空时回退 legacy notify.wechat_webhook；secondary 为空则允许留空。
    webhook URL 支持从 .env 文件的 WEIXIN_WEBHOOK_URL / WEIXIN_WEBHOOK_URL_SECONDARY 覆盖 YAML 配置。
    """
    raw = _load_yaml_raw(force=force_reload)
    n = raw.get("notification")
    if not isinstance(n, dict):
        n = {}
    legacy = raw.get("notify")
    legacy_wh = ""
    if isinstance(legacy, dict):
        legacy_wh = str(legacy.get("wechat_webhook") or "").strip()

    # webhook URL：优先从 .env 读取（最高优先级），其次读 YAML
    yaml_url = str(n.get("wechat_webhook_url") or "").strip()
    url = _get_env_webhook_url("WEIXIN_WEBHOOK_URL") or yaml_url or legacy_wh
    yaml_sec = str(n.get("wechat_webhook_url_secondary") or "").strip()
    sec = _get_env_webhook_url("WEIXIN_WEBHOOK_URL_SECONDARY") or yaml_sec
    if not sec and url:
        sec = url
    return {
        "enabled": bool(n.get("enabled", True)),
        "wechat_webhook_url": url,
        "wechat_webhook_url_secondary": sec,
    }


def get_deepseek_analysis_config(force_reload: bool = False) -> Dict[str, Any]:
    """
    读取根节点 deepseek_analysis：用于 P2/P3/P4/P5 企微推送前追加 AI 分析建议。
    API Key 优先从环境变量 DEEPSEEK_API_KEY 读取，其次从 .env 文件读取，最后回退到 YAML。
    默认切到 Pro 模型，并开启 thinking 模式参数。
    """
    raw = _load_yaml_raw(force=force_reload)
    cfg = raw.get("deepseek_analysis")
    if not isinstance(cfg, dict):
        cfg = {}
    api_key = str(os.environ.get("DEEPSEEK_API_KEY") or cfg.get("api_key") or "").strip()
    base_url = str(cfg.get("base_url") or cfg.get("api_url") or "https://api.deepseek.com/chat/completions").strip()
    model = str(cfg.get("model") or "deepseek-v4-pro").strip()
    if model == "deepseek-chat":
        model = "deepseek-v4-pro"
    thinking_enabled = bool(cfg.get("thinking_enabled", True))
    reasoning_effort = str(cfg.get("reasoning_effort") or "high").strip().lower()
    if reasoning_effort in {"xhigh", "maximum"}:
        reasoning_effort = "max"
    if reasoning_effort not in {"high", "max"}:
        reasoning_effort = "high"
    try:
        timeout_seconds = float(cfg.get("timeout_seconds", 45.0))
    except (TypeError, ValueError):
        timeout_seconds = 45.0
    timeout_seconds = max(10.0, min(timeout_seconds, 120.0))
    try:
        max_tokens = int(cfg.get("max_tokens", 8000))
    except (TypeError, ValueError):
        max_tokens = 8000
    max_tokens = max(500, min(max_tokens, 32000))
    return {
        "enabled": bool(cfg.get("enabled", False)),
        "api_key": api_key,
        "base_url": base_url,
        "model": model,
        "thinking_enabled": thinking_enabled,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "max_tokens": max_tokens,
    }


def get_daemon_alert_silence_config(force_reload: bool = False) -> Dict[str, Any]:
    """
    读取根节点 daemon_alert_silence: { enabled, whitelist_keywords, blacklist_categories, silence_keywords }。
    - whitelist_keywords 命中时优先放行
    - blacklist_categories 精确类别静默
    - silence_keywords 为 title/detail 子串静默
    """
    raw = _load_yaml_raw(force=force_reload)
    blk = raw.get("daemon_alert_silence")
    if not isinstance(blk, dict):
        blk = {}
    def _norm_list(v: Any) -> List[str]:
        if not isinstance(v, list):
            return []
        out: List[str] = []
        for x in v:
            s = str(x or "").strip()
            if s:
                out.append(s)
        return out
    return {
        "enabled": bool(blk.get("enabled", True)),
        "whitelist_keywords": _norm_list(blk.get("whitelist_keywords")),
        "blacklist_categories": _norm_list(blk.get("blacklist_categories")),
        "silence_keywords": _norm_list(blk.get("silence_keywords")),
    }


def get_ui_alert_only(force_reload: bool = False) -> bool:
    """
    读取 config.yaml 根节点 ``risk_control.ui_alert_only``。

    为 True：纯 UI 预警模式（``RiskControlConfig.ui_alert_only=True``），一层/二层红线以标签提示为主，
    不拦截战法命中，便于极端行情下仅改配置即可切换「防守」强度。

    未配置 ``risk_control`` 或未写 ``ui_alert_only`` 时默认为 True，与引擎 dataclass 默认值一致。
    """
    raw = _load_yaml_raw(force=force_reload)
    rc = raw.get("risk_control")
    if not isinstance(rc, dict):
        return True
    v = rc.get("ui_alert_only")
    if v is None:
        return True
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "on", "y")


def get_ui_alert_only_flag(force_reload: bool = False) -> bool:
    """与 :func:`get_ui_alert_only` 同义，便于与外部接口命名对齐。"""
    return get_ui_alert_only(force_reload=force_reload)


def get_config_yaml_path() -> str:
    """项目根目录 ``config.yaml`` 绝对路径（供 UI 比对 mtime）。"""
    return _config_path()


def write_ui_alert_only_to_config_yaml(value: bool) -> bool:
    """
    将 ``risk_control.ui_alert_only`` 写回 config.yaml。

    实现为**仅替换**首个匹配行 ``ui_alert_only: ...``（保留文件其余内容、注释与键顺序），
    不使用整文件 ``yaml.safe_dump``，避免冲掉用户手工注释。

    若文件中不存在 ``ui_alert_only:`` 行但存在根键 ``risk_control:``，则在该行下一行插入一行
    ``  ui_alert_only: true|false``。

    成功后调用 :func:`invalidate_config_cache`。
    """
    path = _config_path()
    val = "true" if bool(value) else "false"
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError as e:
        logger.warning("write_ui_alert_only_to_config_yaml 读盘失败: %s", e)
        return False

    replaced = False
    out: List[str] = []
    for line in lines:
        if (not replaced) and line.lstrip().startswith("ui_alert_only:"):
            indent = line[: len(line) - len(line.lstrip())]
            out.append(f"{indent}ui_alert_only: {val}\n")
            replaced = True
        else:
            out.append(line)

    if not replaced:
        out2: List[str] = []
        inserted = False
        for line in lines:
            out2.append(line)
            if (not inserted) and line.strip().startswith("risk_control:"):
                out2.append(f"  ui_alert_only: {val}\n")
                inserted = True
        if not inserted:
            logger.warning("write_ui_alert_only_to_config_yaml: 未找到 ui_alert_only 行且未找到 risk_control 锚点")
            return False
        out = out2

    try:
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(out)
    except OSError as e:
        logger.warning("write_ui_alert_only_to_config_yaml 写盘失败: %s", e)
        return False

    # 用 yaml 安全解析回读，确认磁盘状态与写入意图一致（不整文件 dump，仅校验）
    try:
        with open(path, "r", encoding="utf-8") as f:
            chk = yaml.safe_load(f) or {}
        rc = chk.get("risk_control") if isinstance(chk, dict) else None
        if isinstance(rc, dict):
            g = rc.get("ui_alert_only")
            g_ok = g if isinstance(g, bool) else str(g).strip().lower() in ("true", "1", "yes", "on")
            if bool(g_ok) != bool(value):
                logger.warning(
                    "write_ui_alert_only_to_config_yaml: YAML 校验与目标不一致 | 文件=%s 期望=%s",
                    g_ok,
                    value,
                )
    except Exception as e:
        logger.debug("write_ui_alert_only_to_config_yaml 校验跳过: %s", e)

    invalidate_config_cache()
    logger.info("config.yaml risk_control.ui_alert_only 已设为 %s", val)
    return True


def get_risk_control_config(force_reload: bool = False):
    """
    返回与 config.yaml 的 ``risk_control`` 节点对齐的 ``RiskControlConfig`` 实例。

    当前仅将 YAML 中的 ``ui_alert_only`` 覆写到引擎配置；其余阈值与
    ``core.strategies.risk_control_engine.DEFAULT_RISK_CONFIG`` 保持一致。
    回测或单测若需完全自定义，可仍向 P2–P5 构造函数传入 ``risk_cfg=...`` 以绕过本函数。

    参数 force_reload 为 True 时强制重新读盘（忽略 mtime 缓存），一般不必使用。
    """
    from core.strategies.risk_control_engine import DEFAULT_RISK_CONFIG, RiskControlConfig

    ui = get_ui_alert_only(force_reload=force_reload)
    out: RiskControlConfig = replace(DEFAULT_RISK_CONFIG, ui_alert_only=ui)
    logger.debug("risk_control 已加载: ui_alert_only=%s", ui)
    return out


# 避免 pool_manager 与 config_manager 循环导入：标签函数在 get_p1_threshold_profile_label 内懒加载
