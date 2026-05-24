# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 - UI 组件库（终极宽屏实战与路径贯通版）
【细节修复】：
1. 指数 DuckDB 兜底：`data.db_core.get_index_latest_from_daily_data`（腾讯失败时用）。
2. 去除涨幅列多余的正号 (+)，保持视觉极致清爽，跌幅 (-) 完美保留。
3. 彻底修复 Streamlit 的 use_container_width 弃用警告（替换为 width="stretch"）。
4. 植入全局搜索过滤逻辑，支持通过股票代码或名称秒级定位。
5. 植入 A股实战级 Pandas Styler（涨红跌绿，烈马高亮）。
"""
import logging
import math
import time
import urllib.request

import pandas as pd
import streamlit as st

from core.stock_name_utils import normalize_stock_display_name

try:
    from data.db_core import get_index_latest_from_daily_data
except Exception as e:
    logging.warning("指数 DuckDB 兜底导入失败，顶部看板将依赖腾讯或空白: %s", e)

    def get_index_latest_from_daily_data(_ts_code):  # type: ignore
        return None

def inject_custom_css():
    st.markdown("""
    <style>
    /* ===== 全局基础 ===== */
    .stApp {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 50%, #e2e8f0 100%);
        color: #1e293b;
    }

    /* ===== 主标题强化 ===== */
    .stApp h2 {
        background: linear-gradient(90deg, #1e40af, #3b82f6, #60a5fa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: 800 !important;
        letter-spacing: 0.05em;
    }

    /* ===== 指标卡片 ===== */
    .metric-card {
        border-radius: 16px;
        padding: 16px;
        color: white;
        text-align: center;
        box-shadow: 0 8px 16px -4px rgba(0,0,0,0.15);
        min-height: 90px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        transition: all 0.3s ease;
        border: 1px solid rgba(255,255,255,0.2);
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 24px -8px rgba(0,0,0,0.2);
    }
    .metric-title {
        font-size: 0.85em;
        opacity: 0.9;
        margin-bottom: 8px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .metric-value {
        font-size: 1.6em;
        font-weight: 700;
        margin-bottom: 2px;
        line-height: 1.2;
    }

    /* ===== 指数卡片渐变底色 ===== */
    .idx-card-sh {
        background: linear-gradient(135deg, #1e3a5f 0%, #1e40af 100%);
        border-left: 4px solid #60a5fa;
    }
    .idx-card-cy {
        background: linear-gradient(135deg, #1a3a2a 0%, #166534 100%);
        border-left: 4px solid #4ade80;
    }
    .idx-card-hs {
        background: linear-gradient(135deg, #3b1a4a 0%, #7c3aed 100%);
        border-left: 4px solid #a78bfa;
    }
    .idx-card-p1 {
        background: linear-gradient(135deg, #1e3a8a 0%, #2563eb 100%);
        border-left: 4px solid #3b82f6;
    }

    /* ===== 表格美化 ===== */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 12px -2px rgba(0,0,0,0.08);
    }
    .stDataFrame table {
        border-collapse: separate !important;
        border-spacing: 0;
        font-size: 13px;
    }
    .stDataFrame thead th {
        background: linear-gradient(90deg, #1e293b, #334155) !important;
        color: white !important;
        font-weight: 600 !important;
        padding: 10px 12px !important;
        border-bottom: 2px solid #475569 !important;
    }
    .stDataFrame tbody tr:hover {
        background: #f1f5f9 !important;
    }
    .stDataFrame tbody tr:nth-child(even) {
        background: #fafafa !important;
    }
    .stDataFrame tbody td {
        padding: 8px 12px !important;
        border-bottom: 1px solid #f1f5f9 !important;
    }

    /* ===== 池子标题 ===== */
    .pool-title {
        background: linear-gradient(90deg, #1e293b, #475569);
        color: white;
        padding: 12px 16px;
        border-radius: 10px 10px 0 0;
        font-weight: 700;
        font-size: 14px;
        margin-bottom: 0;
    }

    /* ===== 按钮美化 ===== */
    .stButton > button {
        padding: 0.5rem 0.3rem;
        border-radius: 10px;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px -2px rgba(0,0,0,0.15);
    }
    .stButton > button p {
        font-size: 14px !important;
        font-weight: 700 !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }
    /* 强调按钮 */
    .stButton[data-primary="true"] > button,
    [data-testid="stMainMenu"] [data-testid="baseButton-primary"] {
        background: linear-gradient(135deg, #dc2626, #ef4444) !important;
        color: white !important;
        border: none !important;
    }

    /* ===== 信息卡片 ===== */
    .info-card {
        background: white;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 2px 8px -2px rgba(0,0,0,0.08);
        border: 1px solid #e2e8f0;
    }

    /* ===== Regime 展示强化 ===== */
    .regime-card {
        border-radius: 12px;
        padding: 18px 20px;
        background: white;
        box-shadow: 0 4px 12px -4px rgba(0,0,0,0.1);
        border: 1px solid #e2e8f0;
    }

    /* ===== 铁律区 ===== */
    .rules-card {
        background: linear-gradient(135deg, #fefce8 0%, #fef9c3 100%);
        border-left: 6px solid #ca8a04;
        border-radius: 12px;
        padding: 18px;
    }

    /* ===== 滚动条美化 ===== */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: #f1f5f9;
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb {
        background: #cbd5e1;
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #94a3b8;
    }

    /* ===== 空状态 ===== */
    .empty-state {
        background: linear-gradient(135deg, #f8fafc, #f1f5f9);
        border-radius: 12px;
        padding: 32px;
        text-align: center;
        color: #64748b;
        border: 2px dashed #cbd5e1;
    }

    /* ===== 标签/标签页美化 ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #f1f5f9;
        padding: 4px;
        border-radius: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        font-weight: 600;
    }
    </style>
    """, unsafe_allow_html=True)

def _fetch_tencent_indices_sync():
    """
    腾讯行情三大指数（与常见看盘软件接近的即时价/昨收涨跌幅）。
    解析失败返回 None。
    """
    url = "http://qt.gtimg.cn/q=sh000001,sz399006,sh000300"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            text = resp.read().decode("gbk", errors="replace")
    except Exception as e:
        logging.warning("腾讯指数行情请求失败: %s", e)
        return None
    lines = [ln for ln in text.strip().split("\n") if ln.strip() and '="' in ln]
    keys = ("000001.SH", "399006.SZ", "000300.SH")
    out = {}
    for i, line in enumerate(lines[:3]):
        try:
            body = line.split('="', 1)[1].strip().strip('";')
            parts = body.split("~")
            if len(parts) < 5:
                continue
            price = float(parts[3])
            pre = float(parts[4])
            if price <= 0 or pre <= 0 or not math.isfinite(price) or not math.isfinite(pre):
                continue
            pct = (price - pre) / pre * 100.0
            out[keys[i]] = {"close": price, "pct_chg": pct}
        except Exception:
            continue
    return out if len(out) == 3 else None


def _series_or_dict_to_idx_dict(row):
    """统一为 format_idx 可用的 dict。"""
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    try:
        d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        return d
    except Exception:
        return None


def fetch_dashboard_indices_safe():
    now = time.time()
    if "dash_idx_cache" in st.session_state:
        cache_time, data = st.session_state["dash_idx_cache"]
        if now - cache_time < 45:
            return data

    data = _fetch_tencent_indices_sync()
    if data is not None:
        data = (data.get("000001.SH"), data.get("399006.SZ"), data.get("000300.SH"))
    else:
        try:
            sz = _series_or_dict_to_idx_dict(get_index_latest_from_daily_data("000001.SH"))
            cy = _series_or_dict_to_idx_dict(get_index_latest_from_daily_data("399006.SZ"))
            hs300 = _series_or_dict_to_idx_dict(get_index_latest_from_daily_data("000300.SH"))
            data = (sz, cy, hs300)
        except Exception as e:
            logging.warning("fetch_dashboard_indices_safe DuckDB 指数兜底失败: %s", e)
            data = (None, None, None)

    st.session_state["dash_idx_cache"] = (now, data)
    return data

def render_top_dashboard(p1_count):
    safe_p1_count = str(p1_count) if p1_count is not None else "0"
    sz, cy, hs300 = fetch_dashboard_indices_safe()
    
    def format_idx(idx_data, prefix=""):
        if idx_data is None: return "---", ""
        try:
            pct = float(idx_data.get('pct_chg', 0))
            close_val = float(idx_data.get('close', 0))
            color = "#ef4444" if pct > 0 else "#10b981"
            arrow = "▲" if pct > 0 else "▼" if pct < 0 else "─"
            return f"{close_val:.2f}", f"<span style='color:{color};font-weight:700'>{arrow} {pct:+.2f}%</span>"
        except Exception as e:
            logging.debug(f"format_idx 解析指数数据异常: {e}")
            return "---", ""

    sz_val, sz_pct = format_idx(sz)
    cy_val, cy_pct = format_idx(cy)
    hs_val, hs_pct = format_idx(hs300)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            f"""<div class='metric-card idx-card-sh'>
            <div class='metric-title'>📈 上证指数</div>
            <div class='metric-value'>{sz_val}</div>
            <div style='font-size:0.9em;margin-top:4px;'>{sz_pct}</div>
            </div>""",
            unsafe_allow_html=True
        )
    with col2:
        st.markdown(
            f"""<div class='metric-card idx-card-cy'>
            <div class='metric-title'>🚀 创业板指</div>
            <div class='metric-value'>{cy_val}</div>
            <div style='font-size:0.9em;margin-top:4px;'>{cy_pct}</div>
            </div>""",
            unsafe_allow_html=True
        )
    with col3:
        st.markdown(
            f"""<div class='metric-card idx-card-hs'>
            <div class='metric-title'>💎 沪深 300</div>
            <div class='metric-value'>{hs_val}</div>
            <div style='font-size:0.9em;margin-top:4px;'>{hs_pct}</div>
            </div>""",
            unsafe_allow_html=True
        )
    with col4:
        st.markdown(
            f"""<div class='metric-card idx-card-p1'>
            <div class='metric-title'>🎯 底仓锁定 (P1)</div>
            <div class='metric-value' style='color:#93c5fd;'>{safe_p1_count} <span style='font-size:0.5em;opacity:0.8;'>只</span></div>
            </div>""",
            unsafe_allow_html=True
        )

def _normalize_name_for_display(name):
    return normalize_stock_display_name(name)

# 🎨 修复1：抛弃依赖 '+' 号的字符串判断，采用直接纯数字大小比较
def _color_pct(val):
    try:
        v = float(val)
        if v > 0: return 'color: #ef4444; font-weight: bold;'
        if v < 0: return 'color: #10b981; font-weight: bold;'
    except (TypeError, ValueError) as e:
        logging.debug(f"_color_pct 数值解析失败: {e}")
    return ''

def _color_nature(val):
    val_str = str(val)
    if '烈马' in val_str: return 'color: #ef4444; font-weight: bold;'
    if '慢牛' in val_str or '基石' in val_str: return 'color: #3b82f6; font-weight: bold;'
    return ''

def show_pool(col, title, key, data, display_cols=None, search_query="", empty_msg="等待雷达扫描..."):
    with col:
        if not data:
            st.markdown(f"""
            <div style='text-align:center; padding:32px 16px; background:linear-gradient(135deg,#f8fafc,#f1f5f9);
                border-radius:12px; border:2px dashed #cbd5e1; margin:8px 0;'>
                <div style='font-size:48px;margin-bottom:12px;'>🔭</div>
                <div style='color:#64748b;font-size:14px;'>{empty_msg}</div>
            </div>
            """, unsafe_allow_html=True)
            return

        filtered_list = []
        for row in data:
            c_code = str(row.get("代码", ""))
            c_name = str(row.get("名称", ""))
            if search_query and (search_query not in c_code and search_query not in c_name):
                continue
            filtered_list.append(row)

        count = len(filtered_list)
        st.markdown(f"""
        <div style='background:linear-gradient(90deg,#1e293b,#475569);color:white;padding:10px 16px;
            border-radius:12px 12px 0 0;font-weight:700;font-size:14px;'>
            {title} <span style='background:#ef4444;padding:2px 8px;border-radius:10px;font-size:12px;margin-left:8px;'>{count}</span>
        </div>
        """, unsafe_allow_html=True)
        if not filtered_list:
            st.warning("⚠️ 未找到匹配标的")
            return

        if display_cols:
            clean_list = [{k: row.get(k, "") for k in display_cols if k in row} for row in filtered_list]
            df = pd.DataFrame(clean_list)
            cols_order = [c for c in display_cols if c in df.columns]
        else:
            df = pd.DataFrame(filtered_list)
            base_cols = ['代码', '名称', '现价', '综合分', '战法', '涨幅', '连板', '机构', '量比', '真换手', '集中度']
            cols_order = [c for c in base_cols if c in df.columns]
            for c in df.columns:
                if '外资' in c and c not in cols_order: cols_order.append(c)
            for c in df.columns:
                if c not in cols_order and c != '建议仓位': cols_order.append(c)

        num_cols = ['涨幅', '现价', '量比', '真换手', '集中度', '综合分', '连板', '机构']
        for c in df.columns:
            if c in num_cols or '外资' in c:
                df[c] = pd.to_numeric(df[c].astype(str).replace(['%', '\\+', '＋', ','], '', regex=True), errors='coerce')

        def highlight_fn(df_to_style):
            styles = pd.DataFrame('', index=df_to_style.index, columns=df_to_style.columns)
            for i in range(len(df_to_style)):
                row = df_to_style.iloc[i]
                try:
                    score = float(row.get('综合分', 0))
                except (TypeError, ValueError):
                    score = 0
                try:
                    pct = float(row.get('涨幅', 0))
                except (TypeError, ValueError):
                    pct = 0
                nature_str = str(row.get('股性', ''))
                
                if score >= 135: row_style = 'color: #7e22ce; font-weight: 900; font-size: 1.05em;' 
                elif score >= 120: row_style = 'color: #dc2626; font-weight: bold;' 
                elif score >= 110: row_style = 'color: #2563eb; font-weight: bold;' 
                else: row_style = 'color: #d97706; font-weight: 500;' 
                styles.iloc[i, :] = row_style
                
                if pct < 0:
                    green_style = 'color: #10b981; font-weight: 600;' 
                    for col_name in ['代码', '名称', '涨幅', '现价']:
                        if col_name in styles.columns: styles.loc[df_to_style.index[i], col_name] = green_style

                if '股性' in styles.columns:
                    if '烈马' in nature_str: styles.loc[df_to_style.index[i], '股性'] = 'color: #ef4444; font-weight: bold;'
                    elif '慢牛' in nature_str or '基石' in nature_str: styles.loc[df_to_style.index[i], '股性'] = 'color: #3b82f6; font-weight: bold;'
            return styles

        col_cfg = {
            "综合分": st.column_config.NumberColumn(format="%.2f"), 
            "涨幅": st.column_config.NumberColumn(format="%.2f%%"), 
            "真换手": st.column_config.NumberColumn(format="%.1f%%"), 
            "集中度": st.column_config.NumberColumn(format="%.1f"), 
            "现价": st.column_config.NumberColumn(format="%.2f"), 
            "量比": st.column_config.NumberColumn(format="%.1f"),
            "连板": st.column_config.NumberColumn(format="%d"),
            "机构": st.column_config.NumberColumn(format="%d")
        }
        
        for c in df.columns:
            if '外资' in c: col_cfg[c] = st.column_config.NumberColumn(format="%d")

        _tc = [
            {"selector": "th", "props": [("text-align", "center"), ("background", "#1e293b"), ("color", "white"), ("font-weight", "600"), ("padding", "10px"), ("border-bottom", "2px solid #475569")]},
            {"selector": "td", "props": [("text-align", "center"), ("padding", "8px 10px"), ("border-bottom", "1px solid #f1f5f9")]},
            {"selector": "tr:hover", "props": [("background", "#f1f5f9")]},
        ]
        st.dataframe(
            df[cols_order]
            .style.set_table_styles(_tc)
            .apply(highlight_fn, axis=None)
            .map(_color_pct, subset=["涨幅"] if "涨幅" in cols_order else []),
            width="stretch",
            hide_index=True,
            column_config=col_cfg,
        )

def push_snapshot_to_mobile(scan_results, limit_up_count=0): 
    pass