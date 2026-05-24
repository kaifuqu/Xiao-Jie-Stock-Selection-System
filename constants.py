# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 全局常量与阈值控制中心
所有硬编码参数集中于此；版本号与全项目审查基线保持一致。

【V26 数据底座】capital_resonance_score 算法见 data/capital_resonance_features.py
    （80 底座 +20 两融加分，向量化）；P1 共振阶梯阈值 P1_CRS_SUPER_MIN / P1_CRS_MID_MIN
    ；P3–P5 动态分共振系数 CAPITAL_RESONANCE_WEIGHT_P345。
【V26 资金记忆体系】fund_memory_score：21 交易日半衰期指数衰减、双重过滤
    （≥100 亿流通 + 60 日放量异动痕迹），由日线管道维护；P1 最终平滑分由
    score_calibration 按 FUND_MEMORY_WEIGHT_P1 / config fund_memory_weight_p1 可选凸组合
    。FUND_MEMORY_HALF_LIFE_DAYS 等见第 7 节。
日线宽表列集与业务字段以本仓库 data_fetcher / db_core 契约为准。
"""

import os

# 全项目统一版本标识（审计 / UI / 日志）
APP_VERSION = "V26.6"

# ==================== 0. 档位命名统一 ====================
POOL_NAME_CN = {
    "p1": "底仓池",
    "p2": "竞价池",
    "p3": "盘中池",
    "p4": "盘尾池",
    "p5": "盘后池",
}

# ==================== 1. 全局路径管理 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
P0_FILE_PATH = os.path.join(BASE_DIR, "p0_custom.txt")

# ==================== 2. 市值过滤门槛 (单位: 万元) ====================
# 日线生肉落库：总市值或流通市值任一侧 ≥100 亿才进入当日下载候选（P0 自选股在 data_fetcher 中单独豁免）
DAILY_BASIC_MIN_MV_WAN = 1_000_000
# P1 全市场洗盘选股：仅流通市值 ≥100 亿参与初筛与打分（与 get_p1_candidate_codes / pool_manager / scan_engine 一致）
P1_SELECT_MIN_CIRC_MV_WAN = 1_000_000
# P1 前置防线：现价低于该值(元)视为机构审美拦截（pool_manager）
P1_INSTITUTION_MIN_PRICE_YUAN = 7.0

MIN_FETCH_MV = 1200000
MIN_STRAT_MV = 1500000

# ==================== 3. 核心大单资金阈值 ====================
ELG_THRESHOLDS = {
    'tail_min': 800,      
    'pool1_min': -1000,   
    'surge_min': 2000     
}

# ==================== 4. 性能与并发配置 ====================
MAX_WORKERS = 16          
API_RETRY_TIMES = 3       

# ==================== 5. 右侧严格模式与风控打分 ====================
STRICT_MODE = {
    'p2_auction_vr': 1.2, 
    'p3_intraday_vr': 1.5,
    'p4_tail_vr': 1.8,    
    'min_turnover_f_yang': 1.5,  # 阳线最低自由换手率：放量进攻
    'max_turnover_f_yin': 0.8    # 阴线最高自由换手率：极致缩量洗盘
}

PENALTY_RULES = {
    'max_score_cap': 200,
    'min_pass_score': 85,
    'penalty_multipliers': {
        'afternoon_sneak': 0.7,
        'high_amplitude': 0.8
    }
}

# ==================== 6. 左侧战法专属阈值 (L1-L8) ====================
LEFT_SIDE = {
    'max_net_outflow': -800,     
    'emotion_crash_pct': -4.5,   
    'vwap_diverge': 0.965,       
    'big_yin_fake_pct': -6.0,    
    
    'elg_crash': 2500,           
    'elg_vwap': 1500,            
    'elg_probe': 1000,           
    'elg_macd_diverge': 2000,    
    'elg_rsi_diverge': 1500,     
    'elg_boll_touch': 1500       
}

# ==================== 市场状态自适应参数（V26.6 沿用）====================
REGIME_PARAMS = {
    "趋势市": {"filter_mult": 0.85, "base_pos": 40, "hold_days": 8, "allow_direct_chase": True},
    "震荡市": {"filter_mult": 1.0, "base_pos": 30, "hold_days": 5, "allow_direct_chase": False},
    "情绪退潮市": {"filter_mult": 1.3, "base_pos": 20, "hold_days": 3, "allow_direct_chase": False}
}

# 日志表名称
LOG_TABLE = "signal_log"

# ==================== 7. P1 资金共振 / 股性记忆（V26.6）====================
# P1 主池/观察池「排序键」中与日线 capital_resonance_score 的融合权重（与 pool_manager._p1_final_sort_key 一致）
CAPITAL_RESONANCE_WEIGHT_P1 = 0.18
# P3–P5：calculate_dynamic_score 中对 crs_ui（0~100）的线性加分系数（与 P1 的 18% 排序权重区分）
CAPITAL_RESONANCE_WEIGHT_P345 = 0.08
# P1 初筛·资金共振阶梯阈值（亿元分层，与 pool_manager 硬闸/扫描闸一致）
P1_CRS_SUPER_MIN = 78.0   # 流通市值 ≥300 亿
P1_CRS_MID_MIN = 60.0     # 100 亿 ~ 300 亿
# 股性记忆池半衰期（交易日）；与 fund_memory_score 状态机、data/fund_memory_score.py 一致
FUND_MEMORY_HALF_LIFE_DAYS = 21
# P1 平滑百分制主分与 fund_memory_score（0~200 映射到 0~100）的凸组合权重，建议 0.15~0.20；由 score_calibration 应用；config 可覆写；不进入 P4/P5
FUND_MEMORY_WEIGHT_P1 = 0.10
