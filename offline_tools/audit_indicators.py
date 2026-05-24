# -*- coding: utf-8 -*-
"""
量化系统指标调用全局扫描（正则捕获 df['xxx'] / row['xxx'] 形式）。
说明：默认排除 data/ 目录，故 data/data_fetcher.py 内 ALL_55_COLS 定义不会计入；
     基座 55 维清单请以 data/data_fetcher.py 中 ALL_55_COLS 为准，与本报告交叉比对。
"""
import os
import re
import sys
from collections import defaultdict


class IndicatorAuditor:
    def __init__(self, root_dir: str):
        """
        量化系统指标调用全局扫描器
        """
        self.root_dir = root_dir
        # 排除扫描的无关目录
        self.exclude_dirs = {'.git', '.vscode', '.idea', '__pycache__', 'venv', '.venv', 'env', 'data', 'logs'}
        # 匹配 df['indicator'] 或 row['indicator'] 格式
        self.dict_pattern = re.compile(r"\[['\"]([a-zA-Z0-9_]+)['\"]\]")
        # 统计结果存储: {indicator_name: [(file_path, line_number, line_content)]}
        self.usage_registry = defaultdict(list)

    def scan_codebase(self):
        """
        遍历项目文件并执行扫描
        """
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            # 过滤排除目录
            dirnames[:] = [d for d in dirnames if d not in self.exclude_dirs]

            for file in filenames:
                if file.endswith('.py'):
                    file_path = os.path.join(dirpath, file)
                    self._analyze_file(file_path)

    def _analyze_file(self, file_path: str):
        """
        逐行分析单文件中的指标调用
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    # 跳过注释行
                    if line.strip().startswith('#'):
                        continue

                    # 查找类似 ['ma5'] 的调用
                    matches = self.dict_pattern.findall(line)
                    for match in matches:
                        # 过滤掉常见的非指标字符串（如常见配置键名，可按需补充）
                        if match not in ['date', 'code', 'symbol', 'open', 'high', 'low', 'close', 'volume']:
                            self.usage_registry[match].append((file_path, line_num, line.strip()))
        except Exception as e:
            print(f"读取文件失败 {file_path}: {e}")

    def generate_report(self):
        """
        生成结构化的指标审计报告
        """
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        print("=" * 60)
        print("Xiaojie Quantitative Pro - 指标依赖全局审计报告")
        print("=" * 60)

        if not self.usage_registry:
            print("未扫描到明显的指标调用痕迹，请检查特征传入逻辑是否被深度封装。")
            return

        # 按调用频次从高到低排序
        sorted_usage = sorted(self.usage_registry.items(), key=lambda item: len(item[1]), reverse=True)

        total_used = len(sorted_usage)
        print(f"总计发现 {total_used} 个独立的衍生特征/指标被代码显式调用。\n")

        for indicator, usages in sorted_usage:
            print(f"🔹 指标: 【 {indicator} 】 (共被调用 {len(usages)} 次)")
            # 仅展示前3次调用作为上下文参考，避免刷屏
            for file_path, line_num, line_content in usages[:3]:
                # 简化文件路径显示
                short_path = os.path.relpath(file_path, self.root_dir)
                print(f"    -> [{short_path}:{line_num}] 代码: {line_content}")
            if len(usages) > 3:
                print(f"    -> ... 还有 {len(usages) - 3} 处调用未展开。")
            print("-" * 40)


if __name__ == "__main__":
    # 以脚本所在目录定位项目根（scripts/ 的上一级）
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    auditor = IndicatorAuditor(project_root)
    auditor.scan_codebase()
    auditor.generate_report()
