import sys
sys.stdout.reconfigure(encoding='utf-8')

with open(r'd:\xiaojiePro\data\data_fetcher.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Lines 2357-2372 (0-indexed: 2356-2371) are the old block
start_idx = 2356
end_idx = 2372  # exclusive

new_block = '''    # 【性能优化 V2】并行生肉拉取（替代顺序 for 循环）
    raw_list = []
    total = len(to_sync)
    _MAX_PARALLEL_DAYS = min(8, max(1, os.cpu_count() or 4))
    _daily_lock = threading.Lock()

    def _fetch_one_day_recent(date_str):
        try:
            df = _core_pipeline(date_str, status_callback=status_callback)
            return (date_str, df)
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.exception("【并行拉取】%s 跳过: %s", date_str, e)
            return (date_str, None)

    try:
        with ThreadPoolExecutor(max_workers=_MAX_PARALLEL_DAYS) as executor:
            futures = {executor.submit(_fetch_one_day_recent, d): d for d in to_sync}
            done = 0
            for future in as_completed(futures):
                done += 1
                try:
                    ds, df = future.result()
                    if df is not None and not df.empty:
                        with _daily_lock:
                            raw_list.append(df)
                except DataFetchCriticalError:
                    raise
                except Exception as e:
                    logging.warning("并行拉取 future.result 异常: %s", e)
                if progress_callback:
                    progress_callback(done / total)
    except DataFetchCriticalError:
        raise
    except Exception as e:
        logging.warning("【并行拉取】近期同步线程池异常，回退顺序拉取: %s", e)
        for i, date_str in enumerate(to_sync):
            try:
                raw_df = _core_pipeline(date_str, status_callback=status_callback)
                if raw_df is not None and not raw_df.empty:
                    raw_list.append(raw_df)
            except DataFetchCriticalError:
                raise
            except Exception as ex:
                status_callback(f"⚠️ {date_str} 已跳过: {ex}")
            if progress_callback:
                progress_callback((i + 1) / total)
'''

new_lines = lines[:start_idx] + [new_block + '\n'] + lines[end_idx:]

with open(r'd:\xiaojiePro\data\data_fetcher.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('SUCCESS: replaced lines 2357-2372')
