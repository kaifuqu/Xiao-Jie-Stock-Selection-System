with open("d:/xiaojie/start_daemon_24x7.bat", "rb") as f:
    raw = f.read()

# Expected GBK bytes for key Chinese strings
checks = {
    "小杰AI选股系统 Pro V26.6": None,
    "7x24 后台看门狗已启动": None,
}

# Try to find each string's position and show surrounding bytes
for key in checks:
    idx = raw.find(key.encode("gbk"))
    if idx >= 0:
        checks[key] = idx
        print(f"FOUND at {idx}: {raw[idx:idx+len(key.encode('gbk'))+4].hex()} <- {key}")
    else:
        print(f"NOT FOUND (GBK bytes): {key}")
        # Try UTF-8
        idx2 = raw.find(key.encode("utf-8"))
        if idx2 >= 0:
            print(f"  Found as UTF-8 at {idx2} - WRONG ENCODING!")
        else:
            print(f"  Not found as UTF-8 either")

# Show first 30 bytes to see BOM / encoding marker
print(f"\nFirst 30 bytes hex: {raw[:30].hex()}")
print(f"File size: {len(raw)} bytes")
