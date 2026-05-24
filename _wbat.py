content = """@echo off
REM start_daemon_24x7.bat - ASCII REM only for cmd.exe compatibility on Chinese Windows.
REM 7x24 daemon watchdog (daemon-only, no UI). Auto-restarts on crash with 60s back-off.
REM Version: 小杰AI选股系统 Pro V26.6
setlocal EnableExtensions
cd /d "%~dp0"

REM Ensure runtime dir exists (RotatingFileHandler writes to data\\runtime\\sniper.log)
if not exist "data\\runtime" mkdir "data\\runtime"

REM Detect Python executable: prefer py launcher, fall back to python
set "PY_EXE=py"
where py >nul 2>&1
if errorlevel 1 set "PY_EXE=python"

REM Verify Python is available
"%PY_EXE%" -c "import sys" 2>nul
if errorlevel 1 goto :NO_PY

REM Pre-flight system check (config, DuckDB, runtime dirs, etc.)
echo [CHECK] Running verify_system.py before entering watchdog loop ...
echo.
"%PY_EXE%" "%~dp0verify_system.py"
if errorlevel 1 goto :VERIFY_FAIL

REM Disable embedded UI scan worker: daemon-only deployment has no Streamlit UI process
set "XIAOJIE_EMBED_UI_SCAN_WORKER=0"
REM Unbuffered output: ensures daemon stdout/stderr reach this terminal without delay
set "PYTHONUNBUFFERED=1"

echo.
echo ============================================================
echo  [小杰AI选股系统 Pro V26.6] 7x24 后台看门狗已启动
echo ============================================================
echo  Log file  : data\\runtime\\sniper.log
echo  Config    : see core\\log_config.py for rotation settings
echo  DB mode   : exclusive (daemon-only, do NOT run start_server.bat simultaneously)
echo.
echo  [WATCHDOG] Daemon process will auto-restart after 60s back-off on exit.
echo  [HINT]   To stop: press Ctrl+C in this window.
echo ============================================================
echo.

:LOOP
echo [START] auto_sniper_daemon.py  [%date% %time%]
"%PY_EXE%" -u "%~dp0auto_sniper_daemon.py"
echo.
echo [INFO] Daemon exited. Watchdog will restart in 60 seconds to avoid
echo        rapid restart during DB maintenance/compression windows ...
timeout /t 60 /nobreak
goto LOOP

:NO_PY
echo [ERROR] Python interpreter not found.
echo         1. Install Python 3.10+ from https://www.python.org/downloads/
echo         2. Ensure py or python is on your system PATH
echo         3. Then run: py -m pip install -r requirements.txt -r requirements-daemon.txt
echo.
echo Press any key to exit ...
pause ^>nul
exit /b 1

:VERIFY_FAIL
echo.
echo [ERROR] verify_system.py failed. Please fix the errors above before running this script.
echo         Common fixes:
echo          - Run: py -m pip install -r requirements.txt -r requirements-daemon.txt
echo          - Ensure DuckDB is not locked by another process (close start_server.bat / Streamlit)
echo          - Check config.yaml syntax: py -c "import yaml; yaml.safe_load(open('config.yaml'))"
echo.
echo Press any key to exit ...
pause ^>nul
exit /b 1
"""

with open("d:/xiaojie/start_daemon_24x7.bat", "wb") as f:
    f.write(content.encode("gbk"))

with open("d:/xiaojie/start_daemon_24x7.bat", "rb") as f:
    raw = f.read()
try:
    txt = raw.decode("gbk")
    print("GBK OK")
    for line in txt.split("\n"):
        if "小杰" in line or "看门狗" in line:
            print(repr(line))
except Exception as e:
    print("FAIL:", e)
print("Done", len(raw), "bytes")
